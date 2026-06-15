// ============================================================================================
// BIG FIXME (perf): The in-memory promoted-keys cache (#keys) is keyed by KeyCodec.mapKey(hashKey),
// which hex-encodes the key bytes. statusFor()/has() run on EVERY request's hot routing path
// (withSplitForwarding / groupItemsByRouting), so we pay a full hex encode of the hashKey per request.
// This needs an optimization pass AFTER the bytes-key refactor lands — e.g. a cheaper stable key
// (latin1/raw-byte string), caching the mapKey on the carried KeyBytes, or a byte-trie keyed cache.
// Do NOT micro-optimize now; revisit once the refactor is complete and we can benchmark.
// ============================================================================================

import { tryWhile } from "durable-utils/retries";
import { isHashPartition, type PartitionContextResolved } from "../partition-topology/partition-context.js";
import { resolveRangePartitionContext } from "../partition-topology/partition-id.js";
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import { RANGE_PROMOTION_FRACTION } from "../partition-topology/split-policy.js";
import type { SplitStatusKVItem } from "../partition-topology/split-state.js";
import type { PartitionPeer } from "./partition-peer.js";
import type { PartitionStore, PromotedKeyStatus } from "./partition-store.js";

/** The subset of the peer surface promotion needs from a range root (phase 3's gateway interface). */
export type PromotionPeer = Pick<PartitionPeer, "initFromSplit" | "triggerMigration">;

export type PromotionManagerDeps = {
	store: PartitionStore;
	/**
	 * Resolves the peer for a range-root context. Boundary rule: only DO classes (and FokosDB)
	 * acquire stubs — the DO passes this factory in; the manager never touches `env`/`.get()`.
	 */
	getRangeRootPeer: (ctx: PartitionContextResolved) => PromotionPeer;
	/** Requests a background-work run via the DO's scheduling machinery. */
	scheduleWork: (opts: { delayMs: number }) => Promise<void>;
	/** Structured-log context (the DO's logParams), so extracted logs keep their shape. */
	logParams: () => Record<string, unknown>;
	/** Max item rows deleted per promoted key per GC cycle (bounded work per cycle). */
	gcBatchLimit?: number;
};

/**
 * The promotion lifecycle (queued → promoting → promoted) together with the in-memory
 * promoted_keys cache it keeps consistent — one consistency domain, one owner. The cache is read
 * by hot-path routing (withSplitForwarding / groupItemsByRouting) via statusFor()/has().
 *
 * Hash DOs only: range DOs never have promoted_keys rows, so every method is a no-op there.
 */
export class PromotionManager {
	// In-memory cache of promoted_keys, loaded at DO startup via loadFromStorage(). Keyed by the
	// stable hex identity of the hashKey (a Uint8Array can't be a Map key); the KeyBytes is kept in
	// the value so callers get the real key back.
	#keys: Map<string, { key: KeyBytes; status: PromotedKeyStatus }> = new Map();

	constructor(private readonly deps: PromotionManagerDeps) {}

	/** Populates the cache from storage; called from the DO's blockConcurrencyWhile at startup. */
	loadFromStorage(): void {
		for (const row of this.deps.store.listPromotedKeys()) {
			this.#keys.set(KeyCodec.mapKey(row.hash_key), { key: row.hash_key, status: row.status });
		}
	}

	/** Hot-path read for routing decisions. */
	statusFor(hashKey: KeyBytes): PromotedKeyStatus | undefined {
		return this.#keys.get(KeyCodec.mapKey(hashKey))?.status;
	}

	/** Whether the key is tracked at any lifecycle stage (queued, promoting, or promoted). */
	has(hashKey: KeyBytes): boolean {
		return this.#keys.has(KeyCodec.mapKey(hashKey));
	}

	// Promotion⇄hash-split mutual exclusion input for the split policy.
	hasInFlightPromotions(): boolean {
		return this.deps.store.hasInFlightPromotedKeys();
	}

	/**
	 * Hash keys whose data lives (fully or in cutover) in their range structure — the DO's cancel
	 * fan-out resolves a range-root context for each of these.
	 */
	activeRangeRootHashKeys(): KeyBytes[] {
		const keys: KeyBytes[] = [];
		for (const { key, status } of this.#keys.values()) {
			if (status === "promoting" || status === "promoted") keys.push(key);
		}
		return keys;
	}

	/** Migration inheritance: a hash child receives a promoted-key entry pulled from its parent. */
	inheritKey(hashKey: KeyBytes, status: PromotedKeyStatus): void {
		this.#keys.set(KeyCodec.mapKey(hashKey), { key: hashKey, status });
	}

	snapshot(): { hashKey: KeyBytes; status: PromotedKeyStatus }[] {
		return Array.from(this.#keys.values()).map(({ key, status }) => ({ hashKey: key, status }));
	}

	async maybeQueuePromotion(pCtx: PartitionContextResolved, hk: KeyBytes, newKeyEst: number): Promise<void> {
		if (!isHashPartition(pCtx)) return;
		const threshold = (pCtx.hashSplitConditions.maxSizeMb ?? 0) * RANGE_PROMOTION_FRACTION * 1024 * 1024;
		if (threshold <= 0 || newKeyEst < threshold || this.has(hk)) return;
		const { inserted } = this.deps.store.insertPromotedKey(hk, "queued", Date.now());
		if (!inserted) {
			// A row already existed (the in-memory cache was stale, e.g. after crash recovery) —
			// resync the cache from storage instead of clobbering a possibly-advanced status
			// (promoting/promoted) back to queued.
			const stored = this.deps.store.getPromotedKeyStatus(hk);
			if (stored) this.#keys.set(KeyCodec.mapKey(hk), { key: hk, status: stored });
			return;
		}
		this.#keys.set(KeyCodec.mapKey(hk), { key: hk, status: "queued" });
		await this.deps.scheduleWork({ delayMs: 10 });
		console.log({
			...this.deps.logParams(),
			message: "fokos/partition: Key queued for promotion.",
			hk: KeyCodec.keyForLog(hk),
			newKeyEst,
			totalPromotedKeys: this.#keys.size,
		});
	}

	async acknowledgePromotionComplete(hashKey: KeyBytes): Promise<void> {
		const { updated } = this.deps.store.updatePromotedKeyStatus(hashKey, "promoting", "promoted", Date.now());
		if (updated) {
			this.#keys.set(KeyCodec.mapKey(hashKey), { key: hashKey, status: "promoted" });
		} else {
			// No row transitioned (e.g. idempotent re-ack of an already-promoted key) — sync the
			// cache from storage's truth rather than assuming the transition happened.
			const stored = this.deps.store.getPromotedKeyStatus(hashKey);
			if (stored) this.#keys.set(KeyCodec.mapKey(hashKey), { key: hashKey, status: stored });
			else this.#keys.delete(KeyCodec.mapKey(hashKey));
		}
		await this.deps.scheduleWork({ delayMs: 1_000 });
		console.log({ ...this.deps.logParams(), message: "fokos/partition: Promotion complete.", hashKey: KeyCodec.keyForLog(hashKey) });
	}

	/**
	 * Background job: advance each queued key through init → cutover → migrate.
	 * Per-key failures are logged and never block the remaining keys.
	 */
	async drive(pCtx: PartitionContextResolved, getSplitStatus: () => SplitStatusKVItem | undefined): Promise<void> {
		for (const { key, status } of this.#keys.values()) {
			if (status === "queued") {
				try {
					await this.startPromotion(pCtx, key, getSplitStatus);
				} catch (error) {
					console.error({
						...this.deps.logParams(),
						message: "fokos/partition: Promotion drive job failed.",
						hashKey: KeyCodec.keyForLog(key),
						error: String(error),
					});
				}
			}
		}
	}

	// Drives the queued→promoting→migrated lifecycle for a single hash key.
	// Idempotent — safe to call repeatedly across background cycles.
	async startPromotion(
		pCtx: PartitionContextResolved,
		hashKey: KeyBytes,
		getSplitStatus: () => SplitStatusKVItem | undefined,
	): Promise<void> {
		// Mutual exclusion: skip if a hash split is already in progress. The status is provided by
		// the DO — promotion does not read split-policy KV directly.
		const splitStatus = getSplitStatus();
		if (splitStatus?.status === "split_queued" || splitStatus?.status === "split_started") return;

		// A. Build identity for the range root (idFromName resolution — allowed everywhere).
		const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(pCtx, hashKey, null, null);
		const rangeRootPeer = this.deps.getRangeRootPeer(rangeRootCtx);

		// B. Initialize the range root (idempotent, retry ≤5). No forwarding yet — status is still 'queued'.
		await tryWhile(
			() =>
				rangeRootPeer.initFromSplit({
					parentPartitionContext: pCtx,
					newPartitionContext: rangeRootCtx,
					splitType: "range",
				}),
			(_err, attempt) => attempt <= 5,
		);

		// C. Cutover queued→promoting in one transactionSync, only if the key has no pending locks.
		const cutover = this.deps.store.transactionSync((): "cutover" | "deferred" | "stale" => {
			const lockCount = this.deps.store.pendingLockCountForHashKey(hashKey);
			if (lockCount > 0) return "deferred"; // retry next background cycle
			const { updated } = this.deps.store.updatePromotedKeyStatus(hashKey, "queued", "promoting", Date.now());
			return updated ? "cutover" : "stale";
		});
		if (cutover === "cutover") {
			this.#keys.set(KeyCodec.mapKey(hashKey), { key: hashKey, status: "promoting" });
		} else if (cutover === "stale") {
			// Storage disagrees with the in-memory cache (the key is no longer 'queued') — sync the
			// cache from storage's truth and skip; the background loop acts on the real status.
			const stored = this.deps.store.getPromotedKeyStatus(hashKey);
			if (stored) this.#keys.set(KeyCodec.mapKey(hashKey), { key: hashKey, status: stored });
			else this.#keys.delete(KeyCodec.mapKey(hashKey));
			console.log({
				...this.deps.logParams(),
				message: "fokos/partition: Promotion cutover skipped (key no longer queued in storage).",
				hashKey: KeyCodec.keyForLog(hashKey),
				storedStatus: stored ?? null,
			});
			return;
		}

		if (cutover === "deferred") {
			console.log({
				...this.deps.logParams(),
				message: "fokos/partition: Promotion cutover deferred (pending locks).",
				hashKey: KeyCodec.keyForLog(hashKey),
			});
			return;
		}

		// D. Trigger migration on the range root (fire-and-forget).
		try {
			await rangeRootPeer.triggerMigration();
		} catch (e) {
			console.error({
				...this.deps.logParams(),
				message: "fokos/partition: Failed to trigger promotion migration.",
				hashKey: KeyCodec.keyForLog(hashKey),
				error: String(e),
			});
		}
	}

	/**
	 * Background job: delete local items and pending_transactions for fully-promoted keys.
	 * Per-key failures are logged and never block the remaining keys.
	 */
	runGC(): void {
		for (const hashKey of this.deps.store.listPromotedKeysNeedingGC(1000)) {
			try {
				this.deps.store.deleteItemsBatchForHashKey(hashKey, this.deps.gcBatchLimit ?? 1000);
				this.deps.store.deletePendingTxForHashKey(hashKey);
				if (!this.deps.store.hasItemsForHashKey(hashKey)) {
					this.deps.store.deleteKeySizeEstimate(hashKey);
					this.deps.store.markPromotedKeyGcDone(hashKey);
				}
			} catch (error) {
				console.error({
					...this.deps.logParams(),
					message: "fokos/partition: Promotion GC job failed.",
					hashKey: KeyCodec.keyForLog(hashKey),
					error: String(error),
				});
			}
		}
	}

	/**
	 * Whether any key still needs a background cycle: drive (queued/promoting) or GC residuals
	 * (promoted keys with local items left). Feeds the DO's next-alarm computation.
	 */
	needsBackgroundWork(): boolean {
		return this.deps.store.hasInFlightPromotedKeys() || this.deps.store.hasResidualItemsForPromotedKeys();
	}
}
