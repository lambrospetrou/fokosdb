import { tryWhile } from "durable-utils/retries";
import { isHashPartition, type PartitionContextLivePartition } from "../partition-topology/partition-context.js";
import { resolveRangePartitionContext } from "../partition-topology/partition-id.js";
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import { RANGE_PROMOTION_FRACTION } from "../partition-topology/split-policy.js";
import type { SplitStatusKVItem } from "../partition-topology/split-state.js";
import type { PartitionPeer } from "./partition-peer.js";
import type { PartitionStore, PromotedKeyStatus } from "./partition-store.js";

/** The part of the peer surface that promotion needs from a range root. */
export type PromotionPeer = Pick<PartitionPeer, "internalInitFromSplit" | "internalTriggerMigration">;

export type PromotionManagerDeps = {
	store: PartitionStore;
	/**
	 * Resolves the peer for a range-root context. Boundary rule: only DO classes (and FokosDB)
	 * acquire stubs — the DO passes this factory in; the manager never touches `env`/`.get()`.
	 */
	getRangeRootPeer: (ctx: PartitionContextLivePartition) => PromotionPeer;
	/** Requests a background-work run via the DO's scheduling machinery. */
	scheduleWork: (opts: { delayMs: number }) => Promise<void>;
	/** Structured-log context (the DO's logParams), so extracted logs keep their shape. */
	logParams: () => Record<string, unknown>;
	/** Max item rows deleted per promoted key per GC cycle (bounded work per cycle). */
	gcBatchLimit?: number;
};

/**
 * The promotion lifecycle (queued → promoting → promoted) owner.
 *
 * Hash DOs only: range DOs never have promoted_keys rows.
 */
export class PromotionManager {
	// TODO: Move this cache into the PartitionStore, so that nothing has to keep the two in sync.
	//
	// In-memory cache of promoted_keys (promoted only). Filled on-demand on reads.
	//
	// Keyed by a stable identity of the hashKey (a Uint8Array can't be a Map key); the KeyBytes is kept in
	// the value so callers get the real key back.
	//
	// key.toHex()/toBase64() gives a stable identity, but it allocates a string per request on the hot
	// routing path (statusFor()/has()). KeyCodec.mapKey(hashKey) gives a stable number identity
	// instead, which allocates nothing and compares faster.
	//
	// The store alone is enough for correctness, but a read of it on each request costs extra billed rows.
	// #cachePromoted = new LRUCache<bigint, { hk: KeyBytes }>(1_000);

	constructor(private readonly deps: PromotionManagerDeps) {}

	/** Populates the cache from storage; called from the DO's blockConcurrencyWhile at startup. */
	// loadFromStorage(): void {
	// 	// for (const row of this.deps.store.listPromotedKeys("promoted")) {
	// 	// 	this.#cachePromoted.set(KeyCodec.mapKey(row.hash_key), { hk: row.hash_key });
	// 	// }
	// }

	/** Hot-path read for routing decisions. */
	statusFor(hashKey: KeyBytes): PromotedKeyStatus | undefined {
		// const cached = this.#cachePromoted.get(KeyCodec.mapKey(hashKey));
		// if (cached && KeyCodec.compare(cached.hk, hashKey) === 0) return "promoted";
		return this.deps.store.getPromotedKeyStatus(hashKey);
	}

	/** Whether the key is tracked at any lifecycle stage (queued, promoting, or promoted). */
	hasStatus(hashKey: KeyBytes): boolean {
		// TODO: Answer from the cache.
		return this.statusFor(hashKey) !== undefined;
	}

	// Promotion⇄hash-split mutual exclusion input for the split policy.
	hasInFlightPromotions(): boolean {
		return this.deps.store.hasInFlightPromotedKeys();
	}

	snapshot(): { hashKey: KeyBytes; status: PromotedKeyStatus }[] {
		return this.deps.store.listPromotedKeys().map(({ hash_key, status }) => ({ hashKey: hash_key, status }));
	}

	async maybeQueuePromotion(pCtx: PartitionContextLivePartition, hk: KeyBytes, newKeyEst: number): Promise<void> {
		if (!isHashPartition(pCtx)) return;
		const threshold = (pCtx.hashSplitConditions.maxSizeMb ?? 0) * RANGE_PROMOTION_FRACTION * 1024 * 1024;
		if (threshold <= 0 || newKeyEst < threshold) return;
		await this.queuePromotion(pCtx, hk, newKeyEst);
	}

	/**
	 * Queues `hk` for promotion to its own range structure, whatever its current size.
	 *
	 * `maybeQueuePromotion` reaches here once a key grows past the threshold on its own. Calling it
	 * directly promotes a key the size heuristic has not caught yet — a known hot key an operator
	 * wants moved now, before it grows. The work itself is identical either way: the next background
	 * cycle runs the same `startPromotion` cutover.
	 *
	 * Returns false when the key already has a promotion entry, which makes repeated calls harmless.
	 */
	async queuePromotion(pCtx: PartitionContextLivePartition, hk: KeyBytes, newKeyEst?: number): Promise<boolean> {
		if (!isHashPartition(pCtx) || this.hasStatus(hk)) return false;
		const { inserted } = this.deps.store.insertPromotedKey(hk, "queued", Date.now());
		await this.deps.scheduleWork({ delayMs: 10 });
		console.log({
			...this.deps.logParams(),
			message: "fokos/partition: Key queued for promotion.",
			hk: KeyCodec.keyForLog(hk),
			newKeyEst,
			inserted,
		});
		return inserted;
	}

	async acknowledgePromotionComplete(hashKey: KeyBytes): Promise<void> {
		const { updated } = this.deps.store.updatePromotedKeyStatus(hashKey, "promoting", "promoted", Date.now());
		await this.deps.scheduleWork({ delayMs: 1_000 });
		console.log({
			...this.deps.logParams(),
			message: "fokos/partition: Promotion complete.",
			hashKey: KeyCodec.keyForLog(hashKey),
			updated,
		});
	}

	/**
	 * Background job: advance each queued key through init → cutover → migrate.
	 * Per-key failures are logged and never block the remaining keys.
	 */
	async drive(pCtx: PartitionContextLivePartition, getSplitStatus: () => SplitStatusKVItem | undefined): Promise<void> {
		for (const { hash_key: key } of this.deps.store.listPromotedKeys("queued")) {
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

	// Drives the queued→promoting→migrated lifecycle for a single hash key.
	// Idempotent — safe to call repeatedly across background cycles.
	async startPromotion(
		pCtx: PartitionContextLivePartition,
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
				rangeRootPeer.internalInitFromSplit({
					parentPartitionContext: pCtx,
					newPartitionContext: rangeRootCtx,
					newPartitionRangeDepth: 0,
					splitType: "range",
					// Spelled out for clarity, even though it's the same as the "no rows" default.
					rangeAncestors: [],
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

		if (cutover === "stale") {
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
			await rangeRootPeer.internalTriggerMigration();
		} catch (e) {
			console.error({
				...this.deps.logParams(),
				message: "fokos/partition: Failed to trigger promotion migration.",
				hashKey: KeyCodec.keyForLog(hashKey),
				error: String(e),
				errorProps: e,
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
					errorProps: error,
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
