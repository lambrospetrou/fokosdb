/**
 * The export of the `materialization_outbox` rows of a base partition to `MaterializationDO`.
 *
 * In-memory workers run while due rows exist. A worker claims a batch of rows in one storage
 * transaction, sends the latest image of each row in one call, and applies the answer in a second
 * storage transaction. Up to `maxCalls` workers, and thus calls, run at the same time. The alarm of
 * the partition only starts a worker again after a restart, an eviction, or a backoff.
 *
 * Each call carries the export epoch of this partition. A receiver rejects a call with an epoch below
 * the highest epoch that it stored for this partition. The exporter raises its epoch when an earlier
 * call can still apply: after an answer that it does not know, and after a restart while a call was
 * in flight (`is_call_open`). Two calls with the same epoch never carry the same item, because the
 * claim skips each key of a call in flight.
 */
import type { LatestImage } from "../../server/materialization/do-materialization.js";
import { KeyCodec, KeyPairMap, type KeyBytes } from "../../sharding/key-codec.js";
import invariant from "../invariant.js";
import type { DueOutboxRow, ExportFence, PartitionStore } from "./partition-store.js";

export type MaterializationExportConfig = {
	/** The export calls in flight at the same time. */
	maxCalls: number;
	/** The items of one call. */
	maxItems: number;
	/** The item bytes of one call, by `est_row_bytes`. A call holds at least one item. */
	maxBytes: number;
	/** The due outbox rows that one claim reads. Unowned rows and the keys of calls in flight count too. */
	maxScannedRows: number;
	/** The wait of an outbox row whose key this partition does not own now (a copy of a moved key). */
	unownedRetryMs: number;
	/** The wait of an item that the receiver did not apply, when other items of the call applied. */
	unprocessedRetryMs: number;
	/** The wait of an item that the receiver rejected. A rejected export is a defect, and it is logged. */
	rejectedRetryMs: number;
	/**
	 * The first wait after a call in which each item failed. Each next failure doubles it. The loop stops
	 * for the wait, and the alarm starts it again. The wait is in memory only, so a restart ends it.
	 */
	failureBackoffMinMs: number;
	failureBackoffMaxMs: number;
	/** The wait for the answer of a call. After it, the answer is unknown, and the epoch goes up. */
	callTimeoutMs: number;
	/** The deadline of the fallback alarm while the loop runs. */
	fallbackAlarmMs: number;
};

/** The answer for one image of a call. */
export type ExportItemOutcome = { outcome: "acknowledged" } | { outcome: "unprocessed" | "rejected" | "unknown"; code: string };

/** One export call: `images[i].opIndex` is `i`. */
export type ExportCall = { epoch: number; ancestorFences: ExportFence[]; images: LatestImage[] };

export type MaterializationExporterDeps = {
	store: PartitionStore;
	/** False on a router, on a target that imports, and behind the destroy fence. */
	canRun: () => boolean;
	/** A check of the keys that this partition owns now. The exporter gets a new check for each transaction. */
	ownerCheck: () => (row: { hk: KeyBytes; sk: KeyBytes }) => boolean;
	/** Sends one call. It returns one outcome for each image in `opIndex` order, and throws when the answer is unknown. */
	send: (call: ExportCall) => Promise<ExportItemOutcome[]>;
	config: () => MaterializationExportConfig;
	/**
	 * Sets the alarm for the end of a backoff. The workers stop for the backoff, and the alarm starts
	 * a worker again at `runAt`. It does not throw.
	 */
	scheduleRetry: (runAt: number) => void;
	now: () => number;
	logParams: () => Record<string, unknown>;
	/** The yield between two calls of a worker. Default: `scheduler.wait`. */
	wait?: (ms: number) => Promise<void>;
};

/** The code of an item that a receiver refused because of the epoch of the call. */
const PRODUCER_EPOCH_FENCED = "producer_epoch_fenced";

/** An item that the receiver did not apply, with a code that a later call with the same epoch can retry. */
function isRetryableUnprocessed(outcome: ExportItemOutcome): boolean {
	return outcome.outcome === "unprocessed" && outcome.code !== PRODUCER_EPOCH_FENCED;
}

/** A claimed call and the keys of its items. */
type ClaimedCall = { call: ExportCall; keys: KeyPairMap<true> };

export class MaterializationExporter {
	readonly #deps: MaterializationExporterDeps;
	readonly #wait: (ms: number) => Promise<void>;
	/** The running workers. Each worker sends at most one call at a time. */
	readonly #workers = new Set<Promise<void>>();
	/** A wake came after the last claim of a worker. A worker that claimed nothing then claims again. */
	#dirty = false;
	/** True after the first claim of this instance. The first claim runs the restart check. */
	#started = false;
	/** The calls in flight. At most `maxCalls`, so a key check reads at most that many maps. */
	readonly #inFlight = new Set<ClaimedCall>();
	#backoffMs = 0;
	#backoffUntil = 0;

	constructor(deps: MaterializationExporterDeps) {
		this.#deps = deps;
		this.#wait = deps.wait ?? (async (ms) => await scheduler.wait(ms));
	}

	/** True while a worker runs. */
	get running(): boolean {
		return this.#workers.size > 0;
	}

	/**
	 * Starts a worker when fewer than `maxCalls` run, so a new change can go in a second call while a
	 * slow call is in flight. A running worker that claimed nothing claims again after a wake.
	 */
	wake(): void {
		this.#dirty = true;
		if (this.#workers.size >= this.#deps.config().maxCalls) {
			return;
		}
		const worker: Promise<void> = this.#worker()
			.catch((error: unknown) => this.#logError("fokos/partition: the materialization export loop failed", { error: String(error) }))
			.finally(() => this.#workers.delete(worker));
		this.#workers.add(worker);
	}

	/** Waits until each worker stops. A worker stops when `canRun` becomes false. */
	async settled(): Promise<void> {
		while (this.#workers.size > 0) {
			await Promise.all(this.#workers);
		}
	}

	/** The time at which the alarm must start the loop, or null when the outbox is empty. */
	deadline(): number | null {
		if (this.#workers.size > 0) {
			return this.#deps.now() + this.#deps.config().fallbackAlarmMs;
		}
		const earliest = this.#deps.store.earliestOutboxAttemptAt();
		return earliest === null ? null : Math.max(earliest, this.#backoffUntil);
	}

	async #worker(): Promise<void> {
		// The write that woke the worker commits before the first claim reads the outbox.
		await this.#wait(0);
		for (;;) {
			// During a backoff the workers stop, so a long outage of the receiver keeps no instance
			// active. `deadline()` gives the end of the backoff, and the alarm starts a worker again.
			if (!this.#deps.canRun() || this.#backoffUntil > this.#deps.now()) {
				return;
			}
			// The claim is synchronous. A wake after it sets the flag again, and this worker claims again.
			this.#dirty = false;
			const claimed = this.#claim();
			if (claimed === null) {
				if (!this.#dirty) {
					return;
				}
				continue;
			}
			this.#complete(claimed, await this.#send(claimed.call));
			await this.#wait(0);
		}
	}

	/**
	 * Sends one call. Returns null when the answer is unknown: the call threw, timed out, or answered
	 * the wrong shape. The timer of the timeout is cleared when the call answers: a pending timer keeps
	 * the Durable Object from hibernation, and it is billed for that time.
	 */
	async #send(call: ExportCall): Promise<ExportItemOutcome[] | null> {
		const timeoutMs = this.#deps.config().callTimeoutMs;
		let timer: ReturnType<typeof setTimeout> | undefined;
		const timeout = new Promise<never>((_, reject) => {
			timer = setTimeout(() => reject(new Error(`the export call had no answer after ${timeoutMs} ms`)), timeoutMs);
		});
		try {
			const outcomes = await Promise.race([this.#deps.send(call), timeout]);
			invariant(outcomes.length === call.images.length, "fokos/export: the send returned one outcome for each image");
			return outcomes;
		} catch (error) {
			this.#logError("fokos/partition: a materialization export call has no answer", { epoch: call.epoch, error: String(error) });
			return null;
		} finally {
			clearTimeout(timer);
		}
	}

	/**
	 * Claims the next batch in one storage transaction, and returns null when no row is due. The
	 * in-memory state changes only after the transaction commits.
	 */
	#claim(): ClaimedCall | null {
		const { store } = this.#deps;
		const firstClaim = !this.#started;
		const claimed = store.transactionSync((): ClaimedCall | null => {
			const { epoch, isCallOpen } = this.#claimEpoch(firstClaim);
			const { images, keys } = this.#claimRows();
			if (images.length === 0) {
				return null;
			}
			if (!isCallOpen) {
				store.updateExportState({ callOpen: true });
			}
			return { call: { epoch, ancestorFences: store.exportFences(), images }, keys };
		});
		this.#started = true;
		if (claimed !== null) {
			this.#inFlight.add(claimed);
		}
		return claimed;
	}

	/**
	 * The epoch of the next call. At the first claim after a restart with a call open, a call of the
	 * instance before the restart can still apply. A raise before the next call makes each receiver
	 * that gets the next call first reject that late call. `is_call_open` stays 1 for the call that
	 * this claim starts, and the completion of the last call clears it.
	 */
	#claimEpoch(firstClaim: boolean): { epoch: number; isCallOpen: boolean } {
		const { store } = this.#deps;
		const state = store.exportState();
		if (!firstClaim || !state.isCallOpen) {
			return state;
		}
		return { epoch: store.raiseExportEpoch(), isCallOpen: true };
	}

	/** The images of the due rows that the next call carries, within the item and byte limits. */
	#claimRows(): { images: LatestImage[]; keys: KeyPairMap<true> } {
		const { store } = this.#deps;
		const config = this.#deps.config();
		const now = this.#deps.now();
		const owns = this.#deps.ownerCheck();
		const images: LatestImage[] = [];
		const keys = new KeyPairMap<true>();
		let bytes = 0;
		for (const row of store.dueOutboxRows(now, config.maxScannedRows)) {
			if (images.length >= config.maxItems) {
				break;
			}
			if (this.#inFlightHas(row.hk, row.sk)) {
				continue;
			}
			// A copy of a moved key: the new owner exports it. The wait keeps the copy from blocking the
			// owned rows behind it, and from making the alarm due again at once.
			if (!owns(row)) {
				store.deferOutboxRow(row.hk, row.sk, row.revision, now + config.unownedRetryMs);
				continue;
			}
			const claimed = this.#imageOf(row, images.length);
			if (claimed === undefined) {
				continue;
			}
			if (images.length > 0 && bytes + claimed.bytes > config.maxBytes) {
				break;
			}
			bytes += claimed.bytes;
			images.push(claimed.image);
			keys.set(row.hk, row.sk, true);
			// From now on a receiver can hold this revision, so a delete must reach it.
			if (row.isLocalOnly) {
				store.markOutboxRowExported(row.hk, row.sk);
			}
		}
		return { images, keys };
	}

	/**
	 * The image of one due row and its item bytes, or undefined when the row does not match its item.
	 * Each write of an item updates its outbox row in the same transaction, so a mismatch is a defect:
	 * it is logged, and the row is dropped.
	 */
	#imageOf(row: DueOutboxRow, opIndex: number): { image: LatestImage; bytes: number } | undefined {
		const { store } = this.#deps;
		if (row.isDeleted) {
			return { image: { opIndex, hashKey: row.hk, sortKey: row.sk, revision: row.revision, deleted: true }, bytes: 0 };
		}
		const item = store.exportItemImage(row.hk, row.sk);
		if (item === undefined || item.v !== row.revision) {
			this.#logError("fokos/partition: an outbox row does not match its item, and the export drops it", {
				key: KeyCodec.pairForLog(row.hk, row.sk),
				revision: row.revision,
				itemVersion: item?.v ?? null,
			});
			store.discardOutboxRow(row.hk, row.sk);
			return undefined;
		}
		const image: LatestImage = {
			opIndex,
			hashKey: row.hk,
			sortKey: row.sk,
			revision: row.revision,
			deleted: false,
			data: item.data,
			kind: item.kind,
			ttlAt: item.ttlAt,
		};
		return { image, bytes: item.estRowBytes };
	}

	/**
	 * Applies the answer of one call. `outcomes` is null when the answer is unknown. A router, a target
	 * that imports, or a fenced partition makes no transition: a restart then finds `is_call_open = 1`
	 * and raises the epoch.
	 */
	#complete(claimed: ClaimedCall, outcomes: ExportItemOutcome[] | null): void {
		this.#inFlight.delete(claimed);
		if (!this.#deps.canRun()) {
			return;
		}
		const failedAsWhole = outcomes === null || outcomes.every((o) => o.outcome === "unknown" || isRetryableUnprocessed(o));
		const applied = this.#deps.store.transactionSync(() => this.#applyAnswer(claimed.call, outcomes, failedAsWhole));
		// A discarded answer says nothing about the receiver now: a call with the current epoch decides.
		if (applied) {
			this.#updateBackoff(failedAsWhole);
		}
	}

	/**
	 * The writes of one answer, in one storage transaction. The answer of a call with an epoch below
	 * the current epoch is discarded: a call with the current epoch decides the outcome of its rows.
	 * Each write is for a key that this partition still owns. Returns false when it discarded the answer.
	 */
	#applyAnswer(call: ExportCall, outcomes: ExportItemOutcome[] | null, failedAsWhole: boolean): boolean {
		const { store } = this.#deps;
		const { epoch, isCallOpen } = store.exportState();
		const closeCall = this.#inFlight.size === 0 && isCallOpen ? { callOpen: false } : {};
		if (call.epoch < epoch) {
			store.updateExportState(closeCall);
			return false;
		}
		// An item with no answer can still apply. The next call goes with a higher epoch, so a receiver
		// that gets it first rejects the old call, and a receiver that got the old call first gets the
		// newer image after it. A fenced item goes again with a higher epoch too.
		let raise = outcomes === null;
		const owns = this.#deps.ownerCheck();
		for (const [i, o] of (outcomes ?? []).entries()) {
			if (o.outcome === "unknown" || (o.outcome === "unprocessed" && o.code === PRODUCER_EPOCH_FENCED)) {
				raise = true;
				continue;
			}
			const image = call.images[i];
			if (owns({ hk: image.hashKey, sk: image.sortKey })) {
				this.#applyItemOutcome(image, o, failedAsWhole);
			}
		}
		store.updateExportState({ raiseEpoch: raise, ...closeCall });
		return true;
	}

	/** The write for one owned item of the answer. A call that failed as a whole writes no row for an item. */
	#applyItemOutcome(image: LatestImage, outcome: ExportItemOutcome, failedAsWhole: boolean): void {
		const { store } = this.#deps;
		const config = this.#deps.config();
		const now = this.#deps.now();
		switch (outcome.outcome) {
			case "acknowledged":
				store.deleteOutboxRow(image.hashKey, image.sortKey, image.revision);
				return;
			case "unprocessed":
				if (!failedAsWhole) {
					store.deferOutboxRow(image.hashKey, image.sortKey, image.revision, now + config.unprocessedRetryMs);
				}
				return;
			case "rejected":
				this.#logError("fokos/partition: the materialization partition rejected an exported item", {
					key: KeyCodec.pairForLog(image.hashKey, image.sortKey),
					revision: image.revision,
					code: outcome.code,
				});
				store.deferOutboxRow(image.hashKey, image.sortKey, image.revision, now + config.rejectedRetryMs);
				return;
			case "unknown":
				return;
		}
	}

	/** A call that failed as a whole doubles the backoff and sets the alarm for its end. Any other answer ends it. */
	#updateBackoff(failedAsWhole: boolean): void {
		if (!failedAsWhole) {
			this.#backoffMs = 0;
			this.#backoffUntil = 0;
			return;
		}
		const config = this.#deps.config();
		this.#backoffMs = Math.min(config.failureBackoffMaxMs, Math.max(config.failureBackoffMinMs, this.#backoffMs * 2));
		this.#backoffUntil = this.#deps.now() + this.#backoffMs;
		// The alarm that the scheduler planned while the workers ran can be later than the backoff.
		this.#deps.scheduleRetry(this.#backoffUntil);
	}

	#inFlightHas(hk: KeyBytes, sk: KeyBytes): boolean {
		for (const claimed of this.#inFlight) {
			if (claimed.keys.has(hk, sk)) {
				return true;
			}
		}
		return false;
	}

	#logError(message: string, fields: Record<string, unknown>): void {
		let logParams: Record<string, unknown> = {};
		try {
			logParams = this.#deps.logParams();
		} catch (error) {
			logParams = { logParamsError: String(error) };
		}
		console.error({ ...logParams, message, ...fields });
	}
}

export function validateMaterializationExportConfig(config: MaterializationExportConfig): void {
	for (const name of [
		"maxCalls",
		"maxItems",
		"maxBytes",
		"maxScannedRows",
		"failureBackoffMinMs",
		"failureBackoffMaxMs",
		"callTimeoutMs",
		"fallbackAlarmMs",
	] as const) {
		invariant(Number.isInteger(config[name]) && config[name] > 0, `materialization-export: ${name} must be an integer greater than zero`);
	}
	for (const name of ["unownedRetryMs", "unprocessedRetryMs", "rejectedRetryMs"] as const) {
		invariant(
			Number.isInteger(config[name]) && config[name] >= 0,
			`materialization-export: ${name} must be an integer greater than or equal to zero`,
		);
	}
	invariant(config.maxScannedRows >= config.maxItems, "materialization-export: maxScannedRows must be at least maxItems");
	invariant(
		config.failureBackoffMaxMs >= config.failureBackoffMinMs,
		"materialization-export: failureBackoffMaxMs must be at least failureBackoffMinMs",
	);
}
