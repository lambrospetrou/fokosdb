/**
 * The in-memory runner that sends the changes of `FokosDBMaterializationDO` to its targets.
 *
 * Up to `concurrency` workers run while a target has due work. They take the targets in turn, and each
 * target has at most one batch in flight, so one slow target holds at most one worker. A step of a
 * target claims a batch in one storage transaction, sends it, and applies the answer in a second
 * storage transaction. When no target has due work, the workers run the cleanup, and then stop. The
 * alarm of the partition only starts a worker again after a restart, an eviction, a backoff, or a probe
 * time of a circuit breaker.
 *
 * A batch takes the due `retry` rows of the target first, up to half of the items and the bytes of the
 * batch, and fills the rest from the change positions above the cursor of the target. The cursor
 * moves to the highest position that the claim read when each item of the batch applied or got a
 * `retry` row.
 *
 * Each call carries the epoch of its target. The runner raises the epoch when an earlier call can
 * still apply: after an answer that it does not know, and at the first claim after a restart while a
 * batch was open (`is_batch_open`). On a split child, each call also carries the ancestor fences of
 * its target until the inherited work of the target is done.
 */
import invariant from "../../shared/invariant.js";
import { decodeItemKeys } from "../../shared/transaction-limits.js";
import type { DataKind, HashKey, ReadItemValue, SortKey } from "../../shared/types.js";
import type { JsonValue } from "../../shared/json-types.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { MaterializationCleanup } from "./materialization-cleanup.js";
import type { GsiKey, MaterializationStore, TargetImage, TargetItemRef } from "./materialization-store.js";
import type {
	FokosTargetDefinition,
	GsiTargetDefinition,
	PushSinkTargetDefinition,
	PushTargetBatch,
	PushTargetItem,
	PushTargetResult,
} from "./targets.js";
import type { GsiWriteEntriesRequest, GsiWriteEntry } from "../do-partition.js";
import { gsiItemSortKey } from "../../shared/gsi/sort-key.js";
import { GSI_VIOLATION_CODES, sameGsiKey, type GsiDesiredEntry } from "./gsi-target.js";

/** One GSI entry with no `opIndex`: the call gives each entry its index. */
type GsiEntryBody = GsiWriteEntry extends infer E ? (E extends unknown ? Omit<E, "opIndex"> : never) : never;

/** The outcome of one entry of a GSI write. */
export type GsiEntryOutcome =
	| { outcome: "applied" }
	/** `wholeCall` is true when the call to the partition failed before it answered for each entry. */
	| { outcome: "rejected" | "unprocessed" | "unknown"; code: string; wholeCall?: boolean };

const BATCH_REQUEST_TOO_LARGE = "batch_request_too_large";
/** The code of each item of a push call to a target that does not implement the method. */
const PUSH_METHOD_MISSING = "push_method_missing";

/**
 * The GSI entry that the latest image of an item gives. It runs in the claim transaction. When it
 * throws, the item gets a `retry` row, and the claim continues with the other items.
 */
export type GsiEvaluator = (image: TargetImage) => GsiDesiredEntry;

export type TargetRunnerConfig = {
	/** The workers that run at the same time. */
	concurrency: number;
	/**
	 * The items of one batch. A batch holds at least one item from its `retry` rows and one item from
	 * its positions, so with a limit of 1 a batch can hold 2 items.
	 */
	maxItemsPerStep: number;
	/**
	 * The item bytes of one batch, by `est_row_bytes`. The first item from the positions goes into the
	 * batch even when it is above the limit.
	 */
	maxBytesPerStep: number;
	/** The change positions that one claim reads. */
	maxScannedPositions: number;
	/** The first backoff of a `retry` row. Each next failure doubles it, up to `retryBackoffMaxMs`. */
	retryBackoffMinMs: number;
	retryBackoffMaxMs: number;
	/** The calls in sequence that fail as a whole and that open the circuit breaker of a target. */
	breakerFailures: number;
	/** While the circuit breaker of a target is open, the runner sends one probe batch at this interval. */
	probeIntervalMs: number;
	/** The wait for the answer of a call. After it, the answer is unknown, and the epoch goes up. */
	callTimeoutMs: number;
	/** The deadline of the fallback alarm while the loop runs. */
	fallbackAlarmMs: number;
};

export type TargetRunnerDeps = {
	store: MaterializationStore;
	cleanup: MaterializationCleanup;
	/** The stored target list. */
	targets: () => readonly FokosTargetDefinition[];
	/** False on a router, on a target that imports, and behind the destroy fence. */
	canRun: () => boolean;
	/** A check of the keys that this partition owns now. The runner gets a new check for each transaction. */
	ownerCheck: () => (row: { hk: KeyBytes; sk: KeyBytes }) => boolean;
	/** The `DurableObjectId` of this partition, in hex. */
	producerId: () => string;
	/**
	 * Throws when this instance cannot call a push target, for example when the Worker env has no
	 * binding of that name. The Worker env does not change during the life of an instance.
	 */
	checkPushTarget: (target: PushSinkTargetDefinition) => void;
	/** Sends one batch to a push target, and returns the answer of the target as it is. */
	push: (target: PushSinkTargetDefinition, batch: PushTargetBatch) => Promise<unknown>;
	/** Sends one `gsiWriteEntries` call to a GSI table, and returns one outcome for each entry, in order. */
	writeGsi: (target: GsiTargetDefinition, request: GsiWriteEntriesRequest) => Promise<GsiEntryOutcome[]>;
	/**
	 * The evaluator of a GSI target for one step. It throws when the target cannot evaluate any item, for
	 * example when its plans do not compile: the step then claims nothing, and counts one failure of the
	 * target.
	 */
	gsiEvaluator: (target: GsiTargetDefinition) => GsiEvaluator;
	config: () => TargetRunnerConfig;
	/** Sets the alarm to start a worker again at `runAt`. It does not throw. */
	scheduleRetry: (runAt: number) => void;
	now: () => number;
	logParams: () => Record<string, unknown>;
	/** The yield between two steps of a worker. Default: `scheduler.wait`. */
	wait?: (ms: number) => Promise<void>;
};

/** The in-memory state of one target. */
type TargetState = {
	/** True while a step of the target runs. */
	inFlight: boolean;
	/** The highest position that the batch in flight read, or null while no batch is in flight. */
	readSeq: number | null;
	/** The last claim found no work. A wake clears it. */
	idle: boolean;
	/** True after the first claim of this instance. The first claim runs the restart check. */
	started: boolean;
	/** The calls in sequence that failed as a whole. */
	failures: number;
	/** While the circuit breaker is open, the time of the next probe. 0 while it is closed. */
	probeAt: number;
	/** True when this instance cannot call the push target. The runner then never steps the target. */
	unavailable: boolean;
};

type ClaimedItem = {
	itemId: number;
	revision: number;
	deleted: boolean;
	hk: KeyBytes;
	sk: KeyBytes;
	record: PushTargetItem;
	/** The writes of a GSI target, and the keys whose delete applied in this batch. */
	gsi?: ClaimedGsiWrites;
};

type ClaimedGsiWrites = {
	put?: { key: GsiKey; data: string | Uint8Array; kind: DataKind; ttlAt: number | null };
	/** The keys of the set other than the key of `put`. */
	deletes: GsiKey[];
	/** The code of a permanent violation. The step then deletes each key of the set. */
	violation?: string;
	removed: GsiKey[];
};

type ClaimedBatch = {
	epoch: number;
	ancestorFences: Array<{ ancestorId: string; minEpoch: number }>;
	/** The cursor that the claim read. */
	cursor: number;
	/** The highest position that the claim read, or null when it read none. */
	highestSeq: number | null;
	items: ClaimedItem[];
};

export class TargetRunner {
	readonly #deps: TargetRunnerDeps;
	readonly #wait: (ms: number) => Promise<void>;
	readonly #workers = new Set<Promise<void>>();
	readonly #states = new Map<string, TargetState>();
	/** The index of the target that the next pick tries first. */
	#next = 0;

	constructor(deps: TargetRunnerDeps) {
		this.#deps = deps;
		this.#wait = deps.wait ?? (async (ms) => await scheduler.wait(ms));
	}

	/** True while a worker runs. */
	get running(): boolean {
		return this.#workers.size > 0;
	}

	/** The highest position that the batch in flight of a target read, or undefined while it has none. */
	readPosition(targetId: string): number | undefined {
		return this.#states.get(targetId)?.readSeq ?? undefined;
	}

	/**
	 * Makes each target try a claim again, and starts workers until `concurrency` run. A worker that
	 * finds no target to step stops, so an extra worker costs one pick.
	 */
	wake(): void {
		for (const state of this.#states.values()) {
			state.idle = false;
		}
		// A request, and not the alarm, can start the workers. The fallback alarm then makes a restart
		// after an eviction continue the work.
		if (this.#workers.size === 0) {
			const config = this.#deps.config();
			this.#deps.scheduleRetry(this.#deps.now() + config.fallbackAlarmMs);
		}
		while (this.#workers.size < this.#deps.config().concurrency) {
			const worker: Promise<void> = this.#worker()
				.catch((error: unknown) => this.#logError("fokos/mat: the target runner failed", { error: String(error) }))
				.finally(() => {
					this.#workers.delete(worker);
					if (this.#workers.size === 0) {
						this.#armAlarm();
					}
				});
			this.#workers.add(worker);
		}
	}

	/** Waits until each worker stops. A worker stops when `canRun` becomes false. */
	async settled(): Promise<void> {
		while (this.#workers.size > 0) {
			await Promise.all(this.#workers);
		}
	}

	/**
	 * The time at which the alarm must start the loop, or null when no work is due. A target is due now
	 * when a position above its cursor exists, or at the earliest backoff end of its `retry` rows. A
	 * target with an open circuit breaker is due at its next probe. A target that this instance cannot
	 * call is never due.
	 */
	deadline(): number | null {
		const now = this.#deps.now();
		if (this.#workers.size > 0) {
			return now + this.#deps.config().fallbackAlarmMs;
		}
		const { store } = this.#deps;
		let at = this.#deps.cleanup.deadline(now);
		for (const row of store.targetRows()) {
			const state = this.#states.get(row.targetId);
			if (state?.unavailable === true) {
				continue;
			}
			const probeAt = state?.probeAt ?? 0;
			const due = probeAt > now ? probeAt : store.hasPositionAfter(row.cursorSeq) ? now : store.earliestRetryAt(row.targetId);
			if (due !== null && (at === null || due < at)) {
				at = due;
			}
		}
		return at;
	}

	async #worker(): Promise<void> {
		// The write that woke the worker commits before the first claim reads the positions.
		await this.#wait(0);
		for (;;) {
			if (!this.#deps.canRun()) {
				return;
			}
			const target = this.#nextTarget();
			if (target !== undefined) {
				await this.#step(target);
			} else if (!this.#deps.cleanup.step()) {
				return;
			}
			await this.#wait(0);
		}
	}

	/**
	 * The last worker sets the alarm for the next due work: a `retry` row with a later backoff end, or
	 * the probe of an open circuit breaker. While the workers ran, the alarm was set later.
	 */
	#armAlarm(): void {
		try {
			if (!this.#deps.canRun()) {
				return;
			}
			const at = this.deadline();
			if (at !== null) {
				this.#deps.scheduleRetry(at);
			}
		} catch (error) {
			this.#logError("fokos/mat: the target runner could not read its next deadline", { error: String(error) });
		}
	}

	/** The next target in turn that has no step in flight, that can have work, and whose breaker allows a call. */
	#nextTarget(): FokosTargetDefinition | undefined {
		const targets = this.#deps.targets();
		const now = this.#deps.now();
		for (let i = 0; i < targets.length; i++) {
			const index = (this.#next + i) % targets.length;
			const target = targets[index];
			const state = this.#state(target.targetId);
			if (state.inFlight || state.idle || state.unavailable || state.probeAt > now) {
				continue;
			}
			this.#next = (index + 1) % targets.length;
			return target;
		}
		return undefined;
	}

	#state(targetId: string): TargetState {
		let state = this.#states.get(targetId);
		if (state === undefined) {
			state = {
				inFlight: false,
				readSeq: null,
				idle: false,
				started: false,
				failures: 0,
				probeAt: 0,
				unavailable: false,
			};
			this.#states.set(targetId, state);
		}
		return state;
	}

	async #step(target: FokosTargetDefinition): Promise<void> {
		const state = this.#state(target.targetId);
		state.inFlight = true;
		try {
			if (target.kind === "push_sink" && !this.#pushTargetAvailable(target, state)) {
				return;
			}
			const evaluator = target.kind === "gsi" ? this.#gsiEvaluator(target) : undefined;
			if (evaluator === null) {
				this.#recordFailure(target.targetId, state);
				return;
			}
			const batch = this.#claim(target, state, evaluator);
			if (batch === null) {
				return;
			}
			const results = target.kind === "gsi" ? await this.#sendGsi(target, batch) : await this.#send(target, batch);
			this.#complete(target.targetId, state, batch, results);
		} finally {
			state.inFlight = false;
			state.readSeq = null;
		}
	}

	/**
	 * False when this instance cannot call a push target. The check runs before the claim, so the
	 * target then writes nothing: no claim, no epoch raise, and no `retry` row. The Worker env does not
	 * change during the life of an instance, so the runner never steps the target again, and the
	 * target sets no alarm. The positions and the tombstones that the target did not read stay.
	 */
	#pushTargetAvailable(target: PushSinkTargetDefinition, state: TargetState): boolean {
		try {
			this.#deps.checkPushTarget(target);
			return true;
		} catch (error) {
			state.unavailable = true;
			this.#logError("fokos/mat: this instance cannot call a push target, and stops it", {
				targetId: target.targetId,
				binding: target.binding,
				error: String(error),
			});
			return false;
		}
	}

	/**
	 * The evaluator of a GSI target for one step, or null when the target cannot evaluate any item. The
	 * failure is not a failure of an item, so the step keeps the cursor and writes no `retry` row.
	 */
	#gsiEvaluator(target: GsiTargetDefinition): GsiEvaluator | null {
		try {
			return this.#deps.gsiEvaluator(target);
		} catch (error) {
			this.#logError("fokos/mat: a GSI target cannot evaluate its entries", { targetId: target.targetId, error: String(error) });
			return null;
		}
	}

	/**
	 * Claims the next batch of a target in one storage transaction. Returns null when the claim found no
	 * item to send. A claim that read positions and found no item to send moves the cursor past them, so
	 * a long run of skipped positions moves the cursor in bounded steps. `evaluator` is set for a GSI
	 * target only.
	 */
	#claim(target: FokosTargetDefinition, state: TargetState, evaluator: GsiEvaluator | undefined): ClaimedBatch | null {
		const { store } = this.#deps;
		const targetId = target.targetId;
		const firstClaim = !state.started;
		const claim = store.transactionSync(() => {
			const row = store.targetRow(targetId);
			// A batch of the instance before the restart can still apply. A raise before the next call
			// makes each receiver that gets the next call first reject that late call.
			const epoch = firstClaim && row.isBatchOpen ? store.raiseTargetEpoch(targetId) : row.epoch;
			const { refs, retryCount, highestSeq } = this.#claimRefs(targetId, row.cursorSeq);
			const items = this.#claimItems(targetId, refs, retryCount, evaluator);
			if (items.length === 0) {
				// No batch of the current epoch is in flight, so the marker can go.
				if (highestSeq !== null || row.isBatchOpen) {
					store.updateTarget(targetId, highestSeq ?? row.cursorSeq, false);
				}
				store.dropDoneTargetFences(targetId);
				return { batch: null, readNothing: highestSeq === null };
			}
			if (!row.isBatchOpen) {
				store.openTargetBatch(targetId);
			}
			// The claim can delete an inherited `retry` row, so the fences can be done.
			store.dropDoneTargetFences(targetId);
			const ancestorFences = store.targetFences(targetId);
			return { batch: { epoch, ancestorFences, cursor: row.cursorSeq, highestSeq, items }, readNothing: false };
		});
		state.started = true;
		if (claim.batch === null) {
			state.idle = claim.readNothing;
			return null;
		}
		state.readSeq = claim.batch.highestSeq ?? claim.batch.cursor;
		return claim.batch;
	}

	/**
	 * The items that the next batch carries, with no item data, and the highest position that the claim
	 * read. The due `retry` rows come first, up to half of the items and half of the bytes of a batch.
	 * Then the positions above the cursor fill the batch. Each part takes at least one item, so the
	 * `retry` rows cannot stop the cursor. A position is done for the target and is skipped when its
	 * item has a `retry` row, or is already in the batch. The first `retryCount` refs come from the
	 * `retry` rows.
	 */
	#claimRefs(targetId: string, cursor: number): { refs: TargetItemRef[]; retryCount: number; highestSeq: number | null } {
		const { store } = this.#deps;
		const config = this.#deps.config();
		const refs: TargetItemRef[] = [];
		const inBatch = new Set<number>();
		let bytes = 0;
		/** Adds an item when the part has no item yet, or when the batch stays within the limits. */
		const add = (ref: TargetItemRef, partStart: number, maxItems: number, maxBytes: number): boolean => {
			if (refs.length > partStart && (refs.length >= maxItems || bytes + ref.estRowBytes > maxBytes)) {
				return false;
			}
			refs.push(ref);
			inBatch.add(ref.itemId);
			bytes += ref.estRowBytes;
			return true;
		};
		const retryItems = Math.max(1, Math.floor(config.maxItemsPerStep / 2));
		const retryBytes = Math.floor(config.maxBytesPerStep / 2);
		for (const ref of store.dueRetryItems(targetId, this.#deps.now(), retryItems)) {
			if (!add(ref, 0, retryItems, retryBytes)) {
				break;
			}
		}
		const positionStart = refs.length;
		let highestSeq: number | null = null;
		for (const position of store.positionsAfter(targetId, cursor, config.maxScannedPositions)) {
			const skip = position.inRetry || inBatch.has(position.itemId);
			if (!skip && !add(position, positionStart, config.maxItemsPerStep, config.maxBytesPerStep)) {
				break;
			}
			highestSeq = position.seq;
		}
		return { refs, retryCount: positionStart, highestSeq };
	}

	/**
	 * Reads the image of each claimed item, in the snapshot of the claim. An item that moved to another
	 * partition is skipped: its position is done here, and the new owner sends it.
	 *
	 * The `retry` row of a moved item sends nothing here, so the claim deletes it. While a promotion of
	 * this partition is between its cutover and its completion, the child can still need the row, so
	 * the claim moves its backoff end instead. The row then does not fill the `retry` part of each batch.
	 */
	#claimItems(targetId: string, refs: readonly TargetItemRef[], retryCount: number, evaluator: GsiEvaluator | undefined): ClaimedItem[] {
		const { store } = this.#deps;
		const owns = this.#deps.ownerCheck();
		const items: ClaimedItem[] = [];
		for (const [i, ref] of refs.entries()) {
			const image = store.targetImage(ref.itemId);
			if (image === undefined) {
				continue;
			}
			if (!owns(image)) {
				if (i < retryCount) {
					if (store.hasCursorSnapshots()) {
						store.deferTargetRetry(targetId, ref.itemId, this.#deps.now() + this.#deps.config().retryBackoffMaxMs);
					} else {
						store.deleteTargetRetry(targetId, ref.itemId);
					}
				}
				continue;
			}
			const gsi = evaluator === undefined ? undefined : this.#claimGsi(targetId, ref.itemId, image, evaluator);
			if (gsi === null) {
				continue;
			}
			items.push({
				itemId: ref.itemId,
				revision: image.revision,
				deleted: image.deleted,
				hk: image.hk,
				sk: image.sk,
				record: toPushItem(image),
				...(gsi === undefined ? {} : { gsi }),
			});
		}
		return items;
	}

	/**
	 * The GSI writes of one item. A desired key that is not in the key set goes into the set before the
	 * call, so that a later step can delete an entry that the call wrote. Returns null when the
	 * evaluation throws: the item then gets a `retry` row, and its key set does not change.
	 */
	#claimGsi(targetId: string, itemId: number, image: TargetImage, evaluator: GsiEvaluator): ClaimedGsiWrites | null {
		const { store } = this.#deps;
		let desired: GsiDesiredEntry;
		try {
			desired = evaluator(image);
		} catch (error) {
			const config = this.#deps.config();
			store.recordTargetRetry(
				targetId,
				itemId,
				this.#deps.now(),
				config.retryBackoffMinMs,
				config.retryBackoffMaxMs,
				"gsi_evaluation_failed",
			);
			this.#logError("fokos/mat: a GSI evaluation failed", {
				targetId,
				key: KeyCodec.pairForLog(image.hk, image.sk),
				revision: image.revision,
				error: String(error),
			});
			return null;
		}
		const keys = store.indexKeys(targetId, itemId);
		if (desired.kind !== "present") {
			return { deletes: keys, removed: [], ...(desired.kind === "violation" ? { violation: desired.code } : {}) };
		}
		if (!keys.some((key) => sameGsiKey(key, desired.key))) {
			keys.push(desired.key);
			store.addIndexKey(targetId, itemId, desired.key);
		}
		return {
			put: { key: desired.key, data: desired.data, kind: desired.dataKind, ttlAt: desired.ttlAt },
			deletes: keys.filter((key) => !sameGsiKey(key, desired.key)),
			removed: [],
		};
	}

	/**
	 * Sends the GSI writes of one batch in two calls in sequence. The first call puts each desired
	 * entry, and deletes each key of an item with no desired entry. The second call deletes the other
	 * keys of each item whose put applied, so that a key move writes the new entry before it deletes
	 * the old one. After a put that the GSI rejected, the second call deletes each key of the item. The
	 * two calls carry different keys of an item, so they keep the same-epoch rule. Returns null when the
	 * outcome of an entry is unknown.
	 */
	async #sendGsi(target: GsiTargetDefinition, batch: ClaimedBatch): Promise<PushTargetResult[] | null> {
		const results: PushTargetResult[] = batch.items.map(() => ({ outcome: "applied" }));
		type Pending = { index: number; entry: GsiEntryBody; key: GsiKey; isPut: boolean };
		const entryOf = (item: ClaimedItem, key: GsiKey): GsiEntryBody => ({
			op: "delete",
			hashKey: key.hk,
			sortKey: gsiItemSortKey(key.sk, item.hk, item.sk),
		});
		/** The code of a permanent failure, by item index: the item gets `rejected` when its deletes apply. */
		const violations = new Map<number, string>();
		const first: Pending[] = [];
		const second: Pending[] = [];
		for (const [index, item] of batch.items.entries()) {
			const gsi = item.gsi;
			invariant(gsi !== undefined, "fokos/mat: a GSI batch item has no GSI writes");
			if (gsi.violation !== undefined) {
				violations.set(index, gsi.violation);
			}
			if (gsi.put !== undefined) {
				const { key, data, kind, ttlAt } = gsi.put;
				first.push({
					index,
					key,
					isPut: true,
					entry: {
						op: "put",
						hashKey: key.hk,
						sortKey: gsiItemSortKey(key.sk, item.hk, item.sk),
						data,
						kind,
						baseRevision: item.revision,
						...(ttlAt === null ? {} : { ttlAt }),
					},
				});
			} else {
				first.push(...gsi.deletes.map((key) => ({ index, key, isPut: false, entry: entryOf(item, key) })));
			}
		}
		const failed = new Set<number>();
		const apply = (pending: Pending[], outcomes: GsiEntryOutcome[]) => {
			for (const [k, { index, key, isPut }] of pending.entries()) {
				const outcome = outcomes[k];
				const item = batch.items[index];
				const gsi = item.gsi as ClaimedGsiWrites;
				if (outcome.outcome === "applied") {
					if (!isPut) {
						gsi.removed.push(key);
					} else {
						second.push(...gsi.deletes.map((other) => ({ index, key: other, isPut: false, entry: entryOf(item, other) })));
					}
					continue;
				}
				if (isPut && outcome.outcome === "rejected" && GSI_VIOLATION_CODES.has(outcome.code)) {
					violations.set(index, outcome.code);
					second.push(...[key, ...gsi.deletes].map((other) => ({ index, key: other, isPut: false, entry: entryOf(item, other) })));
					continue;
				}
				if (outcome.outcome === "rejected") {
					this.#logError("fokos/mat: a GSI rejected an entry with a code that the item data does not cause", {
						targetId: target.targetId,
						key: KeyCodec.pairForLog(item.hk, item.sk),
						code: outcome.code,
					});
				}
				failed.add(index);
				results[index] = { outcome: "retry", code: outcome.code };
			}
		};
		for (const pending of [first, second]) {
			if (pending.length === 0) {
				continue;
			}
			const outcomes = await this.#callGsi(
				target,
				batch,
				pending.map((p) => p.entry),
			);
			if (outcomes === null) {
				return null;
			}
			// A rejection of the whole call is not a failure of its items: each item retries, and the
			// circuit breaker counts one failure.
			const whole = outcomes.find((outcome) => outcome.outcome === "rejected" && outcome.wholeCall === true);
			if (whole !== undefined && whole.outcome !== "applied") {
				this.#logError("fokos/mat: a GSI rejected a whole call", { targetId: target.targetId, code: whole.code });
				return batch.items.map(() => ({ outcome: "retry", code: whole.code }));
			}
			apply(pending, outcomes);
		}
		for (const [index, code] of violations) {
			if (!failed.has(index)) {
				results[index] = { outcome: "rejected", code };
			}
		}
		return results;
	}

	/**
	 * One GSI call. A call above the RPC limit applied nothing, so its two halves go again, down to one
	 * entry. Returns null when the outcome of an entry is unknown.
	 */
	async #callGsi(target: GsiTargetDefinition, batch: ClaimedBatch, entries: Array<GsiEntryBody>): Promise<GsiEntryOutcome[] | null> {
		const outcomes = await this.#callGsiOnce(target, batch, entries);
		if (
			outcomes === null ||
			entries.length < 2 ||
			!outcomes.some((outcome) => outcome.outcome === "rejected" && outcome.code === BATCH_REQUEST_TOO_LARGE)
		) {
			return outcomes;
		}
		const half = Math.ceil(entries.length / 2);
		const first = await this.#callGsi(target, batch, entries.slice(0, half));
		const second = first === null ? null : await this.#callGsi(target, batch, entries.slice(half));
		return first === null || second === null ? null : [...first, ...second];
	}

	async #callGsiOnce(target: GsiTargetDefinition, batch: ClaimedBatch, entries: Array<GsiEntryBody>): Promise<GsiEntryOutcome[] | null> {
		const request: GsiWriteEntriesRequest = {
			producerId: this.#deps.producerId(),
			epoch: batch.epoch,
			ancestorFences: batch.ancestorFences,
			items: entries.map((entry, opIndex) => ({ ...entry, opIndex }) as GsiWriteEntry),
		};
		try {
			const outcomes = await this.#withTimeout(this.#deps.writeGsi(target, request));
			invariant(outcomes.length === entries.length, "fokos/mat: the GSI writer answered a wrong number of outcomes");
			const unknown = outcomes.find((outcome) => outcome.outcome === "unknown");
			if (unknown !== undefined) {
				throw new Error(`an entry has the outcome unknown (${(unknown as { code: string }).code})`);
			}
			return outcomes;
		} catch (error) {
			this.#logError("fokos/mat: a GSI call has no answer", { targetId: target.targetId, epoch: batch.epoch, error: String(error) });
			return null;
		}
	}

	/** Waits for `call` up to `callTimeoutMs`. The timer is cleared when the call answers. */
	async #withTimeout<T>(call: Promise<T>): Promise<T> {
		const timeoutMs = this.#deps.config().callTimeoutMs;
		let timer: ReturnType<typeof setTimeout> | undefined;
		const timeout = new Promise<never>((_, reject) => {
			timer = setTimeout(() => reject(new Error(`the call had no answer after ${timeoutMs} ms`)), timeoutMs);
		});
		try {
			return await Promise.race([call, timeout]);
		} finally {
			clearTimeout(timer);
		}
	}

	/**
	 * Sends one batch. Returns null when the answer is unknown: the call threw, timed out, or answered
	 * the wrong shape. The timer of the timeout is cleared when the call answers: a pending timer keeps
	 * the Durable Object from hibernation, and it is billed for that time.
	 *
	 * A target that does not implement the method answers with an error, so the call is done, and no
	 * item of it can apply later. The call then fails as a whole, and the epoch stays.
	 */
	async #send(target: PushSinkTargetDefinition, batch: ClaimedBatch): Promise<PushTargetResult[] | null> {
		const request: PushTargetBatch = {
			producerId: this.#deps.producerId(),
			epoch: batch.epoch,
			ancestorFences: batch.ancestorFences,
			items: batch.items.map((item) => item.record),
		};
		try {
			const answer = await this.#withTimeout(this.#deps.push(target, request));
			if (!Array.isArray(answer) || answer.length !== batch.items.length || !answer.every(isPushTargetResult)) {
				throw new Error(`the push target answered ${Array.isArray(answer) ? `${answer.length} results` : typeof answer}`);
			}
			return answer;
		} catch (error) {
			if (isMissingRpcMethod(error)) {
				this.#logError("fokos/mat: a push target does not implement its method", {
					targetId: target.targetId,
					binding: target.binding,
					method: target.method,
					error: String(error),
				});
				return batch.items.map(() => ({ outcome: "retry", code: PUSH_METHOD_MISSING }));
			}
			this.#logError("fokos/mat: a push call has no answer", { targetId: target.targetId, epoch: batch.epoch, error: String(error) });
			return null;
		}
	}

	/**
	 * Applies the answer of one batch in one storage transaction. `results` is null when the answer is
	 * unknown. The answer of a call with an epoch below the current epoch is discarded: a call with the
	 * current epoch decides. A router, a target that imports, or a fenced partition makes no transition.
	 */
	#complete(targetId: string, state: TargetState, batch: ClaimedBatch, results: PushTargetResult[] | null): void {
		if (!this.#deps.canRun()) {
			return;
		}
		const { store } = this.#deps;
		const outcome = store.transactionSync((): "discarded" | "unknown" | "failed" | { unblocked: boolean } => {
			const row = store.targetRow(targetId);
			if (batch.epoch < row.epoch) {
				return "discarded";
			}
			// An item of the call can still apply. The next call goes with a higher epoch, so a receiver
			// that gets it first rejects the old call, and a receiver that got the old call first gets
			// the newer call after it. The cursor stays, so the next call carries each item again.
			if (results === null) {
				store.raiseTargetEpoch(targetId);
				return "unknown";
			}
			// The target refused each item, so a `retry` row for each item would not help: the circuit
			// breaker backs off instead, and the cursor stays.
			if (results.every((result) => result.outcome === "retry")) {
				return "failed";
			}
			const owns = this.#deps.ownerCheck();
			let unblocked = false;
			// A key whose delete applied has no entry now. Only this batch changes the key sets of the target.
			for (const item of batch.items) {
				if (item.gsi !== undefined && item.gsi.removed.length > 0) {
					if (store.removeIndexKeys(targetId, item.itemId, item.gsi.removed) && item.deleted) {
						unblocked = true;
					}
				}
			}
			for (const [i, result] of results.entries()) {
				const item = batch.items[i];
				if (owns(item) && this.#applyResult(targetId, item, result)) {
					unblocked = true;
				}
			}
			const cursor = batch.highestSeq !== null && row.cursorSeq === batch.cursor ? batch.highestSeq : row.cursorSeq;
			const now = this.#deps.now();
			const more = store.hasPositionAfter(cursor) || (store.earliestRetryAt(targetId) ?? Number.POSITIVE_INFINITY) <= now;
			if (cursor !== row.cursorSeq || !more) {
				store.updateTarget(targetId, cursor, more);
			}
			store.dropDoneTargetFences(targetId);
			return { unblocked };
		});
		if (outcome === "discarded") {
			return;
		}
		if (outcome === "unknown" || outcome === "failed") {
			this.#recordFailure(targetId, state);
			return;
		}
		state.failures = 0;
		state.probeAt = 0;
		if (outcome.unblocked) {
			this.#deps.cleanup.tombstoneUnblocked();
		}
	}

	/**
	 * The writes for one owned item of an answer. The result is for the revision that the call sent.
	 * When the item changed during the call, the result writes nothing: the new image has a position
	 * above the cursor, or the ingest made the `retry` row of the item due. Returns true when it deleted
	 * the row of a tombstone, which the cleanup can then delete.
	 */
	#applyResult(targetId: string, item: ClaimedItem, result: PushTargetResult): boolean {
		const { store } = this.#deps;
		if (store.itemRevision(item.itemId) !== item.revision) {
			return false;
		}
		const config = this.#deps.config();
		switch (result.outcome) {
			case "applied":
				return store.deleteTargetRetry(targetId, item.itemId) && item.deleted;
			case "retry":
				store.recordTargetRetry(
					targetId,
					item.itemId,
					this.#deps.now(),
					config.retryBackoffMinMs,
					config.retryBackoffMaxMs,
					result.code ?? null,
				);
				return false;
			case "rejected":
				this.#logError("fokos/mat: a target rejected an item", {
					targetId,
					key: KeyCodec.pairForLog(item.hk, item.sk),
					revision: item.revision,
					code: result.code,
				});
				// A tombstone keeps no row, so that the cleanup can delete it. A newer write of a live item
				// sends the item again, and its application deletes the row.
				if (item.deleted) {
					return store.deleteTargetRetry(targetId, item.itemId);
				}
				store.recordTargetViolation(targetId, item.itemId, result.code);
				return false;
		}
	}

	/** A call that failed as a whole. After `breakerFailures` such calls, the target waits for its next probe. */
	#recordFailure(targetId: string, state: TargetState): void {
		const config = this.#deps.config();
		state.failures += 1;
		if (state.failures < config.breakerFailures) {
			return;
		}
		if (state.failures === config.breakerFailures) {
			this.#logError("fokos/mat: the circuit breaker of a target is open", { targetId, failures: state.failures });
		}
		state.probeAt = this.#deps.now() + config.probeIntervalMs;
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

/** The push record of an image: the keys as the caller wrote them, and the decoded value. */
function toPushItem(image: TargetImage): PushTargetItem {
	const keys = decodeItemKeys(image.hk, image.sk);
	const base = {
		baseHashKey: keys.hashKey as HashKey,
		...(keys.sortKey !== undefined ? { baseSortKey: keys.sortKey as SortKey } : {}),
		revision: image.revision,
	};
	if (image.deleted) {
		return { ...base, deleted: true };
	}
	let value: ReadItemValue;
	switch (image.kind) {
		case "json":
			value = { kind: "json", data: JSON.parse(image.data as string) as JsonValue };
			break;
		case "text":
			value = { kind: "text", data: image.data as string };
			break;
		case "bytes":
			value = { kind: "bytes", data: image.data as Uint8Array };
			break;
	}
	return { ...base, deleted: false, value, ...(image.ttlAt !== null ? { ttlAt: image.ttlAt } : {}) };
}

/** The start of each message that workerd gives for a method that the receiver does not implement. */
const MISSING_RPC_METHOD_MESSAGE = /^The RPC receiver does not implement /;

/**
 * True for the error of an RPC call to a method that the receiver does not implement. The receiver
 * runs no code of the target for such a call. workerd gives a `TypeError` with `remote: true`, and
 * the message is not documented, so a test in workerd pins it. When the message changes, the error
 * is an unknown answer, which is safe: the epoch goes up.
 *
 * The match is also safe when a method of the target throws an error with this message: the call
 * is done, so no item of it can apply later.
 */
export function isMissingRpcMethod(error: unknown): boolean {
	return error instanceof TypeError && (error as { remote?: unknown }).remote === true && MISSING_RPC_METHOD_MESSAGE.test(error.message);
}

function isPushTargetResult(value: unknown): value is PushTargetResult {
	if (typeof value !== "object" || value === null) {
		return false;
	}
	const { outcome, code } = value as { outcome?: unknown; code?: unknown };
	switch (outcome) {
		case "applied":
			return true;
		case "retry":
			return code === undefined || typeof code === "string";
		case "rejected":
			return typeof code === "string";
		default:
			return false;
	}
}

export function validateTargetRunnerConfig(config: TargetRunnerConfig): void {
	for (const name of [
		"concurrency",
		"maxItemsPerStep",
		"maxBytesPerStep",
		"maxScannedPositions",
		"retryBackoffMinMs",
		"retryBackoffMaxMs",
		"breakerFailures",
		"probeIntervalMs",
		"callTimeoutMs",
		"fallbackAlarmMs",
	] as const) {
		invariant(Number.isInteger(config[name]) && config[name] > 0, `target-runner: ${name} must be an integer greater than zero`);
	}
	invariant(config.retryBackoffMaxMs >= config.retryBackoffMinMs, "target-runner: retryBackoffMaxMs must be at least retryBackoffMinMs");
}
