/**
 * One durable flow for hash splits, range splits and key promotions.
 *
 * All three move ownership from one source to one or more targets, and all three do the same work:
 * choose what moves, create the targets, change routing, copy the state, collect the acknowledgements
 * and clean the source. They used to run as three state models over two storage keys and a table,
 * which could not arbitrate between each other atomically — a split record and a promotion record for
 * one key could both exist. Here one SQL model holds all three, so one transaction decides every
 * queue request and every cutover.
 *
 * The source and the target run separate state machines:
 *
 *   source: (none) -> queued -> planned -> cutover -> completed -> cleaned
 *   target: fokosInit -> awaiting_data -> importing -> imported -> active
 *
 * The source persists the plan and every target before it calls `fokosInit`, persists `initializing`
 * before each call, and changes routing only once every target is `initialized`. The target commits
 * one bounded page with its cursor at a time, persists `imported` before it acknowledges, and then
 * retries that acknowledgement until the source accepts it.
 *
 * The two roles are two classes, both in this file. They share no in-memory state and never call each
 * other: the only thing they have in common is the `PartitionStore`. A partition holds one of each,
 * because a hash child is a target first and a source later, and the request path joins them at the
 * Durable Object rather than inside either class. Neither holds a stub or makes an RPC of its own —
 * the DO passes `getPeer` down (boundary rule: only DO classes and FokosDB hold stubs).
 *
 * `RepartitionSource.servePage` is the last member of the source and `RepartitionTarget.importOnePage`
 * is the first of the target, so the two ends of the migration protocol stay next to each other across
 * the class boundary — as do the overrides page they build and apply.
 */
import { collectBatch } from "../batch-scan.js";
import invariant from "../../invariant.js";
import { KeyCodec, type KeyBytes } from "../../partition-topology/key-codec.js";
import { isHashPartition, isRangePartition, type PartitionContextLivePartition } from "../../partition-topology/partition-context.js";
import {
	PartitionIdHelper,
	resolveHashChildPartitionContexts,
	resolveRangePartitionContext,
} from "../../partition-topology/partition-id.js";
import type { RangeAncestorInfo } from "../../partition-topology/types.js";
import { selectRangeAncestors } from "../../partition-topology/split-policy.js";
import { FokosInternalError, FokosUnavailableError, FokosError, INTERNAL_CODES, UNAVAILABLE_CODES } from "../../errors.js";
import type {
	PartitionStore,
	PromotedKeyCursor,
	RepartitionKind,
	RepartitionRow,
	RepartitionSlice,
	RepartitionState,
	RepartitionTargetRow,
} from "../partition-store.js";
import type { FokosSlice } from "./repartition-slice.js";
import { sliceIncludesHashKey } from "./repartition-slice.js";
import type {
	FokosImportRecord,
	FokosImportState,
	FokosInitRequest,
	FokosMigrationAckRequest,
	FokosMigrationCursor,
	FokosMigrationPage,
	FokosMigrationPullRequest,
	FokosPartitionRef,
	FokosRepartitionPeer,
	FokosStartImportRequest,
	FokosStatusCursor,
	FokosStatusEntry,
	MigrationHost,
	RepartitionRouting,
} from "./repartition-types.js";
import { jitterBackoff } from "durable-utils/retries";

/**
 * The Workers runtime allows six simultaneous outgoing connections per request, so one source step
 * calls at most six targets. A wider fan-out would queue behind that limit and hold the step open.
 */
export const REPARTITION_RPC_CONCURRENCY = 6;

/** The page budgets of the overrides phase. The host owns its own. */
const PAGE_BYTES = 20 * 1024 * 1024;
/** The same ceiling over an administration page, measured with the estimator below. */
const STATUS_PAGE_BYTES = 20 * 1024 * 1024;
const PAGE_ROWS = 1_000;
const SCAN_ROWS = 10_000;

const SOURCE_RETRY_BASE_MS = 5_000;
const SOURCE_RETRY_MAX_MS = 5 * 60_000;
/** A lock-blocked promotion retries at a flat interval: only a commit or a cancel can change the answer. */
const LOCK_RETRY_MS = 5_000;
const CLEANUP_RETRY_MS = 5_000;
const IMPORT_RETRY_BASE_MS = 10_000;
const IMPORT_RETRY_MAX_MS = 5 * 60_000;
/** The source still owns the slice, so the target waits a flat interval rather than backing off. */
const NOT_CUT_OVER_RETRY_MS = 10_000;
/** A protocol error no retry can fix. The state stays, the identifiers are logged, and the retry is slow. */
const NON_RETRYABLE_RETRY_MS = 5 * 60_000;

export const REPARTITION_KV_KEYS = {
	IMPORT: "__fokos/import",
	DESTROYING: "__fokos/destroying",
	plan: (repartitionId: string) => `__fokos/repartition/${repartitionId}/plan`,
} as const;

/**
 * The immutable part of a repartition, written once with the target rows and deleted at cutover.
 *
 * It holds only what the target rows cannot: the source identity and the range ancestors selected for
 * this split. The computed boundaries are the target slices themselves, so the plan does not repeat
 * them, and the mutable split thresholds are never stored — a router rebuilds every forwarded context
 * from its own current context instead.
 */
export type RepartitionPlan = {
	schema: 1;
	source: FokosPartitionRef;
	/** The depth the targets receive. A range split's children, or 0 for a promotion's range root. */
	rangeDepth?: number;
	rangeAncestors?: RangeAncestorInfo[];
};

export type RepartitionIdentity = {
	pCtx: PartitionContextLivePartition;
	depth: number;
	rangeAncestors: RangeAncestorInfo[];
};

/** What both halves need: the peer factory, the application host, and this partition's own identity. */
export type RepartitionCommonDeps = {
	/** Resolves a peer for one remote participant. Only the DO acquires stubs. */
	getPeer: (ref: FokosPartitionRef) => FokosRepartitionPeer;
	host: MigrationHost;
	/** This partition's own context, depth and range ancestors. */
	identity: () => RepartitionIdentity;
	scheduleWork: () => void;
	logParams: () => Record<string, unknown>;
};

export type RepartitionSourceDeps = RepartitionCommonDeps & {
	/** The boundaries of a range split, or null when the interval cannot yield N non-empty children. */
	computeRangeBoundaries: (hashKey: KeyBytes, start: KeyBytes | null, end: KeyBytes | null, n: number) => KeyBytes[] | null;
	/** Pending locks on one hash key. A promotion must not move a locked key. */
	lockCountForKey: (hashKey: KeyBytes) => number;
	/** Deletes one bounded batch of a promoted key's source rows. Returns whether they are all gone. */
	cleanupStep: (hashKey: KeyBytes) => boolean;
	/** Deletes the source's pending transaction rows once every split target holds its own copies. */
	onSplitCompleted: () => void;
};

export type RepartitionTargetDeps = RepartitionCommonDeps & {
	/** Whether this partition has a stored context at all. A target with one and no import record is a conflict. */
	hasIdentity: () => boolean;
	/** Writes this partition's identity, depth and ancestors. Synchronous: it runs inside the init transaction. */
	applyTargetIdentity: (req: FokosInitRequest) => void;
	ensureAlarmSet: (targetMs: number) => Promise<void>;
};

/** What one source or target step did, so the caller can decide whether to keep going. */
export type StepOutcome = "idle" | "progressed" | "stopped";

/**
 * The source half: the partition that gives ownership away.
 *
 * It arbitrates every queue request, plans the move, creates and initializes the targets, changes
 * routing at cutover, collects the acknowledgements, and reclaims what it no longer owns. It also
 * serves the three requests a target makes of it while that target catches up — a migration page, an
 * acknowledgement, and a read of the slice the target does not hold yet.
 */
export class RepartitionSource implements RepartitionRouting {
	constructor(
		private readonly store: PartitionStore,
		private readonly storage: DurableObjectStorage,
		private readonly deps: RepartitionSourceDeps,
	) {}

	/** The split this source queued, if it ever queued one. */
	splitRepartition(): RepartitionRow | undefined {
		return this.#split()?.row;
	}

	/**
	 * Whether this partition has become a pure router. A split source owns no key from cutover onwards:
	 * its targets do, and serving a local copy would answer with data that has moved on.
	 */
	routerRole(): boolean {
		const row = this.splitRepartition();
		return row !== undefined && row.state !== "queued" && row.state !== "planned";
	}

	/** The split's targets in `target_index` order — the order range children tile their interval. */
	splitTargets(): RepartitionTargetRow[] {
		return this.#split()?.targets ?? [];
	}

	/**
	 * How far the promotion of one hash key has got, or undefined when this partition still owns it.
	 * One indexed join on the point-read path.
	 */
	overrideFor(hashKey: KeyBytes): RepartitionState | undefined {
		return this.store.routeOverrideFor(hashKey)?.state;
	}

	/** Whether the range tree, not this partition, owns the key. True from cutover onwards. */
	ownedByRangeTree(hashKey: KeyBytes): boolean {
		const state = this.overrideFor(hashKey);
		return state === "cutover" || state === "completed" || state === "cleaned";
	}

	/**
	 * The split row and its targets, read from SQL on every call.
	 *
	 * Section 4.3 allows a cache here, and an earlier revision held one. The risk is larger than the
	 * gain. The case worth a cache is ABSENCE, which every leaf hits on every request, and a stale
	 * negative answer is the dangerous direction: a router that believes it is not a router serves its
	 * own rows for keys its targets already own. `getSplitRepartition` costs one seek of a partial
	 * index, which is what the KV read it replaced cost. Add a cache here only with a test that proves
	 * eviction and staleness change nothing.
	 */
	#split(): { row: RepartitionRow; targets: RepartitionTargetRow[] } | null {
		const row = this.store.getSplitRepartition();
		return row ? { row, targets: this.store.listRepartitionTargets(row.id, row.kind) } : null;
	}

	/**
	 * Decides one queue request and writes it, in one transaction that reads every row the decision
	 * depends on. Returns the new repartition, or undefined when arbitration refused it.
	 *
	 * An unfinished promotion blocks a hash split, and a split row in ANY state blocks every later
	 * promotion on that source. Keeping the two mutually exclusive is what removes the need for a
	 * target cancellation protocol and for transaction-wide key-size reservations.
	 */
	queue(request: { kind: RepartitionKind; hashKey?: KeyBytes }, now = Date.now()): RepartitionRow | undefined {
		const { pCtx } = this.deps.identity();
		const row = this.store.transactionSync((): RepartitionRow | undefined => {
			if (this.store.getSplitRepartition()) return undefined;

			switch (request.kind) {
				case "hash_split":
					if (!isHashPartition(pCtx)) return undefined;
					// A promotion that has not finished still owns its key's move; a split would have to
					// abandon or carry it, and neither is possible without a cancellation fence.
					if (this.store.hasUnfinishedPromotion()) return undefined;
					break;
				case "range_split":
					if (!isRangePartition(pCtx)) return undefined;
					break;
				case "key_promotion":
					if (!isHashPartition(pCtx)) return undefined;
					invariant(request.hashKey, "fokos/repartition.queue: a key promotion needs its hash key");
					if (this.store.hasRouteOverride(request.hashKey)) return undefined;
					break;
			}

			const seq = this.store.nextRepartitionSeq();
			const id = `r${seq}`;
			this.store.insertRepartition({
				id,
				seq,
				kind: request.kind,
				state: "queued",
				hashKey: request.hashKey ?? null,
				queuedAt: now,
				nextAttemptAt: now,
			});
			// The override exists from the moment the promotion is queued, so a second request for the
			// same key finds it and no key is ever queued twice.
			if (request.kind === "key_promotion") this.store.insertRouteOverride(request.hashKey!, id);
			return this.store.getRepartition(id);
		});
		return row;
	}

	/**
	 * Advances one due repartition by one bounded step. It selects a single row by
	 * `(next_attempt_at, seq)`, so a row that keeps failing falls behind another due row and cannot
	 * starve it.
	 */
	async sourceStep(now = Date.now()): Promise<StepOutcome> {
		const row = this.store.selectDueRepartition(now);
		if (!row) return "idle";
		switch (row.state) {
			case "queued":
				return this.#plan(row, now);
			case "planned":
				return await this.#advancePlanned(row, now);
			case "cutover":
				return await this.#notifyStart(row, now);
			default:
				return "idle";
		}
	}

	/** Writes the plan, every target row, and `planned`, in one transaction. */
	#plan(row: RepartitionRow, now: number): StepOutcome {
		const { pCtx, depth, rangeAncestors } = this.deps.identity();
		const plan: RepartitionPlan = { schema: 1, source: { partitionId: pCtx.partitionId, doName: pCtx.doName } };
		let targets: Array<{ ref: FokosPartitionRef; slice: RepartitionSlice }>;

		switch (row.kind) {
			case "hash_split": {
				const children = resolveHashChildPartitionContexts(pCtx);
				invariant(children.length === pCtx.hashSplitN, "fokos/repartition.plan: unexpected hash child count");
				targets = children.map((child) => ({
					ref: { partitionId: child.partitionId, doName: child.doName },
					slice: { kind: "hash_child", childIndex: PartitionIdHelper.lastChildIdx(Uint8Array.fromHex(child.partitionId)) },
				}));
				break;
			}
			case "range_split": {
				const rp = pCtx.rangePartition;
				invariant(rp, "fokos/repartition.plan: a range split needs a range identity");
				const n = pCtx.rangeSplitN;
				invariant(n != null && n >= 2, "fokos/repartition.plan: rangeSplitN must be at least 2");
				const boundaries = this.deps.computeRangeBoundaries(rp.hashKey, rp.startBoundary, rp.endBoundary, n);
				if (!boundaries) {
					// A size-triggered split can find fewer than N items in its interval, because each child
					// needs one. Only a new write can change that, so the retry backs off to five minutes.
					this.#deferSource(row, now, jitterBackoff(row.attempts, SOURCE_RETRY_BASE_MS, SOURCE_RETRY_MAX_MS));
					return "progressed";
				}
				const starts: (KeyBytes | null)[] = [rp.startBoundary, ...boundaries];
				const ends: (KeyBytes | null)[] = [...boundaries, rp.endBoundary];
				plan.rangeDepth = depth + 1;
				plan.rangeAncestors = selectRangeAncestors(
					depth,
					rangeAncestors,
					{
						depth,
						startBoundary: rp.startBoundary ?? KeyCodec.encodeOptional(undefined),
						endBoundary: rp.endBoundary ?? KeyCodec.encodeOptional(undefined),
					},
					pCtx.rangeAncestorsConfig,
				);
				targets = starts.map((start, i) => {
					const child = resolveRangePartitionContext(pCtx, rp.hashKey, start, ends[i]).partitionContext;
					return {
						ref: { partitionId: child.partitionId, doName: child.doName },
						slice: { kind: "range", hashKey: rp.hashKey, start, end: ends[i] },
					};
				});
				break;
			}
			case "key_promotion": {
				const hashKey = row.hashKey;
				invariant(hashKey, "fokos/repartition.plan: a key promotion needs its hash key");
				const root = resolveRangePartitionContext(pCtx, hashKey, null, null).partitionContext;
				plan.rangeDepth = 0;
				plan.rangeAncestors = [];
				targets = [{ ref: { partitionId: root.partitionId, doName: root.doName }, slice: { kind: "promoted_key", hashKey } }];
				break;
			}
		}

		const names = new Set(targets.map((t) => t.ref.doName));
		invariant(names.size === targets.length, "fokos/repartition.plan: duplicate target names");

		this.store.transactionSync(() => {
			const current = this.store.getRepartition(row.id);
			if (current?.state !== "queued") return;
			this.storage.kv.put<RepartitionPlan>(REPARTITION_KV_KEYS.plan(row.id), plan);
			targets.forEach((t, index) => {
				this.store.insertRepartitionTarget({
					repartitionId: row.id,
					kind: row.kind,
					partitionId: t.ref.partitionId,
					doName: t.ref.doName,
					targetIndex: index,
					slice: t.slice,
					nextAttemptAt: now,
				});
			});
			this.store.setRepartitionState(row.id, "planned");
			this.store.refreshRepartitionDue(row.id, now);
		});
		return "progressed";
	}

	async #advancePlanned(row: RepartitionRow, now: number): Promise<StepOutcome> {
		const counts = this.store.countRepartitionTargets(row.id);
		if (counts.total > 0 && counts.initialized === counts.total) return this.#cutover(row, now);
		return await this.#initializeTargets(row, now);
	}

	/**
	 * Initializes up to six due targets. Each target moves to `initializing` BEFORE its call, so a
	 * call that is in flight or lost its reply is indistinguishable from one that never started, and
	 * the retry repeats the same idempotent `fokosInit`.
	 */
	async #initializeTargets(row: RepartitionRow, now: number): Promise<StepOutcome> {
		if (row.kind === "key_promotion") {
			const hashKey = row.hashKey;
			invariant(hashKey, "fokos/repartition.initializeTargets: a key promotion needs its hash key");
			if (this.deps.lockCountForKey(hashKey) > 0) {
				// The target stays `pending`. A guarded lock counts too: skipping it would route the key to
				// the range root, and a later forced commit would find no pending row there and lose the write.
				this.#deferTargets(row, now, LOCK_RETRY_MS);
				return "progressed";
			}
		}

		const due = this.store.selectDueTargets(row.id, row.kind, "init", now, REPARTITION_RPC_CONCURRENCY);
		if (due.length === 0) {
			this.store.transactionSync(() => this.store.refreshRepartitionDue(row.id, now));
			return "idle";
		}

		const plan = this.storage.kv.get<RepartitionPlan>(REPARTITION_KV_KEYS.plan(row.id));
		invariant(plan, () => `fokos/repartition.initializeTargets: no plan for ${row.id}`);
		const { pCtx } = this.deps.identity();

		this.store.transactionSync(() => {
			for (const target of due) {
				if (target.initialization !== "pending") continue;
				this.store.setTargetInitialization(row.id, target.partitionId, "initializing", target.attempts, target.nextAttemptAt);
			}
		});

		// allSettled, not all: one unreachable target must not skip the five beside it. Every target
		// keeps its own durable retry state, so a success advances even when a sibling fails.
		const results = await Promise.allSettled(
			due.map(async (target) => {
				const peer = this.deps.getPeer({ partitionId: target.partitionId, doName: target.doName });
				await peer.fokosInit({
					repartitionId: row.id,
					source: pCtx,
					target: this.#targetContext(pCtx, target),
					slice: this.materializeSlice(target.slice),
					...(plan.rangeDepth === undefined ? {} : { rangeDepth: plan.rangeDepth }),
					...(plan.rangeAncestors === undefined ? {} : { rangeAncestors: plan.rangeAncestors }),
				});
			}),
		);

		results.forEach((result, i) => {
			const target = due[i];
			this.store.transactionSync(() => {
				if (result.status === "fulfilled") {
					this.store.setTargetInitialization(row.id, target.partitionId, "initialized", 0, now);
				} else {
					const attempts = target.attempts + 1;
					this.store.setTargetAttempt(row.id, target.partitionId, attempts, now + retryDelay(result.reason, attempts));
				}
				this.store.refreshRepartitionDue(row.id, now);
			});
			if (result.status === "rejected") this.#logStepFailure("fokosInit", row, target, result.reason);
		});
		return "progressed";
	}

	/**
	 * Moves routing to the targets. Every target is `initialized` at this point, and the target rows
	 * hold every routing slice, so the plan is spent and the transaction deletes it.
	 */
	#cutover(row: RepartitionRow, now: number): StepOutcome {
		const outcome = this.store.transactionSync((): StepOutcome => {
			const current = this.store.getRepartition(row.id);
			if (current?.state !== "planned") return "idle";
			const counts = this.store.countRepartitionTargets(row.id);
			if (counts.total === 0 || counts.initialized !== counts.total) return "idle";

			if (current.kind === "key_promotion") {
				// Checked again here, not only before initialization: a lock can appear while the range root
				// is being created, and moving the key then would strand that lock on the wrong partition.
				invariant(current.hashKey, "fokos/repartition.cutover: a key promotion needs its hash key");
				if (this.deps.lockCountForKey(current.hashKey) > 0) {
					this.store.setRepartitionAttempt(row.id, current.attempts, now + LOCK_RETRY_MS);
					return "progressed";
				}
			}

			this.store.setRepartitionState(row.id, "cutover", { cutoverAt: now });
			this.storage.kv.delete(REPARTITION_KV_KEYS.plan(row.id));
			this.store.refreshRepartitionDue(row.id, now);
			return "progressed";
		});
		return outcome;
	}

	/**
	 * Tells up to six targets to start importing. The call is an optimisation: each target has its own
	 * fallback alarm, so an import still starts when none of these calls arrives.
	 */
	async #notifyStart(row: RepartitionRow, now: number): Promise<StepOutcome> {
		const due = this.store.selectDueTargets(row.id, row.kind, "start", now, REPARTITION_RPC_CONCURRENCY);
		if (due.length === 0) {
			this.store.transactionSync(() => this.store.refreshRepartitionDue(row.id, now));
			return "idle";
		}
		const { pCtx } = this.deps.identity();
		const sourceRef: FokosPartitionRef = { partitionId: pCtx.partitionId, doName: pCtx.doName };

		// The retry moves forward before the calls, so a crash in the middle of the fan-out still leaves
		// a deadline that is later than now and the pass cannot spin on the same targets.
		this.store.transactionSync(() => {
			for (const target of due) {
				const attempts = target.attempts + 1;
				this.store.setTargetAttempt(
					row.id,
					target.partitionId,
					attempts,
					now + jitterBackoff(attempts, SOURCE_RETRY_BASE_MS, SOURCE_RETRY_MAX_MS),
				);
			}
			this.store.refreshRepartitionDue(row.id, now);
		});

		const results = await Promise.allSettled(
			due.map(async (target) => {
				const peer = this.deps.getPeer({ partitionId: target.partitionId, doName: target.doName });
				await peer.fokosStartImport({ repartitionId: row.id, source: sourceRef });
			}),
		);

		results.forEach((result, i) => {
			const target = due[i];
			if (result.status === "fulfilled") {
				this.store.transactionSync(() => {
					this.store.setTargetStartNotified(row.id, target.partitionId, now);
					this.store.refreshRepartitionDue(row.id, now);
				});
			} else {
				this.#logStepFailure("fokosStartImport", row, target, result.reason);
			}
		});
		return "progressed";
	}

	/** Runs one cleanup step for one completed repartition, whatever its kind. */
	sourceCleanupStep(now = Date.now()): StepOutcome {
		const row = this.store.selectDueCleanup(now);
		if (!row) return "idle";
		return this.store.transactionSync((): StepOutcome => {
			const current = this.store.getRepartition(row.id);
			if (current?.state !== "completed") return "idle";
			// A split keeps its item rows, so its step reclaims nothing and finishes at once. Only a
			// promotion has rows to give back: its key moved, and the rest of its keys stay here.
			const done = current.kind === "key_promotion" ? this.deps.cleanupStep(current.hashKey!) : true;
			if (done) {
				this.store.setRepartitionState(row.id, "cleaned");
			} else {
				this.store.setRepartitionAttempt(row.id, current.attempts, now + CLEANUP_RETRY_MS);
			}
			return "progressed";
		});
	}

	/**
	 * Tells the source that a pending lock has just gone.
	 *
	 * A promotion cannot move a locked key, so it parks its targets 5 seconds out and asks again. Only
	 * a commit and a cancel change that answer, so only they are worth a signal. Without this call, a
	 * key that is ready to move waits out an interval chosen for polling.
	 */
	onLockReleased(now = Date.now()): void {
		this.store.transactionSync(() => this.store.markPromotionsDueNow(now));
	}

	/** The earliest durable deadline of any source work, or null when the source has none left. */
	sourceDeadline(): number | null {
		const step = this.store.earliestRepartitionDeadline();
		const cleanup = this.store.earliestCleanupDeadline();
		if (step === null) return cleanup;
		if (cleanup === null) return step;
		return Math.min(step, cleanup);
	}

	/**
	 * Adds the depth a stored hash-child slice does not carry. The depth of a hash child is the source
	 * depth plus one, which only the source knows, so it fills it in on the way out.
	 */
	materializeSlice(stored: RepartitionSlice): FokosSlice {
		if (stored.kind !== "hash_child") return stored;
		return { kind: "hash_child", childIndex: stored.childIndex, depth: this.deps.identity().depth + 1 };
	}

	/**
	 * Rebuilds a target's context from this source's CURRENT context and the target's stored immutable
	 * slice. A stored context is a snapshot: forwarding it would hand the target split thresholds an
	 * operator has since changed, and the target would persist those stale values as its own.
	 */
	#targetContext(pCtx: PartitionContextLivePartition, target: RepartitionTargetRow) {
		if (target.slice.kind === "hash_child") {
			const child = resolveHashChildPartitionContexts(pCtx).find((c) => c.partitionId === target.partitionId);
			invariant(child, () => `fokos/repartition: no hash child matches target ${target.partitionId}`);
			return child;
		}
		const slice = target.slice;
		const start = slice.kind === "range" ? slice.start : null;
		const end = slice.kind === "range" ? slice.end : null;
		return resolveRangePartitionContext(pCtx, slice.hashKey, start, end).partitionContext;
	}

	#deferSource(row: RepartitionRow, now: number, delayMs: number): void {
		this.store.transactionSync(() => this.store.setRepartitionAttempt(row.id, row.attempts + 1, now + delayMs));
	}

	/** Holds every target that still needs a call at a flat interval, without counting an attempt. */
	#deferTargets(row: RepartitionRow, now: number, delayMs: number): void {
		this.store.transactionSync(() => {
			for (const target of this.store.listRepartitionTargets(row.id, row.kind)) {
				if (target.initialization === "initialized") continue;
				this.store.setTargetAttempt(row.id, target.partitionId, target.attempts, now + delayMs);
			}
			this.store.refreshRepartitionDue(row.id, now);
		});
	}

	#logStepFailure(operation: string, row: RepartitionRow, target: RepartitionTargetRow, error: unknown): void {
		console.error({
			...this.deps.logParams(),
			message: "fokos/repartition: a source step failed for one target.",
			operation,
			repartitionId: row.id,
			kind: row.kind,
			state: row.state,
			target: { doName: target.doName, partitionId: target.partitionId, index: target.targetIndex },
			attempts: target.attempts + 1,
			error: String(error),
		});
	}

	/**
	 * One bounded page of the administration view, ordered by `(seq, target_index)`.
	 *
	 * Two limits bound a page: the row count the caller asks for, and `maxBytes` over the estimated
	 * serialized size. An unbounded promotion count then cannot build a reply that the RPC layer
	 * refuses. The first entry always goes out, because a page with no entry makes no progress.
	 */
	statusEntries(
		cursor: FokosStatusCursor | null,
		limit: number,
		maxBytes = STATUS_PAGE_BYTES,
	): { entries: FokosStatusEntry[]; nextCursor: FokosStatusCursor | null } {
		const rows = this.store.queryRepartitionStatusPage(cursor, limit);
		const entries: FokosStatusEntry[] = [];
		let bytes = 0;
		for (const r of rows) {
			const entry: FokosStatusEntry = {
				repartition: { id: r.id, seq: r.seq, kind: r.kind, state: r.state },
				target:
					r.targetIndex < 0 || r.partitionId === null || r.doName === null || r.initialization === null
						? null
						: {
								index: r.targetIndex,
								ref: { partitionId: r.partitionId, doName: r.doName },
								initialization: r.initialization,
								acknowledged: r.acknowledged,
							},
			};
			bytes += statusEntryBytes(entry);
			if (bytes > maxBytes && entries.length > 0) break;
			entries.push(entry);
		}
		const last = rows[entries.length - 1];
		// The view is drained when the store ran out of rows AND the byte budget held all of them.
		const drained = entries.length === rows.length && rows.length < limit;
		const nextCursor = !last || drained ? null : { seq: last.seq, targetIndex: last.targetIndex };
		return { entries, nextCursor };
	}

	/**
	 * Records one target's acknowledgement, and completes the repartition once every target has sent
	 * one. Membership is checked before the mark, so an unknown name cannot consume an entry and fail
	 * the count later.
	 */
	acceptAck(req: FokosMigrationAckRequest, now = Date.now()): void {
		this.store.transactionSync(() => {
			const { row } = this.#requireTarget(req.repartitionId, req.target);
			if (row.state === "queued" || row.state === "planned") throw notCutOver(row.id);
			// A repeated acknowledgement in cutover, completed or cleaned is a success: the target retries
			// until the source answers, and it cannot know which attempt landed.
			if (row.state !== "cutover") return;

			this.store.setTargetAcknowledged(row.id, req.target.partitionId);
			const counts = this.store.countRepartitionTargets(row.id);
			if (counts.acknowledged < counts.total) return;

			this.store.setRepartitionState(row.id, "completed", { completedAt: now });
			// Every target now holds the authoritative copy of its own locks, so the source's are
			// redundant. A promotion moved one key of many and must not touch the rest.
			if (row.kind !== "key_promotion") this.deps.onSplitCompleted();
			this.store.setRepartitionAttempt(row.id, 0, now);
		});
		this.deps.scheduleWork();
	}

	/**
	 * The slice a read-through caller owns. The source answers only for it, so a target cannot read a
	 * sibling's keys or keys this partition has already given to a range tree.
	 */
	resolveCallerSlice(repartitionId: string, caller: FokosPartitionRef): FokosSlice {
		const { row, target } = this.#requireTarget(repartitionId, caller);
		if (row.state === "queued" || row.state === "planned") throw notCutOver(row.id);
		// A split source keeps its item rows for life, so it can still answer a read from them. A
		// promotion gives the rows of its key back after every target acknowledges. From that moment
		// its local copies are stale or already gone.
		//
		// The state machines make this case unreachable. A promotion has one target. It reaches
		// `completed` only after that target acknowledges, and a target acknowledges only from
		// `imported`, where it serves its own reads and reads through no more. The guard stays because
		// this method answers the one read path with no lifecycle gate, so a wrong answer here is
		// silent. The error is internal and not retryable, because a reclaimed slice never comes back
		// and a caller that retried would loop until it gave up.
		if (row.kind === "key_promotion" && (row.state === "completed" || row.state === "cleaned")) {
			throw new FokosInternalError(INTERNAL_CODES.repartition_slice_reclaimed, {
				message: "the promoted key's rows have been reclaimed; read it through the range tree",
				attributes: { repartitionId: row.id, state: row.state },
			});
		}
		return this.materializeSlice(target.slice);
	}

	#requireTarget(repartitionId: string, ref: FokosPartitionRef): { row: RepartitionRow; target: RepartitionTargetRow } {
		const row = this.store.getRepartition(repartitionId);
		if (!row) {
			throw new FokosInternalError(INTERNAL_CODES.repartition_unknown, {
				message: "no repartition with this id on this partition",
				attributes: { repartitionId, caller: ref.doName },
			});
		}
		const target = this.store.getRepartitionTarget(repartitionId, ref.partitionId, row.kind);
		// Both halves must match. A doName alone is a value the caller chose, and an id alone does not
		// prove which DO is asking.
		if (!target || target.doName !== ref.doName) {
			throw new FokosInternalError(INTERNAL_CODES.repartition_target_unknown, {
				message: "the caller is not a target of this repartition",
				attributes: { repartitionId, caller: ref.doName, callerPartitionId: ref.partitionId },
			});
		}
		return { row, target };
	}

	/**
	 * Serves one bounded page. The checks run in order, and each one names a different condition the
	 * caller must handle: an unknown repartition or target is a protocol defect, a source before
	 * cutover still owns the slice and will serve later, and a source past completion may already have
	 * given the rows back.
	 */
	servePage(req: FokosMigrationPullRequest): FokosMigrationPage {
		const { row, target } = this.#requireTarget(req.repartitionId, req.target);
		if (row.state === "queued" || row.state === "planned") throw notCutOver(row.id);
		if (row.state === "completed" || row.state === "cleaned") {
			throw new FokosUnavailableError(UNAVAILABLE_CODES.partition_migrating, {
				message: "the repartition is complete and the source no longer serves its pages",
				attributes: { repartitionId: row.id, state: row.state },
			});
		}

		const slice = this.materializeSlice(target.slice);
		const cursor: FokosMigrationCursor = req.cursor ?? { phase: "overrides", inner: null };
		if (cursor.phase === "overrides") return this.#buildOverridesPage(row, slice, cursor.inner);

		const { page, nextCursor } = this.deps.host.buildPage(cursor.inner, slice);
		return { phase: "host", page, nextCursor: nextCursor === null ? null : { phase: "host", inner: nextCursor } };
	}

	/**
	 * The route overrides inside the target's slice. Only a hash split has any to give: a range or
	 * promoted-key slice is itself inside a range tree, which holds no overrides of its own.
	 */
	#buildOverridesPage(row: RepartitionRow, slice: FokosSlice, inner: PromotedKeyCursor | null): FokosMigrationPage {
		if (row.kind !== "hash_split") return { phase: "overrides", overrides: [], nextCursor: { phase: "host", inner: null } };

		const n = this.deps.identity().pCtx.hashSplitN;
		const { rows, nextCursor } = collectBatch<{ hashKey: KeyBytes }, PromotedKeyCursor>({
			fetchPage: (c, pageSize) => this.store.queryTerminalRouteOverridesPage(c, pageSize),
			advanceCursor: (r) => ({ hashKey: r.hashKey }),
			include: (r) => sliceIncludesHashKey(slice, r.hashKey, n),
			estimateBytes: (r) => r.hashKey.byteLength + 64,
			budgetBytes: PAGE_BYTES,
			maxItems: PAGE_ROWS,
			maxScannedRows: SCAN_ROWS,
			pageSize: PAGE_ROWS,
			startCursor: inner,
		});
		return {
			phase: "overrides",
			overrides: rows,
			nextCursor: nextCursor ? { phase: "overrides", inner: nextCursor } : { phase: "host", inner: null },
		};
	}
}

/**
 * The target half: the partition that receives ownership.
 *
 * It is created by `fokosInit`, pulls one bounded page at a time until its copy is complete, persists
 * `imported` BEFORE it acknowledges, and then retries that acknowledgement until its source accepts
 * it. It never reads a repartition row of its own; the only row it writes is the promotion it adopts
 * from an overrides page, which its own source half reads later through the same store.
 */
export class RepartitionTarget {
	constructor(
		private readonly store: PartitionStore,
		private readonly storage: DurableObjectStorage,
		private readonly deps: RepartitionTargetDeps,
	) {}

	/**
	 * Pulls one page and commits it with its cursor. One step applies at most one page and never
	 * prefetches: two decoded 20 MiB pages would not fit beside each other in a 128 MB isolate, and the
	 * durable cursor stays the only record of progress.
	 */
	async importOnePage(now = Date.now()): Promise<StepOutcome> {
		const rec = this.importRecord();
		if (!rec || rec.state === "imported" || rec.state === "active") return "idle";
		if (rec.nextAttemptAt > now) return "idle";

		const { pCtx } = this.deps.identity();
		const peer = this.deps.getPeer({ partitionId: rec.source.partitionId, doName: rec.source.doName });
		let page: FokosMigrationPage;
		try {
			page = await peer.fokosMigrationPull({
				repartitionId: rec.repartitionId,
				target: { partitionId: pCtx.partitionId, doName: pCtx.doName },
				cursor: rec.cursor,
			});
		} catch (error) {
			this.#deferImport(rec, now, error);
			return "stopped";
		}

		const requested: FokosMigrationCursor = rec.cursor ?? { phase: "overrides", inner: null };
		// Checked before the transaction opens: a page of the wrong phase, or one whose cursor moves
		// backwards, must change nothing at all.
		if (page.phase !== requested.phase) {
			this.#deferImport(rec, now, new Error(`asked for the ${requested.phase} phase and received ${page.phase}`));
			return "stopped";
		}
		if (page.nextCursor !== null && PHASE_ORDER[page.nextCursor.phase] < PHASE_ORDER[requested.phase]) {
			this.#deferImport(rec, now, new Error(`the page cursor moves back from ${requested.phase} to ${page.nextCursor.phase}`));
			return "stopped";
		}
		if (page.phase === "host") {
			try {
				this.deps.host.validatePage(
					requested.inner,
					page.page,
					page.nextCursor?.phase === "host" ? page.nextCursor.inner : null,
				);
			} catch (error) {
				this.#deferImport(rec, now, error);
				return "stopped";
			}
		}

		const applied = this.store.transactionSync(() => {
			// The page outlived the decision to fetch it. A second run of this import, or this instance
			// revived after eviction, can hold a page the durable cursor has already moved past; applying
			// it would re-insert rows a user deleted after the import finished.
			const current = this.importRecord();
			if (!current || current.state === "imported" || current.state === "active") return false;
			if (current.repartitionId !== rec.repartitionId || !cursorsEqual(current.cursor, rec.cursor)) return false;

			if (page.phase === "overrides") this.#applyOverrides(page.overrides, now);
			else this.deps.host.applyPage(page.page, current.slice);

			this.#putImport({
				...current,
				state: page.nextCursor === null ? "imported" : "importing",
				cursor: page.nextCursor,
				attempts: 0,
				nextAttemptAt: now,
				updatedAt: now,
			});
			return true;
		});

		if (!applied) {
			console.warn({
				...this.deps.logParams(),
				message: "fokos/repartition: the durable import moved on while a page was in flight; dropping it.",
				repartitionId: rec.repartitionId,
				phase: page.phase,
			});
			return "stopped";
		}
		return "progressed";
	}

	/**
	 * Adopts the promotions the source already finished, for the keys this target now owns.
	 *
	 * The child receives the forward pointer and no item copy: the data lives in a range tree that
	 * neither partition owns. The row exists so routing, status and destroy traversal can all see the
	 * link — which is why it is written as a finished promotion rather than as a bare override.
	 */
	#applyOverrides(overrides: readonly { hashKey: KeyBytes }[], now: number): void {
		if (overrides.length === 0) return;
		const { pCtx } = this.deps.identity();
		for (const { hashKey } of overrides) {
			if (this.store.hasRouteOverride(hashKey)) continue;
			const seq = this.store.nextRepartitionSeq();
			const id = `r${seq}`;
			this.store.insertRepartition({
				id,
				seq,
				kind: "key_promotion",
				state: "cleaned",
				hashKey,
				queuedAt: now,
				cutoverAt: now,
				completedAt: now,
				nextAttemptAt: now,
			});
			const root = resolveRangePartitionContext(pCtx, hashKey, null, null).partitionContext;
			this.store.insertRepartitionTarget({
				repartitionId: id,
				kind: "key_promotion",
				partitionId: root.partitionId,
				doName: root.doName,
				targetIndex: 0,
				slice: { kind: "promoted_key", hashKey },
				initialization: "initialized",
				startNotified: true,
				acknowledged: true,
				nextAttemptAt: now,
			});
			this.store.insertRouteOverride(hashKey, id);
		}
	}

	importRecord(): FokosImportRecord | undefined {
		return this.storage.kv.get<FokosImportRecord>(REPARTITION_KV_KEYS.IMPORT);
	}

	/** The import state the request gate reads, or null when this partition is not a target. */
	importState(): FokosImportState | null {
		return this.importRecord()?.state ?? null;
	}

	/** True while this partition's copy is incomplete: it cannot serve its own reads or accept a write. */
	isImporting(): boolean {
		const state = this.importState();
		return state === "awaiting_data" || state === "importing";
	}

	/**
	 * Creates this partition as a target, or confirms an identical earlier call.
	 *
	 * It restores the fallback alarm every time, including on a retry, because a lost reply leaves the
	 * source believing the target is initialized while the target may have no alarm to start itself.
	 */
	async initAsTarget(req: FokosInitRequest, now = Date.now()): Promise<void> {
		const existing = this.importRecord();
		if (existing) {
			this.#assertInitMatches(existing, req);
			// The policy inside the context is mutable and the source may have newer values; the identity
			// and the slice are not, and the check above has already proved they are unchanged.
			this.store.transactionSync(() => {
				this.deps.applyTargetIdentity(req);
				this.#putImport({ ...existing, source: req.source, updatedAt: now });
			});
		} else {
			if (this.deps.hasIdentity()) {
				throw new FokosInternalError(INTERNAL_CODES.partition_context_mismatch, {
					message: "this partition already has a context and is not a target of any import",
					attributes: { repartitionId: req.repartitionId, target: req.target.doName },
				});
			}
			this.store.transactionSync(() => {
				this.deps.applyTargetIdentity(req);
				this.#putImport({
					schema: 1,
					state: "awaiting_data",
					repartitionId: req.repartitionId,
					source: req.source,
					slice: req.slice,
					cursor: null,
					attempts: 0,
					nextAttemptAt: now,
					updatedAt: now,
				});
			});
		}
		// The alarm only. The source is still `planned` here, because it cuts over after EVERY target is
		// initialized. A pull now would earn a `repartition_not_cut_over` and put this target behind a
		// retry deadline for no reason. `fokosStartImport` starts the import, and the alarm starts it
		// when that call never arrives.
		await this.deps.ensureAlarmSet(now + IMPORT_RETRY_BASE_MS);
	}

	#assertInitMatches(existing: FokosImportRecord, req: FokosInitRequest): void {
		const sameSlice = slicesEqual(existing.slice, req.slice);
		if (
			existing.repartitionId !== req.repartitionId ||
			existing.source.partitionId !== req.source.partitionId ||
			existing.source.doName !== req.source.doName ||
			!sameSlice
		) {
			throw new FokosInternalError(INTERNAL_CODES.partition_context_mismatch, {
				message: "fokosInit conflicts with the import this partition already holds",
				attributes: {
					repartitionId: [existing.repartitionId, req.repartitionId],
					source: [existing.source.doName, req.source.doName],
					sliceMatches: sameSlice,
				},
			});
		}
	}

	/** Asks this target to begin importing now, instead of waiting for its own fallback alarm. */
	async startImport(req: FokosStartImportRequest, now = Date.now()): Promise<void> {
		const rec = this.importRecord();
		if (!rec || rec.repartitionId !== req.repartitionId) {
			throw new FokosInternalError(INTERNAL_CODES.repartition_unknown, {
				message: "this partition holds no import for that repartition",
				attributes: { repartitionId: req.repartitionId },
			});
		}
		if (rec.source.partitionId !== req.source.partitionId || rec.source.doName !== req.source.doName) {
			throw new FokosInternalError(INTERNAL_CODES.partition_context_mismatch, {
				message: "fokosStartImport came from a different source than the stored one",
				attributes: { repartitionId: req.repartitionId, stored: rec.source.doName, received: req.source.doName },
			});
		}
		if (rec.state === "imported" || rec.state === "active") return;
		// The source has cut over. That is new information, so it clears the deadline this target sits
		// behind. Without it, a target that pulled too early waits out a backoff it earned before the
		// source was ready.
		this.store.transactionSync(() => {
			const current = this.importRecord();
			if (!current || current.state === "imported" || current.state === "active") return;
			this.#putImport({ ...current, attempts: 0, nextAttemptAt: now, updatedAt: now });
		});
		await this.deps.ensureAlarmSet(now + IMPORT_RETRY_BASE_MS);
		this.deps.scheduleWork();
	}

	/**
	 * One acknowledgement attempt. `imported` is already durable, so a failure here costs nothing: the
	 * target keeps serving its complete copy and tries again.
	 */
	async sendAck(now = Date.now()): Promise<StepOutcome> {
		const rec = this.importRecord();
		if (!rec || rec.state !== "imported") return "idle";
		if (rec.nextAttemptAt > now) return "idle";

		const { pCtx } = this.deps.identity();
		const peer = this.deps.getPeer({ partitionId: rec.source.partitionId, doName: rec.source.doName });
		try {
			await peer.fokosMigrationAck({
				repartitionId: rec.repartitionId,
				target: { partitionId: pCtx.partitionId, doName: pCtx.doName },
			});
		} catch (error) {
			this.#deferImport(rec, now, error);
			return "stopped";
		}
		this.store.transactionSync(() => {
			const current = this.importRecord();
			if (current?.state !== "imported") return;
			this.#putImport({ ...current, state: "active", attempts: 0, nextAttemptAt: now, updatedAt: now });
		});
		return "progressed";
	}

	/** The target's own next deadline, or null when it has no import work left. */
	importDeadline(): number | null {
		const rec = this.importRecord();
		if (!rec || rec.state === "active") return null;
		return rec.nextAttemptAt;
	}

	#putImport(record: FokosImportRecord): void {
		this.storage.kv.put<FokosImportRecord>(REPARTITION_KV_KEYS.IMPORT, record);
	}

	#deferImport(rec: FokosImportRecord, now: number, error: unknown): void {
		const attempts = rec.attempts + 1;
		const delay = retryDelay(error, attempts, IMPORT_RETRY_BASE_MS, IMPORT_RETRY_MAX_MS, NOT_CUT_OVER_RETRY_MS);
		this.store.transactionSync(() => {
			const current = this.importRecord();
			if (!current || current.state === "active") return;
			this.#putImport({ ...current, attempts, nextAttemptAt: now + delay, updatedAt: now });
		});
		console.error({
			...this.deps.logParams(),
			message: "fokos/repartition: an import step failed.",
			repartitionId: rec.repartitionId,
			source: rec.source.doName,
			phase: rec.cursor?.phase ?? "overrides",
			cursor: rec.cursor,
			attempts,
			nextAttemptAt: now + delay,
			error: String(error),
		});
	}
}

function retryDelay(error: unknown, attempts: number, base = SOURCE_RETRY_BASE_MS, max = SOURCE_RETRY_MAX_MS, flat?: number): number {
	if (flat !== undefined && FokosError.isCode(error, UNAVAILABLE_CODES.repartition_not_cut_over)) return flat;
	// A protocol defect no retry can fix still keeps its state and its identifiers; it simply waits
	// long enough that it costs nothing while an operator looks at the log.
	if (FokosError.isCode(error, INTERNAL_CODES.repartition_unknown) || FokosError.isCode(error, INTERNAL_CODES.repartition_target_unknown)) {
		return NON_RETRYABLE_RETRY_MS;
	}
	return jitterBackoff(attempts, base, max);
}

const PHASE_ORDER: Record<FokosMigrationCursor["phase"], number> = { overrides: 0, host: 1 };

function notCutOver(repartitionId: string): FokosUnavailableError {
	return new FokosUnavailableError(UNAVAILABLE_CODES.repartition_not_cut_over, {
		message: "the repartition source still owns this slice; retry after cutover",
		attributes: { repartitionId },
	});
}

/*
 * A conservative serialized size for one status entry. It counts fixed overhead for the keys and the
 * short enumerated values, plus 2 bytes for every character of the three identifiers. Those three are
 * the only fields whose length the partition does not choose. This estimate must never under-count.
 */
function statusEntryBytes(entry: FokosStatusEntry): number {
	const ids = entry.repartition.id.length + (entry.target ? entry.target.ref.doName.length + entry.target.ref.partitionId.length : 0);
	return 256 + 2 * ids;
}

/** Compares two migration cursors by value. Both ends survive a KV structured-clone round trip. */
function cursorsEqual(a: FokosMigrationCursor | null, b: FokosMigrationCursor | null): boolean {
	if (a === null || b === null) return a === b;
	if (a.phase !== b.phase) return false;
	if (a.phase === "overrides" && b.phase === "overrides") {
		if (a.inner === null || b.inner === null) return a.inner === b.inner;
		return KeyCodec.compare(a.inner.hashKey, b.inner.hashKey) === 0;
	}
	// The host cursor is opaque, so it is compared as its serialized form rather than field by field.
	return JSON.stringify(a.inner ?? null) === JSON.stringify(b.inner ?? null);
}

function slicesEqual(a: FokosSlice, b: FokosSlice): boolean {
	if (a.kind !== b.kind) return false;
	const keyEq = (x: KeyBytes | null, y: KeyBytes | null) => (x === null || y === null ? x === y : KeyCodec.compare(x, y) === 0);
	switch (a.kind) {
		case "hash_child":
			return b.kind === "hash_child" && a.childIndex === b.childIndex && a.depth === b.depth;
		case "promoted_key":
			return b.kind === "promoted_key" && keyEq(a.hashKey, b.hashKey);
		case "range":
			return b.kind === "range" && keyEq(a.hashKey, b.hashKey) && keyEq(a.start, b.start) && keyEq(a.end, b.end);
	}
}
