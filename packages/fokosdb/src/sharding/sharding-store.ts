/**
 * The durable state of the sharding layer: every SQL table with the prefix `fokos_` and every KV key
 * under `__fokos/`. No other class reads or writes them, and the host's own migration runner must not
 * name a `fokos_` table.
 *
 * The store runs its own schema migrations, tracked under `__fokos/schema_version`, so it can share
 * one `DurableObjectStorage` with a host that runs migrations of its own.
 *
 * Design rules:
 * - Single-purpose methods named for intent; raw SQL is fine because it lives only here.
 * - The CALLER composes multi-statement atomicity with `transactionSync`. The store does not decide
 *   the transaction boundaries.
 * - Row-reading methods return `KeyBytes`, never ArrayBuffer.
 */
import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import invariant from "../shared/invariant.js";
import { one, tryOne } from "../shared/sql-cursor.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import type { HashTopologySnapshot } from "./hash-topology.js";
import type { PartialRangeTopologySnapshot } from "./partial-range-topology.js";
import type { FokosPartitionIdentity, FokosStoredPolicy } from "./route-context.js";
import type { FokosImportRecord, FokosStoredRepartitionPlan } from "./repartition-types.js";

// ---------------------------------------------------------------------------
// KV keys
// ---------------------------------------------------------------------------

export const FOKOS_KV_KEYS = {
	/** `FokosPartitionIdentity`, written once at bootstrap or `fokosInit`. */
	IDENTITY: "__fokos/identity",
	/** `FokosStoredPolicy`: the mutable part of the last route context this partition received. */
	POLICY: "__fokos/policy",
	/** `FokosImportRecord`: the whole durable import state of a target. */
	IMPORT: "__fokos/import",
	/** `true` after `fokosPrepareDestroy` fences the partition. */
	DESTROYING: "__fokos/destroying",
	/** `FokosJobsRecord`: the next run of every job that a request or a pass scheduled. */
	JOBS: "__fokos/jobs",
	/** The byte-bounded hash topology cache of a hash router. */
	HASH_ARENA: "__fokos/cache/hash_arena",
	/** The byte-bounded Bloom filter of promoted keys a hash partition has learned. */
	PROMOTION_BLOOM: "__fokos/cache/promotion_bloom",
	/** The last sharding schema migration that ran. */
	SCHEMA_VERSION: "__fokos/schema_version",
	/** The head of the plan chain of one repartition, written at queue time and deleted by the final cleanup. */
	planHead: (repartitionId: string) => `__fokos/repartition/${repartitionId}/plan/00000001`,
} as const;

/** `__fokos/jobs`: the next durable run of each job by name. A job without an entry has no scheduled run. */
export type FokosJobsRecord = Record<string, { nextRunAt: number }>;

// ---------------------------------------------------------------------------
// Row and cursor types
// ---------------------------------------------------------------------------

/** What one repartition moves: every key, one sort-key interval, or one hash key. */
export type RepartitionKind = "hash_split" | "range_split" | "key_promotion";

/**
 * The source lifecycle, identical for all three kinds. `completed` means every target acknowledged
 * and source cleanup is pending; `cleaned` means that cleanup finished and is terminal. A split has
 * nothing to reclaim, so it passes through `completed` and reaches `cleaned` on the next pass.
 */
export type RepartitionState = "queued" | "planned" | "cutover" | "completed" | "cleaned";

/** Where one target stands in the source's initialization fan-out. */
export type TargetInitialization = "pending" | "initializing" | "initialized";

/**
 * The stored part of a target's slice. A hash child carries no depth: its depth is the source depth
 * plus one, which only the source knows, so the flow adds it when it builds an import record or a
 * migration filter.
 */
export type RepartitionSlice =
	| { kind: "hash_child"; childIndex: number }
	| { kind: "range"; hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null }
	| { kind: "promoted_key"; hashKey: KeyBytes };

export type RepartitionRow = {
	id: string;
	seq: number;
	kind: RepartitionKind;
	state: RepartitionState;
	/** The promoted key of a `key_promotion`; null for a split. */
	hashKey: KeyBytes | null;
	// The lifecycle stamps, in the order the state machine reaches them.
	queuedAt: number;
	cutoverAt: number | null;
	completedAt: number | null;
	attempts: number;
	nextAttemptAt: number;
};

export type RepartitionTargetRow = {
	repartitionId: string;
	partitionId: string;
	doName: string;
	targetIndex: number;
	slice: RepartitionSlice;
	initialization: TargetInitialization;
	startNotified: boolean;
	acknowledged: boolean;
	attempts: number;
	nextAttemptAt: number;
};

/** Used by each source step to decide whether the repartition can advance. */
export type RepartitionTargetCounts = {
	total: number;
	initialized: number;
	startNotified: number;
	acknowledged: number;
};

/** One `(repartition, target)` pair of the paginated administration view; `targetIndex` −1 means no target. */
export type RepartitionStatusRow = {
	id: string;
	seq: number;
	kind: RepartitionKind;
	state: RepartitionState;
	/** The key a promotion moves. Null for a split. */
	hashKey: KeyBytes | null;
	targetIndex: number;
	partitionId: string | null;
	doName: string | null;
	initialization: TargetInitialization | null;
	acknowledged: boolean;
};

export type RepartitionStatusCursor = { seq: number; targetIndex: number };

/** Scan checkpoint of the route-override stream, in `hash_key` order. */
export type PromotedKeyCursor = { hashKey: KeyBytes };

/** The deepest learned range slice that contains a sort key. `null` boundaries are unbounded edges. */
export type LearnedRangeSlice = { depth: number; startBoundary: KeyBytes | null; endBoundary: KeyBytes | null };

export type FokosShardingStoreOptions = {
	/** The row bound of `fokos_range_hierarchy`. Default: 10,000. */
	rangeHierarchyMaxRows?: number;
};

/** The default row bound of the learned range hierarchy. A row holds two boundary keys and one hash key. */
export const RANGE_HIERARCHY_MAX_ROWS = 10_000;

/**
 * A learn refreshes `learned_at` only when the row is older than this. Every forwarded request learns
 * the boundaries of the partition that answered, so an unconditional refresh would be one write per
 * forward; this keeps a hot row at one seek and no write.
 */
const RANGE_HIERARCHY_REFRESH_MS = 60_000;

/**
 * A repartition row has a source step to run when this predicate holds, and the alarm and the due-row
 * selection must agree on it. A `cutover` row whose targets are all notified waits for their
 * acknowledgements instead, which arrive as RPCs — selecting it would count an empty step as progress
 * and reschedule the pass forever.
 */
const REPARTITION_HAS_STEP = `(
	r.state IN ('queued', 'planned')
	OR (r.state = 'cutover'
	    AND EXISTS (SELECT 1 FROM fokos_repartition_targets t WHERE t.repartition_id = r.id AND t.start_notified = 0))
)`;

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/**
 * SQLite returns BLOB key columns as ArrayBuffer (or Uint8Array). Materialize them as KeyBytes via a
 * trusted re-brand — they were written as canonical bytes, so this is the asKeyBytes path.
 */
function fromSqlKey(value: ArrayBuffer | Uint8Array): KeyBytes {
	return KeyCodec.asKeyBytes(value instanceof Uint8Array ? value : new Uint8Array(value));
}

const REPARTITION_SELECT = `SELECT r.id, r.seq, r.kind, r.state, r.queued_at, r.cutover_at, r.completed_at,
	r.attempts, r.next_attempt_at, r.hash_key FROM fokos_repartitions r`;

const TARGET_SELECT = `SELECT repartition_id, target_index, initialization, start_notified, acknowledged, attempts, next_attempt_at,
	slice_child_idx, partition_id, do_name, slice_hash_key, slice_start, slice_end FROM fokos_repartition_targets`;

/** The slice kind each repartition kind hands its targets. The one place the relation is written down. */
const SLICE_KIND_OF: Record<RepartitionKind, RepartitionSlice["kind"]> = {
	hash_split: "hash_child",
	range_split: "range",
	key_promotion: "promoted_key",
};

type SqlRepartitionRow = {
	id: string;
	seq: number;
	kind: RepartitionKind;
	state: RepartitionState;
	queued_at: number;
	cutover_at: number | null;
	completed_at: number | null;
	attempts: number;
	next_attempt_at: number;
	hash_key: ArrayBuffer | null;
};

type SqlTargetRow = {
	repartition_id: string;
	target_index: number;
	initialization: TargetInitialization;
	start_notified: number;
	acknowledged: number;
	attempts: number;
	next_attempt_at: number;
	slice_child_idx: number | null;
	partition_id: string;
	do_name: string;
	slice_hash_key: ArrayBuffer | null;
	slice_start: ArrayBuffer | null;
	slice_end: ArrayBuffer | null;
};

function toRepartitionRow(r: SqlRepartitionRow): RepartitionRow {
	return {
		id: r.id,
		seq: r.seq,
		kind: r.kind,
		state: r.state,
		hashKey: r.hash_key === null ? null : fromSqlKey(r.hash_key),
		queuedAt: r.queued_at,
		cutoverAt: r.cutover_at,
		completedAt: r.completed_at,
		attempts: r.attempts,
		nextAttemptAt: r.next_attempt_at,
	};
}

function toTargetSlice(r: SqlTargetRow, kind: RepartitionKind): RepartitionSlice {
	switch (SLICE_KIND_OF[kind]) {
		case "hash_child":
			invariant(r.slice_child_idx !== null, "fokos/sharding-store: hash_child target slice has no child index");
			return { kind: "hash_child", childIndex: r.slice_child_idx };
		case "range":
			invariant(r.slice_hash_key !== null, "fokos/sharding-store: range target slice has no hash key");
			return {
				kind: "range",
				hashKey: fromSqlKey(r.slice_hash_key),
				start: r.slice_start === null ? null : fromSqlKey(r.slice_start),
				end: r.slice_end === null ? null : fromSqlKey(r.slice_end),
			};
		case "promoted_key":
			invariant(r.slice_hash_key !== null, "fokos/sharding-store: promoted_key target slice has no hash key");
			return { kind: "promoted_key", hashKey: fromSqlKey(r.slice_hash_key) };
	}
}

function toTargetRow(r: SqlTargetRow, kind: RepartitionKind): RepartitionTargetRow {
	return {
		repartitionId: r.repartition_id,
		partitionId: r.partition_id,
		doName: r.do_name,
		targetIndex: r.target_index,
		slice: toTargetSlice(r, kind),
		initialization: r.initialization,
		startNotified: r.start_notified === 1,
		acknowledged: r.acknowledged === 1,
		attempts: r.attempts,
		nextAttemptAt: r.next_attempt_at,
	};
}

// ---------------------------------------------------------------------------
// Schema migrations
// ---------------------------------------------------------------------------

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "The repartition plan, its targets, the route overrides of promoted keys, and the learned range hierarchy",
		// One durable model carries hash splits, range splits and key promotions, so one transaction can
		// arbitrate between them. Two separate models cannot: each one decides without seeing the other,
		// and a split record and a promotion record can then exist at the same time for one key.
		//
		// Column order in both rowid tables: SQLite reads a record from the start until it has every
		// column the query needs, so the wide columns go LAST and a query that reads only the small ones
		// never touches their overflow pages. The wide columns are the keys and the derived names: a hash
		// key reaches MAX_HASH_KEY_BYTES, and a range partition_id and doName both encode the two
		// boundaries, so each can be kilobytes. The columns the hot paths read — the due-row scan, the
		// alarm deadline, and the target counts — all sit before them. The order of a PRIMARY KEY or
		// UNIQUE declaration is independent of it, so the indexes are unaffected.
		//
		// fokos_repartitions:
		// - `id` is `r<seq>`, local to this partition. `seq` comes from MAX(seq) + 1 and never repeats,
		//   because rows are permanent. The source doName plus this id is a global identity.
		// - The lifecycle stamps are declared in the order the state machine reaches them.
		// - Every kind ends the same way: `completed` means every target acknowledged and source cleanup
		//   is pending, and `cleaned` means that cleanup finished. A split reclaims no item rows, so its
		//   cleanup step deletes nothing and reports itself done, which keeps one code path for all three
		//   kinds. No separate "cleanup has started" flag: `completed` already implies it.
		// - `hash_key` is the promoted key of a key_promotion, and NULL for a split: a hash split moves
		//   every key, and a range split moves an interval of the key the partition identity already holds.
		// - `next_attempt_at` is the deadline of this row's NEXT source step, so the due-row scan and the
		//   alarm both read one column. idx_fokos_repartitions_due serves that ordered scan.
		// - idx_fokos_repartitions_split makes "does a split row exist?" — asked by every arbitration —
		//   one partial-index seek rather than a scan of every promotion row.
		//
		// fokos_repartition_targets:
		// - The slice is columns, not one blob: migration filters rows by it and range routing orders
		//   children by it, and both are SQL.
		// - No slice_kind column. A repartition never mixes slice kinds, so the parent row's `kind` gives
		//   it: hash_split means hash_child, range_split means range, key_promotion means promoted_key.
		//   Storing it again on every target would also make a range slice with two unbounded edges
		//   indistinguishable from a promoted_key slice by its columns alone.
		// - `target_index` defines target order. Range partition IDs do not sort by boundary.
		// - No depth column for a hash_child slice: the depth is the source depth plus one, and the
		//   target partition_id already encodes it.
		//
		// fokos_route_overrides:
		// - The forward pointer for one hash key whose data now lives in a range tree. It joins to its
		//   repartition row for the state, so a point lookup on the routing path is one indexed join.
		// - WITHOUT ROWID: the row is its own index entry, so a rowid table would store the hash key
		//   twice. There is nothing to move last — the key must lead the primary key, and the only other
		//   column is the short repartition id.
		//
		// fokos_range_hierarchy:
		// - The learned descendant boundaries of range trees. A hash partition holds the boundaries of
		//   many promoted keys in one table, so every row names the hash key it describes.
		// - Unbounded edges are stored as the empty key `x''`, which KeyCodec never produces for a real
		//   key. `findDeepestKnownRangeSlice` decodes the sentinel back to null.
		// - `learned_at` is the eviction order. The table is row-bounded, and the oldest rows go first.
		sql: `
            CREATE TABLE IF NOT EXISTS fokos_repartitions (
                id              TEXT    NOT NULL PRIMARY KEY,
                seq             INTEGER NOT NULL,
                kind            TEXT    NOT NULL,
                state           TEXT    NOT NULL,
                queued_at       INTEGER NOT NULL,
                cutover_at      INTEGER,
                completed_at    INTEGER,
                attempts        INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER NOT NULL,
                hash_key        BLOB
            ) STRICT;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_fokos_repartitions_seq
                ON fokos_repartitions (seq);
            CREATE INDEX IF NOT EXISTS idx_fokos_repartitions_due
                ON fokos_repartitions (state, next_attempt_at, seq);
            CREATE INDEX IF NOT EXISTS idx_fokos_repartitions_split
                ON fokos_repartitions (kind) WHERE kind IN ('hash_split', 'range_split');

            CREATE TABLE IF NOT EXISTS fokos_repartition_targets (
                repartition_id     TEXT    NOT NULL,
                target_index       INTEGER NOT NULL,
                initialization     TEXT    NOT NULL DEFAULT 'pending',
                start_notified     INTEGER NOT NULL DEFAULT 0,
                acknowledged       INTEGER NOT NULL DEFAULT 0,
                attempts           INTEGER NOT NULL DEFAULT 0,
                next_attempt_at    INTEGER NOT NULL,
                slice_child_idx    INTEGER,
                partition_id       TEXT    NOT NULL,
                do_name            TEXT    NOT NULL,
                slice_hash_key     BLOB,
                slice_start        BLOB,
                slice_end          BLOB,
                PRIMARY KEY (repartition_id, partition_id),
                UNIQUE (repartition_id, target_index)
            ) STRICT;

            CREATE TABLE IF NOT EXISTS fokos_route_overrides (
                hash_key       BLOB NOT NULL PRIMARY KEY,
                repartition_id TEXT NOT NULL
            ) WITHOUT ROWID, STRICT;

            CREATE TABLE IF NOT EXISTS fokos_range_hierarchy (
                hk                BLOB    NOT NULL DEFAULT x'',
                sk_start_boundary BLOB    NOT NULL DEFAULT x'',
                sk_end_boundary   BLOB    NOT NULL DEFAULT x'',
                depth             INTEGER NOT NULL,
                learned_at        INTEGER NOT NULL,
                PRIMARY KEY (hk, sk_start_boundary, sk_end_boundary)
            ) WITHOUT ROWID, STRICT;
            CREATE INDEX IF NOT EXISTS idx_fokos_range_hierarchy_depth
                ON fokos_range_hierarchy (hk, depth, sk_start_boundary, sk_end_boundary);`,
	},
];

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

export class FokosShardingStore {
	#storage: DurableObjectStorage;
	#migrations: SQLSchemaMigrations;
	#rangeHierarchyMaxRows: number;

	constructor(storage: DurableObjectStorage, options: FokosShardingStoreOptions = {}) {
		this.#storage = storage;
		this.#rangeHierarchyMaxRows = options.rangeHierarchyMaxRows ?? RANGE_HIERARCHY_MAX_ROWS;
		invariant(this.#rangeHierarchyMaxRows >= 1, "fokos/sharding-store: rangeHierarchyMaxRows must be at least 1");
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: storage,
			keyNameTrackingLastMigrationID: FOKOS_KV_KEYS.SCHEMA_VERSION,
		});
	}

	runMigrations(): void {
		this.#migrations.runAllSync();
	}

	/**
	 * Atomicity passthrough: the flow composes each durable transition around store calls, and the
	 * host's own writes join the same transaction when a hook runs inside it.
	 */
	transactionSync<T>(fn: () => T): T {
		return this.#storage.transactionSync(fn);
	}

	// ─── KV: identity and policy ────────────────────────────────────────────

	getIdentity(): FokosPartitionIdentity | undefined {
		return this.#storage.kv.get<FokosPartitionIdentity>(FOKOS_KV_KEYS.IDENTITY);
	}

	putIdentity(identity: FokosPartitionIdentity): void {
		this.#storage.kv.put<FokosPartitionIdentity>(FOKOS_KV_KEYS.IDENTITY, identity);
	}

	getPolicy<TPolicy>(): FokosStoredPolicy<TPolicy> | undefined {
		return this.#storage.kv.get<FokosStoredPolicy<TPolicy>>(FOKOS_KV_KEYS.POLICY);
	}

	putPolicy<TPolicy>(stored: FokosStoredPolicy<TPolicy>): void {
		this.#storage.kv.put<FokosStoredPolicy<TPolicy>>(FOKOS_KV_KEYS.POLICY, stored);
	}

	// ─── KV: import, plan, destroy fence ────────────────────────────────────

	getImport(): FokosImportRecord | undefined {
		return this.#storage.kv.get<FokosImportRecord>(FOKOS_KV_KEYS.IMPORT);
	}

	putImport(record: FokosImportRecord): void {
		this.#storage.kv.put<FokosImportRecord>(FOKOS_KV_KEYS.IMPORT, record);
	}

	getPlanHead<TPolicy = unknown>(repartitionId: string): FokosStoredRepartitionPlan<TPolicy> | undefined {
		return this.#storage.kv.get<FokosStoredRepartitionPlan<TPolicy>>(FOKOS_KV_KEYS.planHead(repartitionId));
	}

	putPlanHead(repartitionId: string, head: FokosStoredRepartitionPlan): void {
		this.#storage.kv.put<FokosStoredRepartitionPlan>(FOKOS_KV_KEYS.planHead(repartitionId), head);
	}

	/** Deletes the head and every item it links to through `nextKey`. */
	deletePlanChain(repartitionId: string): void {
		let key: string | null = FOKOS_KV_KEYS.planHead(repartitionId);
		while (key !== null) {
			const item: { nextKey: string | null } | undefined = this.#storage.kv.get(key);
			this.#storage.kv.delete(key);
			key = item?.nextKey ?? null;
		}
	}

	/** True after `fokosPrepareDestroy` fences this partition. Every transition must then stop. */
	isDestroying(): boolean {
		return this.#storage.kv.get<boolean>(FOKOS_KV_KEYS.DESTROYING) === true;
	}

	setDestroying(): void {
		this.#storage.kv.put<boolean>(FOKOS_KV_KEYS.DESTROYING, true);
	}

	// ─── KV: jobs ───────────────────────────────────────────────────────────

	getJobs(): FokosJobsRecord {
		return this.#storage.kv.get<FokosJobsRecord>(FOKOS_KV_KEYS.JOBS) ?? {};
	}

	putJobs(record: FokosJobsRecord): void {
		if (Object.keys(record).length === 0) {
			this.#storage.kv.delete(FOKOS_KV_KEYS.JOBS);
		} else {
			this.#storage.kv.put<FokosJobsRecord>(FOKOS_KV_KEYS.JOBS, record);
		}
	}

	// ─── KV: route caches ───────────────────────────────────────────────────

	getHashArena(): HashTopologySnapshot | undefined {
		return this.#storage.kv.get<HashTopologySnapshot>(FOKOS_KV_KEYS.HASH_ARENA);
	}

	putHashArena(snapshot: HashTopologySnapshot): void {
		this.#storage.kv.put<HashTopologySnapshot>(FOKOS_KV_KEYS.HASH_ARENA, snapshot);
	}

	getPromotionBloom(): PartialRangeTopologySnapshot | undefined {
		return this.#storage.kv.get<PartialRangeTopologySnapshot>(FOKOS_KV_KEYS.PROMOTION_BLOOM);
	}

	putPromotionBloom(snapshot: PartialRangeTopologySnapshot): void {
		this.#storage.kv.put<PartialRangeTopologySnapshot>(FOKOS_KV_KEYS.PROMOTION_BLOOM, snapshot);
	}

	// ─── fokos_repartitions ─────────────────────────────────────────────────

	/** The sequence of the next local repartition. Rows are permanent, so a value never repeats. */
	nextRepartitionSeq(): number {
		const row = this.#storage.sql.exec<{ next: number }>(`SELECT COALESCE(MAX(seq), 0) + 1 AS next FROM fokos_repartitions`).one();
		return row.next;
	}

	insertRepartition(row: {
		id: string;
		seq: number;
		kind: RepartitionKind;
		state: RepartitionState;
		hashKey: KeyBytes | null;
		queuedAt: number;
		cutoverAt?: number | null;
		completedAt?: number | null;
		nextAttemptAt: number;
	}): void {
		this.#storage.sql.exec(
			`INSERT INTO fokos_repartitions (id, seq, kind, state, queued_at, cutover_at, completed_at, attempts, next_attempt_at, hash_key)
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?9)`,
			row.id,
			row.seq,
			row.kind,
			row.state,
			row.queuedAt,
			row.cutoverAt ?? null,
			row.completedAt ?? null,
			row.nextAttemptAt,
			row.hashKey,
		);
	}

	getRepartition(id: string): RepartitionRow | undefined {
		const row = tryOne(this.#storage.sql.exec<SqlRepartitionRow>(`${REPARTITION_SELECT} WHERE r.id = ?`, id));
		return row && toRepartitionRow(row);
	}

	/**
	 * The one split row of this source, if it has ever queued one. A split source becomes a router and
	 * owns nothing, so it never queues a second split.
	 */
	getSplitRepartition(): RepartitionRow | undefined {
		const row = tryOne(
			this.#storage.sql.exec<SqlRepartitionRow>(`${REPARTITION_SELECT} WHERE r.kind IN ('hash_split', 'range_split') LIMIT 1`),
		);
		return row && toRepartitionRow(row);
	}

	/** The earliest repartition that has not cut over and completed: the split row first, else the oldest promotion. */
	firstActiveRepartition(): RepartitionRow | undefined {
		const row = tryOne(
			this.#storage.sql.exec<SqlRepartitionRow>(
				`${REPARTITION_SELECT} WHERE r.state IN ('queued', 'planned', 'cutover')
				  ORDER BY (r.kind = 'key_promotion'), r.seq LIMIT 1`,
			),
		);
		return row && toRepartitionRow(row);
	}

	/** True while any key promotion has not reached `completed`. It blocks a hash split. */
	hasUnfinishedPromotion(): boolean {
		return (
			this.#storage.sql
				.exec(`SELECT 1 FROM fokos_repartitions WHERE kind = 'key_promotion' AND state IN ('queued', 'planned', 'cutover') LIMIT 1`)
				.toArray().length > 0
		);
	}

	setRepartitionState(id: string, state: RepartitionState, stamps?: { cutoverAt?: number; completedAt?: number }): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartitions
			    SET state = ?2,
			        cutover_at = COALESCE(?3, cutover_at),
			        completed_at = COALESCE(?4, completed_at)
			  WHERE id = ?1`,
			id,
			state,
			stamps?.cutoverAt ?? null,
			stamps?.completedAt ?? null,
		);
	}

	setRepartitionAttempt(id: string, attempts: number, nextAttemptAt: number): void {
		this.#storage.sql.exec(`UPDATE fokos_repartitions SET attempts = ?2, next_attempt_at = ?3 WHERE id = ?1`, id, attempts, nextAttemptAt);
	}

	/**
	 * Recomputes the deadline of a row's next source step from its targets: the earliest deadline of a
	 * target that still needs a call, or `now` when no target does and a state step remains.
	 */
	refreshRepartitionDue(id: string, now: number): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartitions
			    SET next_attempt_at = COALESCE(
			        (SELECT MIN(t.next_attempt_at) FROM fokos_repartition_targets t
			          WHERE t.repartition_id = fokos_repartitions.id
			            AND ((fokos_repartitions.state = 'planned' AND t.initialization != 'initialized')
			              OR (fokos_repartitions.state = 'cutover' AND t.start_notified = 0))),
			        ?2)
			  WHERE id = ?1`,
			id,
			now,
		);
	}

	/**
	 * Makes every unfinished promotion due now, with every target of one that still needs a call.
	 *
	 * A promotion that cannot move a locked key parks itself 5 seconds out. The release of that lock is
	 * the event it waits for, so the caller brings the deadline forward. The retry interval must not
	 * decide how long the key stays where it is.
	 */
	markPromotionsDueNow(now: number): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartitions SET next_attempt_at = ?1 WHERE kind = 'key_promotion' AND state IN ('queued', 'planned')`,
			now,
		);
		this.#storage.sql.exec(
			`UPDATE fokos_repartition_targets SET next_attempt_at = ?1
			  WHERE initialization != 'initialized'
			    AND repartition_id IN (SELECT id FROM fokos_repartitions WHERE kind = 'key_promotion' AND state IN ('queued', 'planned'))`,
			now,
		);
	}

	/** The earliest due repartition that has a source step. Ordered so a failed row falls behind another. */
	selectDueRepartition(now: number): RepartitionRow | undefined {
		const row = tryOne(
			this.#storage.sql.exec<SqlRepartitionRow>(
				`${REPARTITION_SELECT} WHERE ${REPARTITION_HAS_STEP} AND r.next_attempt_at <= ?1 ORDER BY r.next_attempt_at, r.seq LIMIT 1`,
				now,
			),
		);
		return row && toRepartitionRow(row);
	}

	/** The earliest deadline of any repartition that has a source step, now or later. */
	earliestRepartitionDeadline(): number | null {
		return one(
			this.#storage.sql.exec<{
				deadline: number | null;
			}>(`SELECT MIN(r.next_attempt_at) AS deadline FROM fokos_repartitions r WHERE ${REPARTITION_HAS_STEP}`),
		).deadline;
	}

	/**
	 * The earliest repartition whose source cleanup is still pending, of any kind. A split reclaims no
	 * item rows, so its step reports itself done at once and the caller needs no branch on the kind.
	 */
	selectDueCleanup(now: number): RepartitionRow | undefined {
		const row = tryOne(
			this.#storage.sql.exec<SqlRepartitionRow>(
				`${REPARTITION_SELECT} WHERE r.state = 'completed' AND r.next_attempt_at <= ?1 ORDER BY r.next_attempt_at, r.seq LIMIT 1`,
				now,
			),
		);
		return row && toRepartitionRow(row);
	}

	earliestCleanupDeadline(): number | null {
		return one(
			this.#storage.sql.exec<{ deadline: number | null }>(
				`SELECT MIN(next_attempt_at) AS deadline FROM fokos_repartitions WHERE state = 'completed'`,
			),
		).deadline;
	}

	// ─── fokos_repartition_targets ──────────────────────────────────────────

	insertRepartitionTarget(target: {
		repartitionId: string;
		/** The parent repartition's kind. It fixes the slice kind, which the target row does not store. */
		kind: RepartitionKind;
		partitionId: string;
		doName: string;
		targetIndex: number;
		slice: RepartitionSlice;
		initialization?: TargetInitialization;
		startNotified?: boolean;
		acknowledged?: boolean;
		nextAttemptAt: number;
	}): void {
		const s = target.slice;
		invariant(
			s.kind === SLICE_KIND_OF[target.kind],
			`fokos/sharding-store.insertRepartitionTarget: a ${target.kind} takes a ${SLICE_KIND_OF[target.kind]} slice, got ${s.kind}`,
		);
		this.#storage.sql.exec(
			`INSERT INTO fokos_repartition_targets
			   (repartition_id, target_index, initialization, start_notified, acknowledged, attempts, next_attempt_at,
			    slice_child_idx, partition_id, do_name, slice_hash_key, slice_start, slice_end)
			 VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, ?8, ?9, ?10, ?11, ?12)`,
			target.repartitionId,
			target.targetIndex,
			target.initialization ?? "pending",
			target.startNotified ? 1 : 0,
			target.acknowledged ? 1 : 0,
			target.nextAttemptAt,
			s.kind === "hash_child" ? s.childIndex : null,
			target.partitionId,
			target.doName,
			s.kind === "hash_child" ? null : s.hashKey,
			s.kind === "range" ? s.start : null,
			s.kind === "range" ? s.end : null,
		);
	}

	/**
	 * Every target of a repartition, in `target_index` order — the order range children tile their
	 * interval. `kind` comes from the repartition row, which every caller reads first; it is what names
	 * the slice kind, because the target rows do not store it.
	 */
	listRepartitionTargets(repartitionId: string, kind: RepartitionKind): RepartitionTargetRow[] {
		return this.#storage.sql
			.exec<SqlTargetRow>(`${TARGET_SELECT} WHERE repartition_id = ? ORDER BY target_index`, repartitionId)
			.toArray()
			.map((row) => toTargetRow(row, kind));
	}

	getRepartitionTarget(repartitionId: string, partitionId: string, kind: RepartitionKind): RepartitionTargetRow | undefined {
		const row = tryOne(
			this.#storage.sql.exec<SqlTargetRow>(`${TARGET_SELECT} WHERE repartition_id = ?1 AND partition_id = ?2`, repartitionId, partitionId),
		);
		return row && toTargetRow(row, kind);
	}

	/**
	 * The due targets of one step, at most `limit` of them. `init` selects the targets that still need
	 * a `fokosInit`; `start` selects the ones that still need a start notification.
	 */
	selectDueTargets(
		repartitionId: string,
		kind: RepartitionKind,
		phase: "init" | "start",
		now: number,
		limit: number,
	): RepartitionTargetRow[] {
		const cond = phase === "init" ? `initialization != 'initialized'` : `start_notified = 0`;
		return this.#storage.sql
			.exec<SqlTargetRow>(
				`${TARGET_SELECT} WHERE repartition_id = ?1 AND ${cond} AND next_attempt_at <= ?2 ORDER BY next_attempt_at, target_index LIMIT ?3`,
				repartitionId,
				now,
				limit,
			)
			.toArray()
			.map((row) => toTargetRow(row, kind));
	}

	setTargetInitialization(
		repartitionId: string,
		partitionId: string,
		initialization: TargetInitialization,
		attempts: number,
		nextAttemptAt: number,
	): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartition_targets SET initialization = ?3, attempts = ?4, next_attempt_at = ?5
			  WHERE repartition_id = ?1 AND partition_id = ?2`,
			repartitionId,
			partitionId,
			initialization,
			attempts,
			nextAttemptAt,
		);
	}

	setTargetAttempt(repartitionId: string, partitionId: string, attempts: number, nextAttemptAt: number): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartition_targets SET attempts = ?3, next_attempt_at = ?4 WHERE repartition_id = ?1 AND partition_id = ?2`,
			repartitionId,
			partitionId,
			attempts,
			nextAttemptAt,
		);
	}

	setTargetStartNotified(repartitionId: string, partitionId: string, nextAttemptAt: number): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartition_targets SET start_notified = 1, attempts = 0, next_attempt_at = ?3
			  WHERE repartition_id = ?1 AND partition_id = ?2`,
			repartitionId,
			partitionId,
			nextAttemptAt,
		);
	}

	/** Records one target's acknowledgement. Returns false when it had already acknowledged. */
	setTargetAcknowledged(repartitionId: string, partitionId: string): boolean {
		const res = this.#storage.sql.exec(
			`UPDATE fokos_repartition_targets SET acknowledged = 1 WHERE repartition_id = ?1 AND partition_id = ?2 AND acknowledged = 0`,
			repartitionId,
			partitionId,
		);
		return res.rowsWritten > 0;
	}

	countRepartitionTargets(repartitionId: string): RepartitionTargetCounts {
		const row = this.#storage.sql
			.exec<{ total: number; initialized: number; start_notified: number; acknowledged: number }>(
				`SELECT COUNT(*) AS total,
				        SUM(initialization = 'initialized') AS initialized,
				        SUM(start_notified) AS start_notified,
				        SUM(acknowledged) AS acknowledged
				   FROM fokos_repartition_targets WHERE repartition_id = ?`,
				repartitionId,
			)
			.one();
		return {
			total: row.total,
			initialized: row.initialized ?? 0,
			startNotified: row.start_notified ?? 0,
			acknowledged: row.acknowledged ?? 0,
		};
	}

	// ─── fokos_route_overrides ──────────────────────────────────────────────

	insertRouteOverride(hashKey: KeyBytes, repartitionId: string): void {
		this.#storage.sql.exec(`INSERT OR IGNORE INTO fokos_route_overrides (hash_key, repartition_id) VALUES (?, ?)`, hashKey, repartitionId);
	}

	/**
	 * The routing answer for one hash key: the promotion that owns it and how far that promotion has
	 * got. One indexed join on the hot point-read path.
	 */
	routeOverrideFor(hashKey: KeyBytes): { repartitionId: string; state: RepartitionState } | undefined {
		const row = tryOne(
			this.#storage.sql.exec<{ repartition_id: string; state: RepartitionState }>(
				`SELECT o.repartition_id, r.state FROM fokos_route_overrides o
				   JOIN fokos_repartitions r ON r.id = o.repartition_id
				  WHERE o.hash_key = ?`,
				hashKey,
			),
		);
		return row && { repartitionId: row.repartition_id, state: row.state };
	}

	hasRouteOverride(hashKey: KeyBytes): boolean {
		return this.#storage.sql.exec(`SELECT 1 FROM fokos_route_overrides WHERE hash_key = ? LIMIT 1`, hashKey).toArray().length > 0;
	}

	/**
	 * A terminal override is one whose promotion reached `completed` or `cleaned`. A hash split exports
	 * exactly these to its children and excludes exactly their keys from the item stream.
	 */
	hasTerminalRouteOverride(hashKey: KeyBytes): boolean {
		return (
			this.#storage.sql
				.exec(
					`SELECT 1 FROM fokos_route_overrides o JOIN fokos_repartitions r ON r.id = o.repartition_id
					  WHERE o.hash_key = ? AND r.state IN ('completed', 'cleaned') LIMIT 1`,
					hashKey,
				)
				.toArray().length > 0
		);
	}

	/** Pages the terminal overrides in `hash_key` order, strictly after `cursor`. */
	queryTerminalRouteOverridesPage(cursor: PromotedKeyCursor | null, limit: number): { hashKey: KeyBytes }[] {
		const where = cursor ? `AND o.hash_key > ?2` : ``;
		const params: unknown[] = cursor ? [limit, cursor.hashKey] : [limit];
		return this.#storage.sql
			.exec<{ hash_key: ArrayBuffer }>(
				`SELECT o.hash_key FROM fokos_route_overrides o JOIN fokos_repartitions r ON r.id = o.repartition_id
				  WHERE r.state IN ('completed', 'cleaned') ${where} ORDER BY o.hash_key LIMIT ?1`,
				...params,
			)
			.toArray()
			.map((r) => ({ hashKey: fromSqlKey(r.hash_key) }));
	}

	// ─── repartition status view ────────────────────────────────────────────

	/**
	 * One page of the administration view, ordered by `(seq, target_index)` and resumed strictly after
	 * `cursor`. A repartition with no target appears once with `target_index` −1, so destroy traversal
	 * and status both see every row exactly once.
	 */
	queryRepartitionStatusPage(cursor: RepartitionStatusCursor | null, limit: number): RepartitionStatusRow[] {
		const after = cursor ? `WHERE seq > ?2 OR (seq = ?2 AND target_index > ?3)` : ``;
		const params: unknown[] = cursor ? [limit, cursor.seq, cursor.targetIndex] : [limit];
		return this.#storage.sql
			.exec<{
				id: string;
				seq: number;
				kind: RepartitionKind;
				state: RepartitionState;
				hash_key: ArrayBuffer | null;
				target_index: number;
				partition_id: string | null;
				do_name: string | null;
				initialization: TargetInitialization | null;
				acknowledged: number | null;
			}>(
				`SELECT * FROM (
				    SELECT r.id, r.seq, r.kind, r.state, r.hash_key, -1 AS target_index,
				           NULL AS partition_id, NULL AS do_name, NULL AS initialization, NULL AS acknowledged
				      FROM fokos_repartitions r
				     WHERE NOT EXISTS (SELECT 1 FROM fokos_repartition_targets t WHERE t.repartition_id = r.id)
				    UNION ALL
				    SELECT r.id, r.seq, r.kind, r.state, r.hash_key, t.target_index,
				           t.partition_id, t.do_name, t.initialization, t.acknowledged
				      FROM fokos_repartitions r
				      JOIN fokos_repartition_targets t ON t.repartition_id = r.id
				 ) ${after}
				 ORDER BY seq, target_index LIMIT ?1`,
				...params,
			)
			.toArray()
			.map((r) => ({
				id: r.id,
				seq: r.seq,
				kind: r.kind,
				state: r.state,
				hashKey: r.hash_key === null ? null : fromSqlKey(r.hash_key),
				targetIndex: r.target_index,
				partitionId: r.partition_id,
				doName: r.do_name,
				initialization: r.initialization,
				acknowledged: r.acknowledged === 1,
			}));
	}

	// ─── fokos_range_hierarchy ──────────────────────────────────────────────

	/**
	 * Learns one range boundary, or refreshes its eviction stamp when it is already known and older
	 * than `RANGE_HIERARCHY_REFRESH_MS`. Every row is written under the real hash key it describes, so
	 * a hash partition can hold the boundaries of many promoted keys in one table.
	 *
	 * The table is bounded by `rangeHierarchyMaxRows`. A write that can have grown it evicts the rows
	 * with the oldest `learned_at`, deepest first. A partition's own ancestors are in its identity, so
	 * eviction cannot change its route evidence; a lost row costs one more hop on a later request.
	 */
	learnRangeBoundary(hk: KeyBytes, startBoundary: KeyBytes, endBoundary: KeyBytes, depth: number, now = Date.now()): void {
		const res = this.#storage.sql.exec(
			`INSERT INTO fokos_range_hierarchy (hk, sk_start_boundary, sk_end_boundary, depth, learned_at) VALUES (?1, ?2, ?3, ?4, ?5)
			 ON CONFLICT (hk, sk_start_boundary, sk_end_boundary) DO UPDATE SET learned_at = excluded.learned_at
			 WHERE learned_at < excluded.learned_at - ?6`,
			hk,
			startBoundary,
			endBoundary,
			depth,
			now,
			RANGE_HIERARCHY_REFRESH_MS,
		);
		if (res.rowsWritten === 0) {
			return;
		}

		// FIXME: Optimize this by using a more efficient eviction strategy rather than counting and deleting excess rows.
		const excess =
			one(this.#storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM fokos_range_hierarchy`)).n - this.#rangeHierarchyMaxRows;
		if (excess <= 0) {
			return;
		}
		this.#storage.sql.exec(
			`DELETE FROM fokos_range_hierarchy WHERE (hk, sk_start_boundary, sk_end_boundary) IN (
			     SELECT hk, sk_start_boundary, sk_end_boundary FROM fokos_range_hierarchy ORDER BY learned_at, depth DESC LIMIT ?1)`,
			excess,
		);
	}

	/** The number of learned rows. For tests and status views; the bound is enforced on every learn. */
	countRangeHierarchyRows(): number {
		return one(this.#storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM fokos_range_hierarchy`)).n;
	}

	/** Every learned slice of one hash key, so the frontier planner can overlay them on its base cover. */
	listLearnedRangeSlices(hk: KeyBytes): LearnedRangeSlice[] {
		return this.#storage.sql
			.exec<{ depth: number; sk_start_boundary: ArrayBuffer; sk_end_boundary: ArrayBuffer }>(
				`SELECT depth, sk_start_boundary, sk_end_boundary FROM fokos_range_hierarchy WHERE hk = ? ORDER BY depth, sk_start_boundary`,
				hk,
			)
			.toArray()
			.map((row) => {
				const start = fromSqlKey(row.sk_start_boundary);
				const end = fromSqlKey(row.sk_end_boundary);
				return { depth: row.depth, startBoundary: start.length === 0 ? null : start, endBoundary: end.length === 0 ? null : end };
			});
	}

	/** Forgets one learned slice, after the partition it names answered that it does not exist. */
	deleteLearnedRangeSlice(hk: KeyBytes, startBoundary: KeyBytes | null, endBoundary: KeyBytes | null): void {
		const unbounded = KeyCodec.encodeOptional(undefined);
		this.#storage.sql.exec(
			`DELETE FROM fokos_range_hierarchy WHERE hk = ?1 AND sk_start_boundary = ?2 AND sk_end_boundary = ?3`,
			hk,
			startBoundary ?? unbounded,
			endBoundary ?? unbounded,
		);
	}

	/**
	 * Returns the deepest learned range slice that contains `sortKey` for the given hash key, or
	 * `null` when nothing is known that covers it. Used to skip intermediate range router hops: the
	 * returned `[startBoundary, endBoundary)` slice resolves deterministically to a DO.
	 */
	findDeepestKnownRangeSlice(hk: KeyBytes, sortKey: KeyBytes): LearnedRangeSlice | null {
		// Boundaries are stored with the empty sentinel `[]` for unbounded edges. `[]` is the byte
		// minimum, which is correct for an unbounded start (`start <= sortKey` always holds) but NOT for
		// an unbounded end — hence the explicit sentinel check in the WHERE clause. Real keys are never
		// empty (KeyCodec rejects empty input), so `[]` is an unambiguous "unbounded" tag. The result
		// decodes `[]` back to `null` for both edges, so callers can feed `resolveRangePartitionContext`
		// directly.
		const unbounded = KeyCodec.encodeOptional(undefined);
		const row = tryOne(
			this.#storage.sql.exec<{ depth: number; sk_start_boundary: ArrayBuffer; sk_end_boundary: ArrayBuffer }>(
				`SELECT depth, sk_start_boundary, sk_end_boundary
				 FROM fokos_range_hierarchy
				 WHERE hk = ?
				   AND sk_start_boundary <= ?
				   AND (sk_end_boundary > ? OR sk_end_boundary = ?)
				 ORDER BY depth DESC
				 LIMIT 1`,
				hk,
				sortKey,
				sortKey,
				unbounded,
			),
		);
		if (!row) {
			return null;
		}

		const start = fromSqlKey(row.sk_start_boundary);
		const end = fromSqlKey(row.sk_end_boundary);
		return {
			depth: row.depth,
			startBoundary: start.length === 0 ? null : start,
			endBoundary: end.length === 0 ? null : end,
		};
	}
}
