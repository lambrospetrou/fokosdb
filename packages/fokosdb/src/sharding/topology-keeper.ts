/**
 * The source of truth of the topology of a shard group, and how a caller reads it.
 *
 * - `FokosTopologyKeeper` stores the reports of the completed repartitions in SQLite, and builds the
 *   encoded hash body and the range bodies some time after a report, so a read never encodes.
 * - `FokosShardingAutoAdminDO` is a Durable Object that embeds one keeper and exposes it over RPC. A
 *   host that has its own admin Durable Object embeds the keeper instead.
 * - `fokosTopologyEntrypointFetch` serves the bodies over `fetch()`, for a `WorkerEntrypoint` of the
 *   host that has Workers Cache enabled.
 *
 * Every table of the keeper has the prefix `fokos_topo_`, so it can share a database with the host.
 */
import { DurableObject } from "cloudflare:workers";
import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import invariant from "../shared/invariant.js";
import { one, tryOne } from "../shared/sql-cursor.js";
import { FokosValidationError } from "../shared/errors.js";
import { SHARDING_VALIDATION_CODES } from "./errors.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import { PartitionIdHelper } from "./partition-id.js";
import { validateTopology } from "./route-context.js";
import { parseHashBody, RANGE_BODY_MAGIC, readBodyVersion, type FokosTopologyEvent, type HashTopologyView } from "./topology-codec.js";
import {
	encodeHashBitmap,
	encodeHashBody,
	hashNodeKey,
	encodeRangeBody,
	mergeHashBitmap,
	promotedKeysBloom,
	topologyEventFromWire,
	topologyEventToJson,
} from "./topology-encode.js";

/** The size of one stored chunk of a body. The documented limit of a row is 2 MB. */
export const TOPOLOGY_BODY_CHUNK_BYTES = 1024 * 1024;

const DEFAULT_BUILD_DELAY_MS = 2_000;
const DEFAULT_MAX_RANGE_BODY_BYTES = 1024 * 1024;
const DEFAULT_BUILD_STEP_ROWS = 100_000;

export type FokosTopologyKeeperOptions = {
	/** The time from the first report of a shard group to its next build. Default 2,000. */
	buildDelayMs?: number;
	/** The largest range body. A larger body drops its deepest boundaries. Default 1 MiB. */
	maxRangeBodyBytes?: number;
	/**
	 * The rows that one `runDueBuilds` call reads before it stops and leaves the rest of the build due.
	 * It always builds at least one body. Default 100,000.
	 */
	buildStepRows?: number;
	/** Host code. Makes sure that `runDueBuilds` runs at `at` or soon after it, for example with the alarm. */
	scheduleBuild(this: void, at: number): Promise<void> | void;
};

/** The answer of a topology read. `body` is the encoded body. 304: the body has the ETag that the caller gave. */
export type FokosTopologyRead = { status: 200; etag: string; body: Uint8Array } | { status: 304; etag: string };

/** The RPC methods of an admin Durable Object that the entrypoint helper calls. */
export type FokosTopologyAdminRpc = {
	getTopology(shardGroup: string, ifNoneMatch?: string): Promise<FokosTopologyRead>;
	getRangeTopology(shardGroup: string, hashKey: Uint8Array, ifNoneMatch?: string): Promise<FokosTopologyRead>;
};

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "The topology keeper: the shard groups, the reports, the range intervals, and the built bodies",
		// fokos_topo_groups:
		// - `epoch` increases at each reset. `version` is a sequence number that increases by 1 with each
		//   report that writes a new fact: a hash node, a range root, or a range interval.
		// - `hash_version` is the `version` of the last new hash node or new range root, which are the
		//   facts of the hash body. `built_version` is the `hash_version` of the stored hash body. The
		//   hash body is built again only when `hash_version > built_version`.
		// - `root_trees_n` and `hash_split_n` come from the first report after a reset, and never change
		//   until the next reset.
		// - `build_due_at` is set by the first report with a new fact after a build, and cleared by the
		//   step that finds the hash body built and no dirty hash key.
		// - `promoted_n` is the number of promoted hash keys, so a build can size the Bloom filter
		//   before it reads the keys. `hash_nodes_n` is the number of rows in fokos_topo_hash_nodes, so
		//   a build can size the bitmap before it reads the rows.
		//
		// fokos_topo_repartitions:
		// - One row for each report, of every kind, for idempotency. `repartition_key` is the source
		//   partition ID of a `hash_split` and a `key_promotion`, and the base64url hash key of a
		//   `range_split`. `seq` is the `version` that the report gave its shard group.
		// - `data` is the JSONB of the event. No build reads it.
		//
		// fokos_topo_hash_nodes:
		// - One row for each split hash partition, and for each of its ancestors, also when the report
		//   of an ancestor is missing. `node` is `[depth, rootHi, rootLo, ...path]`, so the primary key
		//   gives the level order of the bitmap.
		// - `seq` is the `version` of the report that inserted the row. The rows with
		//   `seq > built_version` are the splits that the stored hash body does not have yet, and
		//   idx_fokos_topo_hash_nodes_seq finds them.
		//
		// fokos_topo_range_dirty:
		// - The promoted hash keys that need a new range body. A report that writes a new interval of a
		//   key inserts the key, and the step that stores the range body deletes it.
		//
		// fokos_topo_range_intervals:
		// - The known intervals of each promoted hash key. `start` x'' is ~min, and `end_is_max` 1 is ~max.
		// - The primary key gives the order of the range build: `start` ascending, then `end`
		//   descending, so a parent comes before its children.
		// - `seq` is the `version` of the report that inserted the row.
		// - idx_fokos_topo_range_roots holds the range root of each promoted hash key only, so the
		//   build reads the promoted keys and no other interval. It is in `seq` order, so a build that
		//   extends the stored hash body reads only the roots with `seq > built_version`.
		//
		// fokos_topo_bodies:
		// - The built bodies in chunks. `kind` is `hash` or `range`. A hash body has the hash key x''.
		sql: `
            CREATE TABLE IF NOT EXISTS fokos_topo_groups (
                shard_group   TEXT    NOT NULL PRIMARY KEY,
                root_trees_n  INTEGER,
                hash_split_n  INTEGER,
                epoch         INTEGER NOT NULL DEFAULT 0,
                version       INTEGER NOT NULL DEFAULT 0,
                hash_version  INTEGER NOT NULL DEFAULT 0,
                reset_at      INTEGER NOT NULL DEFAULT 0,
                built_version INTEGER NOT NULL DEFAULT 0,
                build_due_at  INTEGER,
                promoted_n    INTEGER NOT NULL DEFAULT 0,
                hash_nodes_n  INTEGER NOT NULL DEFAULT 0
            ) STRICT;

            CREATE TABLE IF NOT EXISTS fokos_topo_repartitions (
                shard_group     TEXT    NOT NULL,
                id              TEXT    NOT NULL,
                kind            TEXT    NOT NULL,
                seq             INTEGER NOT NULL,
                completed_at    INTEGER NOT NULL,
                received_at     INTEGER NOT NULL,
                actor_id        TEXT    NOT NULL,
                repartition_key TEXT    NOT NULL,
                data            BLOB    NOT NULL,
                PRIMARY KEY (shard_group, id)
            ) STRICT;

            CREATE TABLE IF NOT EXISTS fokos_topo_hash_nodes (
                shard_group TEXT NOT NULL,
                node        BLOB    NOT NULL,
                seq         INTEGER NOT NULL,
                PRIMARY KEY (shard_group, node)
            ) WITHOUT ROWID, STRICT;
            CREATE INDEX IF NOT EXISTS idx_fokos_topo_hash_nodes_seq ON fokos_topo_hash_nodes (shard_group, seq);

            CREATE TABLE IF NOT EXISTS fokos_topo_range_dirty (
                shard_group TEXT NOT NULL,
                hash_key    BLOB NOT NULL,
                PRIMARY KEY (shard_group, hash_key)
            ) WITHOUT ROWID, STRICT;

            CREATE TABLE IF NOT EXISTS fokos_topo_range_intervals (
                shard_group TEXT    NOT NULL,
                hash_key    BLOB    NOT NULL,
                start       BLOB    NOT NULL,
                end_is_max  INTEGER NOT NULL,
                end         BLOB    NOT NULL,
                seq         INTEGER NOT NULL,
                PRIMARY KEY (shard_group, hash_key, start, end_is_max DESC, end DESC)
            ) WITHOUT ROWID, STRICT;
            CREATE INDEX IF NOT EXISTS idx_fokos_topo_range_roots
                ON fokos_topo_range_intervals (shard_group, seq) WHERE start = x'' AND end_is_max = 1;

            CREATE TABLE IF NOT EXISTS fokos_topo_bodies (
                shard_group TEXT    NOT NULL,
                kind        TEXT    NOT NULL,
                hash_key    BLOB    NOT NULL,
                chunk       INTEGER NOT NULL,
                bytes       BLOB    NOT NULL,
                PRIMARY KEY (shard_group, kind, hash_key, chunk)
            ) STRICT;`,
	},
];

type GroupRow = {
	shard_group: string;
	root_trees_n: number | null;
	hash_split_n: number | null;
	epoch: number;
	version: number;
	hash_version: number;
	reset_at: number;
	built_version: number;
	build_due_at: number | null;
	promoted_n: number;
	hash_nodes_n: number;
};

type IntervalRow = { start: ArrayBuffer; end_is_max: number; end: ArrayBuffer };

const NO_KEY = new Uint8Array(0);

function toBase64Url(bytes: Uint8Array): string {
	return bytes.toBase64({ alphabet: "base64url", omitPadding: true });
}

function fromBase64Url(text: string): Uint8Array {
	return Uint8Array.fromBase64(text, { alphabet: "base64url" });
}

function invalidEvent(message: string, attributes: Record<string, unknown>): FokosValidationError {
	return new FokosValidationError(SHARDING_VALIDATION_CODES.topology_event_invalid, { message, attributes });
}

function etagOf(epoch: number, version: number | bigint): string {
	return `"${epoch}.${version}"`;
}

type DecodedId = ReturnType<typeof PartitionIdHelper.decode> & { bytes: Uint8Array };

/** The root index and the child path of a hash partition ID, or of a range partition ID: its hash key and interval. */
function decodePartitionId(partitionId: string): DecodedId {
	const bytes = PartitionIdHelper.partitionIdToBytes(partitionId);
	return { ...PartitionIdHelper.decode(bytes), bytes };
}

/**
 * The topology keeper of one or more shard groups. It is idempotent by the report id, and it accepts
 * reports in any order: registration writes each fact of a report, with the ancestors of a hash split,
 * into the fact tables, and the build reads only those tables.
 */
export class FokosTopologyKeeper {
	readonly #storage: DurableObjectStorage;
	readonly #buildDelayMs: number;
	readonly #maxRangeBodyBytes: number;
	readonly #buildStepRows: number;
	readonly #scheduleBuild: FokosTopologyKeeperOptions["scheduleBuild"];
	/** The last hash body of each shard group, so a read answers with no SQL. */
	readonly #hashBodies = new Map<string, { etag: string; body: Uint8Array }>();

	constructor(storage: DurableObjectStorage, opts: FokosTopologyKeeperOptions) {
		this.#storage = storage;
		this.#buildDelayMs = opts.buildDelayMs ?? DEFAULT_BUILD_DELAY_MS;
		this.#maxRangeBodyBytes = opts.maxRangeBodyBytes ?? DEFAULT_MAX_RANGE_BODY_BYTES;
		this.#buildStepRows = opts.buildStepRows ?? DEFAULT_BUILD_STEP_ROWS;
		this.#scheduleBuild = opts.scheduleBuild;
	}

	/** Applies the schema. Call it once, before any other method, inside `blockConcurrencyWhile`. */
	runMigrations(): void {
		new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: this.#storage,
			keyNameTrackingLastMigrationID: "__fokos/topo_sql_schema_version",
		}).runAllSync();
	}

	/**
	 * Stores one report of a completed repartition, in its JSON form. A repeated report, and a report
	 * that completed before the last reset of its shard group, change nothing.
	 */
	async registerRepartition(wire: unknown, now = Date.now()): Promise<void> {
		const event = topologyEventFromWire(wire);
		validateTopology({ shardGroup: event.shardGroup, rootTreesN: event.rootTreesN, hashSplitN: event.hashSplitN });
		const { source, repartitionKey } = this.#checkSource(event);
		const sql = this.#storage.sql;
		const dueAt = this.#storage.transactionSync((): number | null => {
			const group = this.#group(event.shardGroup);
			if (group && event.completedAt < group.reset_at) {
				return null;
			}
			if (group?.root_trees_n != null && (group.root_trees_n !== event.rootTreesN || group.hash_split_n !== event.hashSplitN)) {
				throw invalidEvent("rootTreesN and hashSplitN of a shard group never change", {
					shardGroup: event.shardGroup,
					stored: { rootTreesN: group.root_trees_n, hashSplitN: group.hash_split_n },
					event: { rootTreesN: event.rootTreesN, hashSplitN: event.hashSplitN },
				});
			}
			// A repeated report changes nothing.
			if (tryOne(sql.exec(`SELECT 1 FROM fokos_topo_repartitions WHERE shard_group = ?1 AND id = ?2`, event.shardGroup, event.id))) {
				return group?.build_due_at ?? null;
			}
			// Each new fact gets the version that this report gives the shard group when it writes a new fact.
			const previous = group?.version ?? 0;
			const hashNodes = event.kind === "hash_split" ? this.#insertHashNodes(event.shardGroup, source.bytes, previous + 1) : 0;
			const range = this.#insertRangeIntervals(event, source, previous + 1);
			// Only a new fact moves `version`. A new hash node or a new range root changes the hash body.
			const hashChanged = hashNodes > 0 || range.roots > 0;
			const changed = hashChanged || range.intervals > 0;
			const version = changed ? previous + 1 : previous;
			sql.exec(
				`INSERT INTO fokos_topo_repartitions
				   (shard_group, id, kind, seq, completed_at, received_at, actor_id, repartition_key, data)
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, jsonb(?9))`,
				event.shardGroup,
				event.id,
				event.kind,
				version,
				event.completedAt,
				now,
				event.actorId,
				repartitionKey,
				topologyEventToJson(event),
			);
			const buildDueAt = group?.build_due_at ?? (changed ? now + this.#buildDelayMs : null);
			sql.exec(
				`INSERT INTO fokos_topo_groups
				   (shard_group, root_trees_n, hash_split_n, version, hash_version, build_due_at, promoted_n, hash_nodes_n)
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
				 ON CONFLICT (shard_group) DO UPDATE SET
				     root_trees_n = ?2, hash_split_n = ?3, version = ?4,
				     hash_version = CASE WHEN ?9 THEN ?4 ELSE hash_version END,
				     build_due_at = COALESCE(build_due_at, ?6),
				     promoted_n = promoted_n + ?7, hash_nodes_n = hash_nodes_n + ?8`,
				event.shardGroup,
				event.rootTreesN,
				event.hashSplitN,
				version,
				hashChanged ? version : 0,
				buildDueAt,
				range.roots,
				hashNodes,
				hashChanged ? 1 : 0,
			);
			return buildDueAt;
		});
		if (dueAt !== null) {
			await this.#scheduleBuild(dueAt);
		}
	}

	/**
	 * Deletes the reports and the bodies of a shard group, and starts a new epoch. A report that
	 * completed before now is then ignored.
	 */
	resetShardGroup(shardGroup: string, now = Date.now()): void {
		const sql = this.#storage.sql;
		this.#storage.transactionSync(() => {
			sql.exec(`INSERT OR IGNORE INTO fokos_topo_groups (shard_group) VALUES (?1)`, shardGroup);
			sql.exec(
				`UPDATE fokos_topo_groups
				    SET epoch = epoch + 1, version = 0, hash_version = 0, built_version = 0, reset_at = ?2,
				        build_due_at = NULL, root_trees_n = NULL, hash_split_n = NULL, promoted_n = 0, hash_nodes_n = 0
				  WHERE shard_group = ?1`,
				shardGroup,
				now,
			);
			sql.exec(`DELETE FROM fokos_topo_repartitions WHERE shard_group = ?1`, shardGroup);
			sql.exec(`DELETE FROM fokos_topo_range_intervals WHERE shard_group = ?1`, shardGroup);
			sql.exec(`DELETE FROM fokos_topo_hash_nodes WHERE shard_group = ?1`, shardGroup);
			sql.exec(`DELETE FROM fokos_topo_range_dirty WHERE shard_group = ?1`, shardGroup);
			sql.exec(`DELETE FROM fokos_topo_bodies WHERE shard_group = ?1`, shardGroup);
		});
		this.#hashBodies.delete(shardGroup);
	}

	/** The last built hash body. A shard group with no build answers an empty topology of version 0. */
	getTopology(shardGroup: string, ifNoneMatch?: string): FokosTopologyRead {
		let cached = this.#hashBodies.get(shardGroup);
		if (!cached) {
			const group = this.#group(shardGroup);
			const epoch = group?.epoch ?? 0;
			const builtVersion = group?.built_version ?? 0;
			const stored = builtVersion > 0 ? this.#readBody(shardGroup, "hash", NO_KEY) : undefined;
			const body =
				stored ?? encodeHashBody({ epoch, version: 0, rootTreesN: 0, hashSplitN: 0, bitmap: { bits: NO_KEY, bitCount: 0 }, bloom: null });
			cached = { etag: etagOf(epoch, stored ? builtVersion : 0), body };
			this.#hashBodies.set(shardGroup, cached);
		}
		return answer(cached.etag, cached.body, ifNoneMatch);
	}

	/** The last built range body of one hash key. A key with no body answers an empty body of version 0. */
	getRangeTopology(shardGroup: string, hashKey: Uint8Array, ifNoneMatch?: string): FokosTopologyRead {
		const stored = this.#readBody(shardGroup, "range", hashKey);
		if (stored) {
			const { epoch, version } = readBodyVersion(stored, RANGE_BODY_MAGIC);
			return answer(etagOf(epoch, version), stored, ifNoneMatch);
		}
		// Version 0, so the first stored body of the key always has another ETag.
		const epoch = this.#group(shardGroup)?.epoch ?? 0;
		const body = encodeRangeBody({ epoch, version: 0, boundaries: [], levels: [], depths: [], truncatedAtRank: null });
		return answer(etagOf(epoch, 0), body, ifNoneMatch);
	}

	/** The earliest build that is due, or null. */
	nextBuildAt(): number | null {
		return one(this.#storage.sql.exec<{ at: number | null }>(`SELECT MIN(build_due_at) AS at FROM fokos_topo_groups`)).at;
	}

	/**
	 * Does the next part of each build that is due at `now`, until it read `buildStepRows` rows. A build
	 * that is not complete stays due at `now`, so `nextBuildAt()` gives `now` and the host calls again.
	 * The bound is in rows and not in time, because the clock of a Worker does not move while the code
	 * runs with no I/O.
	 */
	runDueBuilds(now = Date.now()): void {
		// The names are read first, because each step writes to the table that the cursor reads.
		const due: string[] = [];
		for (const row of this.#storage.sql.exec<{ shard_group: string }>(
			`SELECT shard_group FROM fokos_topo_groups WHERE build_due_at <= ?1 ORDER BY build_due_at`,
			now,
		)) {
			due.push(row.shard_group);
		}
		let rows = 0;
		for (const shardGroup of due) {
			if (rows >= this.#buildStepRows) {
				break;
			}
			rows += this.#buildStep(shardGroup, now, this.#buildStepRows - rows);
		}
	}

	// ─── reports ─────────────────────────────────────────────────────────────

	/**
	 * Checks the source and the targets of the report against its kind and its topology values, and
	 * gives the decoded source ID and the `repartition_key` of the row. A hash ID with a root index or a
	 * child index outside the topology would put a bit at the position of another partition.
	 */
	#checkSource(event: FokosTopologyEvent): { source: DecodedId; repartitionKey: string } {
		let source: DecodedId;
		try {
			source = decodePartitionId(event.source.partitionId);
		} catch {
			throw invalidEvent("the source partition ID is not valid", { id: event.id });
		}
		const sliceKind = { hash_split: "hash_child", range_split: "range", key_promotion: "promoted_key" }[event.kind];
		if (event.targets.some((t) => t.slice.kind !== sliceKind)) {
			throw invalidEvent(`a ${event.kind} has ${sliceKind} slices only`, { id: event.id });
		}
		if (event.kind === "range_split") {
			if (source.schema !== PartitionIdHelper.SCHEMA_RANGE_V1) {
				throw invalidEvent("the source of a range_split is a range partition", { id: event.id });
			}
			const hashKey = source.hashKey;
			if (event.targets.some((t) => t.slice.kind === "range" && KeyCodec.compare(t.slice.hashKey, hashKey) !== 0)) {
				throw invalidEvent("the targets of a range_split have the hash key of the source", { id: event.id });
			}
			return { source, repartitionKey: toBase64Url(hashKey) };
		}
		if (source.schema !== PartitionIdHelper.SCHEMA_HASH_V1) {
			throw invalidEvent(`the source of a ${event.kind} is a hash partition`, { id: event.id });
		}
		const path = source.bytes.subarray(4);
		if (source.rootIdx >= event.rootTreesN || path.length !== source.depth || path.some((c) => c >= event.hashSplitN)) {
			throw invalidEvent("the source partition ID is outside the topology of the shard group", { id: event.id });
		}
		if (event.kind === "hash_split" && event.targets.length !== event.hashSplitN) {
			throw invalidEvent("a hash_split has hashSplitN targets", { id: event.id });
		}
		// The build reads the promoted key of the first target only.
		if (event.kind === "key_promotion" && event.targets.length !== 1) {
			throw invalidEvent("a key_promotion has one target", { id: event.id });
		}
		return { source, repartitionKey: event.source.partitionId };
	}

	/**
	 * A promotion and a range split insert the range root, the source interval, and each target
	 * interval of the hash key. A range split exists only after its promotion, so the range root is real
	 * also when the report of the promotion is missing. A new interval marks the hash key dirty. Returns
	 * the number of new range roots and of new intervals, roots included.
	 */
	#insertRangeIntervals(event: FokosTopologyEvent, source: DecodedId, seq: number): { roots: number; intervals: number } {
		const sql = this.#storage.sql;
		const insert = (hashKey: Uint8Array, start: Uint8Array | null, end: Uint8Array | null): number =>
			sql.exec(
				`INSERT OR IGNORE INTO fokos_topo_range_intervals (shard_group, hash_key, start, end_is_max, end, seq)
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6)`,
				event.shardGroup,
				hashKey,
				start ?? NO_KEY,
				end === null ? 1 : 0,
				end ?? NO_KEY,
				seq,
			).rowsWritten;
		if (event.kind === "hash_split") {
			return { roots: 0, intervals: 0 };
		}
		let hashKey: Uint8Array;
		let roots: number;
		let intervals: number;
		if (event.kind === "key_promotion") {
			hashKey = (event.targets[0].slice as { hashKey: KeyBytes }).hashKey;
			roots = insert(hashKey, null, null);
			intervals = roots;
		} else {
			invariant(source.schema === PartitionIdHelper.SCHEMA_RANGE_V1, "fokos/topology-keeper: a range_split has a range source");
			// `#checkSource` checked that each target has the hash key of the source.
			hashKey = source.hashKey;
			roots = insert(hashKey, null, null);
			intervals = roots + insert(hashKey, source.startBoundary, source.endBoundary);
			for (const { slice } of event.targets) {
				if (slice.kind === "range") {
					intervals += insert(hashKey, slice.start, slice.end);
				}
			}
		}
		if (intervals > 0) {
			sql.exec(`INSERT OR IGNORE INTO fokos_topo_range_dirty (shard_group, hash_key) VALUES (?1, ?2)`, event.shardGroup, hashKey);
		}
		return { roots, intervals };
	}

	/**
	 * Inserts the node of a split hash partition and the nodes of its ancestors that are not known, and
	 * returns the number of new nodes. A known node has all its ancestors, so the walk stops at the
	 * first known node.
	 */
	#insertHashNodes(shardGroup: string, partitionIdBytes: Uint8Array, seq: number): number {
		const node = hashNodeKey(partitionIdBytes);
		let inserted = 0;
		for (let depth = node[0]; depth >= 0; depth--) {
			const ancestor = node.slice(0, 3 + depth);
			ancestor[0] = depth;
			const written = this.#storage.sql.exec(
				`INSERT OR IGNORE INTO fokos_topo_hash_nodes (shard_group, node, seq) VALUES (?1, ?2, ?3)`,
				shardGroup,
				ancestor,
				seq,
			).rowsWritten;
			if (written === 0) {
				break;
			}
			inserted += 1;
		}
		return inserted;
	}

	#group(shardGroup: string): GroupRow | undefined {
		return tryOne(this.#storage.sql.exec<GroupRow>(`SELECT * FROM fokos_topo_groups WHERE shard_group = ?1`, shardGroup));
	}

	// ─── the build ───────────────────────────────────────────────────────────

	/**
	 * One step of the build of a shard group, with a budget of `budget` rows. Returns the rows that it read.
	 *
	 * 1. When `hash_version > built_version`, it builds the hash body, and stores it with
	 *    `built_version = hash_version` in one transaction.
	 * 2. It builds the range body of each dirty hash key, each in its own transaction, until the budget.
	 * 3. When the hash body is built and no key is dirty, it clears `build_due_at`. Otherwise the build
	 *    stays due at `now`.
	 *
	 * The two kinds of body do not wait for each other. A report can arrive between two steps, and the
	 * next step builds what it changed. No report arrives during a step, because a step does not yield.
	 */
	#buildStep(shardGroup: string, now: number, budget: number): number {
		const sql = this.#storage.sql;
		const group = this.#group(shardGroup);
		if (!group || group.root_trees_n === null || group.hash_split_n === null) {
			this.#storage.transactionSync(() => sql.exec(`UPDATE fokos_topo_groups SET build_due_at = NULL WHERE shard_group = ?1`, shardGroup));
			return 0;
		}
		let rows = 0;
		if (group.hash_version > group.built_version) {
			rows += this.#buildHashBody(group, group.root_trees_n, group.hash_split_n);
		}
		for (;;) {
			const dirty = tryOne(
				sql.exec<{ hash_key: ArrayBuffer }>(`SELECT hash_key FROM fokos_topo_range_dirty WHERE shard_group = ?1 LIMIT 1`, shardGroup),
			);
			if (dirty === undefined) {
				break;
			}
			if (rows >= budget) {
				sql.exec(`UPDATE fokos_topo_groups SET build_due_at = ?2 WHERE shard_group = ?1`, shardGroup, now);
				return rows;
			}
			const hashKey = new Uint8Array(dirty.hash_key);
			this.#storage.transactionSync(() => {
				const built = this.#buildRangeBody(shardGroup, hashKey, group);
				rows += built.rows;
				this.#writeBody(shardGroup, "range", hashKey, built.body);
				sql.exec(`DELETE FROM fokos_topo_range_dirty WHERE shard_group = ?1 AND hash_key = ?2`, shardGroup, hashKey);
			});
		}
		sql.exec(`UPDATE fokos_topo_groups SET build_due_at = NULL WHERE shard_group = ?1`, shardGroup);
		return rows;
	}

	/**
	 * Builds and stores the hash body of `hash_version`. Returns the rows that it read.
	 *
	 * With a stored hash body, the build reads only the hash nodes and the range roots with
	 * `seq > built_version`, which the stored body does not have. It merges the new nodes into the
	 * stored bitmap (`mergeHashBitmap`), and adds the new roots to a copy of the stored Bloom filter.
	 * The first build of an epoch reads all the rows.
	 */
	#buildHashBody(group: GroupRow, rootTreesN: number, hashSplitN: number): number {
		const sql = this.#storage.sql;
		const shardGroup = group.shard_group;
		const builtVersion = group.built_version;
		let rows = 0;
		const nodes = function* (afterSeq: number) {
			for (const row of sql.exec<{ node: ArrayBuffer }>(
				`SELECT node FROM fokos_topo_hash_nodes INDEXED BY idx_fokos_topo_hash_nodes_seq
				  WHERE shard_group = ?1 AND seq > ?2 ORDER BY node`,
				shardGroup,
				afterSeq,
			)) {
				rows += 1;
				yield new Uint8Array(row.node);
			}
		};
		const allNodes = function* () {
			for (const row of sql.exec<{ node: ArrayBuffer }>(
				`SELECT node FROM fokos_topo_hash_nodes WHERE shard_group = ?1 ORDER BY node`,
				shardGroup,
			)) {
				rows += 1;
				yield new Uint8Array(row.node);
			}
		};
		const promoted = function* (afterSeq: number) {
			for (const row of sql.exec<{ hash_key: ArrayBuffer }>(
				`SELECT hash_key FROM fokos_topo_range_intervals INDEXED BY idx_fokos_topo_range_roots
				  WHERE shard_group = ?1 AND seq > ?2 AND start = x'' AND end_is_max = 1`,
				shardGroup,
				afterSeq,
			)) {
				rows += 1;
				yield new Uint8Array(row.hash_key);
			}
		};
		let old: HashTopologyView | undefined;
		if (builtVersion > 0) {
			const stored = this.#hashBodies.get(shardGroup);
			const body = stored?.etag === etagOf(group.epoch, builtVersion) ? stored.body : this.#readBody(shardGroup, "hash", NO_KEY);
			invariant(body !== undefined, "fokos/topology-keeper: a built hash body is not stored");
			old = parseHashBody(body);
		}
		const hashBody = encodeHashBody({
			epoch: group.epoch,
			version: group.hash_version,
			rootTreesN,
			hashSplitN,
			bitmap: old
				? mergeHashBitmap(old, group.hash_nodes_n, nodes(builtVersion))
				: encodeHashBitmap(rootTreesN, hashSplitN, group.hash_nodes_n, allNodes),
			bloom: promotedKeysBloom(
				group.promoted_n,
				() => promoted(0),
				old?.bloom ? { base: old.bloom, added: () => promoted(builtVersion) } : undefined,
			),
		});
		this.#storage.transactionSync(() => {
			this.#writeBody(shardGroup, "hash", NO_KEY, hashBody);
			sql.exec(`UPDATE fokos_topo_groups SET built_version = ?2 WHERE shard_group = ?1`, shardGroup, group.hash_version);
		});
		this.#hashBodies.set(shardGroup, { etag: etagOf(group.epoch, group.hash_version), body: hashBody });
		return rows;
	}

	/**
	 * The range body of one hash key, from its intervals in primary-key order. The first pass also
	 * finds the size of the boundaries at each level. Only a body above `maxRangeBodyBytes` reads the
	 * rows again, and keeps the ranks that fit. Two intervals that cross give a body with the range
	 * root only.
	 */
	#buildRangeBody(shardGroup: string, hashKey: Uint8Array, group: GroupRow): { body: Uint8Array; rows: number } {
		const sql = this.#storage.sql;
		let rows = 0;
		const scan = function* () {
			for (const row of sql.exec<IntervalRow>(
				`SELECT start, end_is_max, end FROM fokos_topo_range_intervals
				  WHERE shard_group = ?1 AND hash_key = ?2 ORDER BY start, end_is_max DESC, end DESC`,
				shardGroup,
				hashKey,
			)) {
				rows += 1;
				yield row;
			}
		};
		const encode = (built: RangeSweep, truncatedAtRank: number | null) => ({
			body: encodeRangeBody({ epoch: group.epoch, version: group.version, ...built, truncatedAtRank }),
			rows,
		});
		const first = sweepRangeIntervals(scan(), Number.POSITIVE_INFINITY);
		if (first === "crossed") {
			console.error({
				message: "fokos/topology-keeper: two known range intervals cross. The range body holds the range root only.",
				shardGroup,
				hashKey: KeyCodec.keyForLog(KeyCodec.asKeyBytes(hashKey)),
			});
			return encode({ boundaries: [], levels: [], depths: [0] }, null);
		}
		// A rough bound of the header and the padding of the sections.
		const fixed = 1024 + (first.boundaries.length + 1) * 4;
		const rankBytes = first.maxRank < 65_535 ? 2 : 4;
		const sizeBelow = (m: number) => {
			let size = fixed;
			for (let level = 0; level < m && level < first.bytesByLevel.length; level++) {
				size += (first.bytesByLevel[level] ?? 0) + (first.countByLevel[level] ?? 0) * 2 * rankBytes;
			}
			return size;
		};
		if (sizeBelow(first.bytesByLevel.length) <= this.#maxRangeBodyBytes) {
			return encode(first, null);
		}
		let keep = 0;
		while (keep + 1 <= first.maxRank && sizeBelow(keep + 1) <= this.#maxRangeBodyBytes) {
			keep += 1;
		}
		const second = sweepRangeIntervals(scan(), keep);
		invariant(second !== "crossed", "fokos/topology-keeper: the second pass found intervals that cross");
		return encode(second, keep);
	}

	/** The stored body, or undefined. A body of one chunk is the chunk itself, with no copy. */
	#readBody(shardGroup: string, kind: "hash" | "range", hashKey: Uint8Array): Uint8Array | undefined {
		let first: Uint8Array | undefined;
		let more: Uint8Array[] | undefined;
		let total = 0;
		for (const row of this.#storage.sql.exec<{ bytes: ArrayBuffer }>(
			`SELECT bytes FROM fokos_topo_bodies WHERE shard_group = ?1 AND kind = ?2 AND hash_key = ?3 ORDER BY chunk`,
			shardGroup,
			kind,
			hashKey,
		)) {
			const chunk = new Uint8Array(row.bytes);
			total += chunk.length;
			if (first === undefined) {
				first = chunk;
			} else {
				(more ??= []).push(chunk);
			}
		}
		if (first === undefined || more === undefined) {
			return first;
		}
		const body = new Uint8Array(total);
		body.set(first, 0);
		let at = first.length;
		for (const chunk of more) {
			body.set(chunk, at);
			at += chunk.length;
		}
		return body;
	}

	#writeBody(shardGroup: string, kind: "hash" | "range", hashKey: Uint8Array, body: Uint8Array): void {
		const sql = this.#storage.sql;
		sql.exec(`DELETE FROM fokos_topo_bodies WHERE shard_group = ?1 AND kind = ?2 AND hash_key = ?3`, shardGroup, kind, hashKey);
		for (let chunk = 0; chunk * TOPOLOGY_BODY_CHUNK_BYTES < body.length; chunk++) {
			sql.exec(
				`INSERT INTO fokos_topo_bodies (shard_group, kind, hash_key, chunk, bytes) VALUES (?1, ?2, ?3, ?4, ?5)`,
				shardGroup,
				kind,
				hashKey,
				chunk,
				body.subarray(chunk * TOPOLOGY_BODY_CHUNK_BYTES, (chunk + 1) * TOPOLOGY_BODY_CHUNK_BYTES),
			);
		}
	}
}

function answer(etag: string, body: Uint8Array, ifNoneMatch: string | undefined): FokosTopologyRead {
	return ifNoneMatch === etag ? { status: 304, etag } : { status: 200, etag, body };
}

/** The range model of one hash key, as `encodeRangeBody` takes it. */
type RangeSweep = { boundaries: Uint8Array[]; levels: number[]; depths: number[] };

/** The sizes that the first pass finds. */
type RangeSweepStats = { maxRank: number; bytesByLevel: number[]; countByLevel: number[] };

/**
 * One pass over the known intervals of one hash key, in the order `start` ascending, then `end`
 * descending. A stack holds the open ancestors of the current interval, so the rank of an interval is
 * the rank of its nearest known ancestor plus 1.
 *
 * - A boundary is a start or an end of a known interval, other than `~min` and `~max`. Its level is
 *   the rank of the deepest known interval that has it strictly inside.
 * - The depth of a segment is the rank of the deepest known interval that holds it.
 * - An interval with a rank above `maxKeptRank` is skipped, and so are its boundaries.
 *
 * The pass also counts the bytes of the boundaries at each level, for the size cap. Returns "crossed"
 * when two intervals cross.
 */
function sweepRangeIntervals(rows: Iterable<IntervalRow>, maxKeptRank: number): (RangeSweep & RangeSweepStats) | "crossed" {
	const out: RangeSweep & RangeSweepStats = {
		boundaries: [],
		levels: [],
		depths: [0],
		maxRank: 0,
		bytesByLevel: [],
		countByLevel: [],
	};
	/** `end` null is ~max. */
	const stack: Array<{ end: KeyBytes | null; rank: number }> = [];
	let last: KeyBytes | null = null;
	const emit = (boundary: KeyBytes, level: number) => {
		last = boundary;
		out.bytesByLevel[level] = (out.bytesByLevel[level] ?? 0) + boundary.length + 4;
		out.countByLevel[level] = (out.countByLevel[level] ?? 0) + 1;
		out.boundaries.push(boundary);
		out.levels.push(level);
		out.depths.push(level);
	};
	/** Closes each open interval that ends at or before `start` (null: the end of the rows). */
	const closeBefore = (start: KeyBytes | null) => {
		for (;;) {
			const top = stack.at(-1);
			if (!top || top.end === null || (start !== null && KeyCodec.compare(top.end, start) > 0)) {
				return;
			}
			const end = top.end;
			while (stack.length > 0 && stack.at(-1)!.end !== null && KeyCodec.compare(stack.at(-1)!.end!, end) === 0) {
				stack.pop();
			}
			// An end that is also the next start gets its boundary from that start.
			if (start === null || KeyCodec.compare(end, start) < 0) {
				emit(end, stack.at(-1)?.rank ?? -1);
			}
		}
	};
	for (const row of rows) {
		const start = KeyCodec.asKeyBytes(new Uint8Array(row.start));
		const end = row.end_is_max === 1 ? null : KeyCodec.asKeyBytes(new Uint8Array(row.end));
		closeBefore(start);
		const top = stack.at(-1);
		if (top && top.end !== null && (end === null || KeyCodec.compare(top.end, end) < 0)) {
			return "crossed";
		}
		const rank = top ? top.rank + 1 : 0;
		if (rank > maxKeptRank) {
			continue;
		}
		if (start.length > 0 && (last === null || KeyCodec.compare(last, start) !== 0)) {
			emit(start, top?.rank ?? -1);
		}
		out.depths[out.depths.length - 1] = rank;
		out.maxRank = Math.max(out.maxRank, rank);
		stack.push({ end, rank });
	}
	closeBefore(null);
	return out;
}

// ─── the default admin Durable Object ────────────────────────────────────────

/**
 * A Durable Object that holds the topology of the shard groups that report to it, with one
 * `FokosTopologyKeeper`. The host gives each shard group its own instance, `topologyAdminName(shardGroup)`.
 * The alarm runs the builds. A subclass can give the keeper options with `keeperOptions()`.
 */
export class FokosShardingAutoAdminDO<Env = unknown> extends DurableObject<Env> {
	protected readonly keeper: FokosTopologyKeeper;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.keeper = new FokosTopologyKeeper(ctx.storage, {
			...this.keeperOptions(),
			scheduleBuild: async (at) => {
				const existing = await ctx.storage.getAlarm();
				if (existing === null || at < existing) {
					await ctx.storage.setAlarm(at);
				}
			},
		});
		void ctx.blockConcurrencyWhile(async () => this.keeper.runMigrations());
	}

	/** The options of the keeper. The constructor reads them once. */
	protected keeperOptions(): Omit<FokosTopologyKeeperOptions, "scheduleBuild"> {
		return {};
	}

	async registerRepartition(event: unknown): Promise<void> {
		await this.keeper.registerRepartition(event);
	}

	async resetShardGroup(shardGroup: string): Promise<void> {
		this.keeper.resetShardGroup(shardGroup);
	}

	async getTopology(shardGroup: string, ifNoneMatch?: string): Promise<FokosTopologyRead> {
		return this.keeper.getTopology(shardGroup, ifNoneMatch);
	}

	async getRangeTopology(shardGroup: string, hashKey: Uint8Array, ifNoneMatch?: string): Promise<FokosTopologyRead> {
		return this.keeper.getRangeTopology(shardGroup, hashKey, ifNoneMatch);
	}

	async alarm(info?: AlarmInvocationInfo): Promise<void> {
		// The handler can start before the scheduled time on the clock of this object, and the platform
		// does not deliver an alarm that its own handler sets at the same time. So the builds that are
		// due at the scheduled time run now.
		const now = Math.max(Date.now(), info?.scheduledTime ?? 0);
		this.keeper.runDueBuilds(now);
		// A build that is not complete is due at `now`. The next alarm continues it, so each alarm
		// blocks the object for one bounded step, and the reads run between two steps.
		const next = this.keeper.nextBuildAt();
		if (next !== null) {
			await this.ctx.storage.setAlarm(Math.max(next, now + 1));
		}
	}
}

// ─── the topology entrypoint ─────────────────────────────────────────────────

export type FokosTopologyEntrypointOptions = {
	/** Host code: the stub of `topologyAdminName(shardGroup)`. */
	admin(this: void, shardGroup: string): FokosTopologyAdminRpc;
	/** `max-age` of a topology answer. 0 makes every request revalidate against the admin Durable Object. */
	maxAgeS: number;
	/** `stale-while-revalidate` of a topology answer. */
	staleWhileRevalidateS: number;
};

const utf8Decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

function noStore(status: number): Response {
	return new Response(null, { status, headers: { "Cache-Control": "no-store" } });
}

/**
 * Serves the encoded topology for the `fetch()` of a `WorkerEntrypoint` of the host that has Workers
 * Cache enabled. It calls the admin Durable Object only on a cache miss or a revalidation.
 *
 * - `GET /internal/v1/topology.Get?sg=<base64url>`: the hash body of the shard group.
 * - `GET /internal/v1/topology.GetForRange?sg=<base64url>&hk=<base64url>`: the range body of one hash key.
 *
 * A topology answer has `Cache-Control` and `ETag`, and an `If-None-Match` that equals the ETag gets
 * a 304. Every other answer has `Cache-Control: no-store`.
 */
export async function fokosTopologyEntrypointFetch(request: Request, opts: FokosTopologyEntrypointOptions): Promise<Response> {
	const url = new URL(request.url);
	const get = url.pathname === "/internal/v1/topology.Get";
	if (!get && url.pathname !== "/internal/v1/topology.GetForRange") {
		return noStore(404);
	}
	if (request.method !== "GET" && request.method !== "HEAD") {
		return noStore(405);
	}
	let shardGroup: string;
	let hashKey: Uint8Array | undefined;
	try {
		shardGroup = utf8Decoder.decode(fromBase64Url(url.searchParams.get("sg") ?? ""));
		hashKey = get ? undefined : fromBase64Url(url.searchParams.get("hk") ?? "");
	} catch {
		return noStore(400);
	}
	if (shardGroup.length === 0 || hashKey?.length === 0) {
		return noStore(400);
	}
	const ifNoneMatch = request.headers.get("If-None-Match")?.replace(/^W\//, "") ?? undefined;
	let read: FokosTopologyRead;
	try {
		const admin = opts.admin(shardGroup);
		read = hashKey ? await admin.getRangeTopology(shardGroup, hashKey, ifNoneMatch) : await admin.getTopology(shardGroup, ifNoneMatch);
	} catch (error) {
		console.error({ message: "fokos/topology-entrypoint: the admin Durable Object failed.", shardGroup, error: String(error) });
		return noStore(503);
	}
	const headers = {
		"Cache-Control": `public, max-age=${opts.maxAgeS}, stale-while-revalidate=${opts.staleWhileRevalidateS}`,
		ETag: read.etag,
		"Content-Type": "application/octet-stream",
	};
	if (read.status === 304) {
		return new Response(null, { status: 304, headers });
	}
	return new Response(request.method === "HEAD" ? null : read.body, { status: 200, headers });
}
