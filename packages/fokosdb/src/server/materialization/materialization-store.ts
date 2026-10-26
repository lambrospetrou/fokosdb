/**
 * The SQLite storage of `FokosDBMaterializationDO`: the latest image of each base item that the partition
 * accepted, the highest epoch of each producer, the change positions that the targets read, the state
 * of each target, and the failed items of each target. The size estimate of each base hash key
 * belongs to `keySizes`, which this store updates in the transaction of each image write.
 */
import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import { DATA_KINDS, type DataKind } from "../../shared/types.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { exists, one, tryOne } from "../../shared/sql-cursor.js";
import { JSON_KIND_CODE } from "../../shared/partition/item-size.js";
import { KeySizeStats } from "../../shared/partition/key-size-stats.js";
import {
	walkByHashKey,
	type HashKeyWalkCursor,
	type HashKeyWalkEntry,
	type KeyRange,
	type ScanCursor,
} from "../../shared/partition/partition-store.js";
import type { FokosTargetDefinition } from "./targets.js";
import { materializedPlanBindings } from "../../shared/expression/bindings.js";
import type { CompiledProjectionPlan } from "../../shared/expression/plan.js";
import { decodeProjectedRow, type ProjectedWireRow } from "../../shared/expression/projection.js";

/** The KV key of the target list. The first valid list is stored, and the partition uses only it. */
const TARGETS_KV_KEY = "__fokosdb/mat/targets";

/**
 * The KV key of the cursor snapshots of the promotions of this partition that are between their
 * cutover and their completion: the cursor of each target at the cutover, by the promoted hash key.
 */
const CURSOR_SNAPSHOTS_KV_KEY = "__fokosdb/mat/cursor_snapshots";

/**
 * A Map, not an object: a `targetId` such as `__proto__` is valid, and an object would not keep it
 * as an own key.
 */
type CursorSnapshots = Map<string, Map<string, number>>;

/**
 * One image that a producer exports. A live image carries the stored representation of the base item
 * (a json item as its JSONB blob), so the store writes it unchanged. A delete carries no data.
 */
export type LatestImage =
	| {
			opIndex: number;
			hashKey: KeyBytes;
			sortKey: KeyBytes;
			revision: number;
			deleted: false;
			data: string | Uint8Array;
			kind: DataKind;
			ttlAt: number | null;
	  }
	| { opIndex: number; hashKey: KeyBytes; sortKey: KeyBytes; revision: number; deleted: true };

/** The fixed part of the `est_row_bytes` of a `latest_items` row. The migration tells what it covers. */
const LATEST_ROW_BYTES_K = 60;

/**
 * The SQL of the `est_row_bytes` of a `latest_items` row, with the base hash key in `?1` and the base
 * sort key in `?2`. `dataParam` holds the stored `data` value, or is null for a tombstone.
 */
function latestRowBytesExpr(dataParam: string | null): string {
	const fixed = `2 * (octet_length(?1) + octet_length(?2)) + ${LATEST_ROW_BYTES_K}`;
	return dataParam === null ? fixed : `octet_length(${dataParam}) + ${fixed}`;
}

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create the latest images, the producer fences, the change positions, the target state, and the ancestor fences",
		// latest_items:
		// - One row for each base item: its newest live image, or a tombstone (`deleted_at_ms` is not
		//   NULL, `data` is NULL). `revision` is the base revision of the image.
		// - A rowid table with `data` last, for the reasons of the items table of PartitionStore: a large
		//   payload does not get a private overflow page, and a read of the metadata columns reads no
		//   overflow page.
		// - `item_id` links the rows of other tables to the item, so `(base_hk, base_sk)` is stored one
		//   time. Each statement that deletes a row must delete its linked rows in the same transaction,
		//   because SQLite can give the id to a new row again.
		// - No rule reads the value of `deleted_at_ms`. It orders the garbage collection of tombstones.
		// - `est_row_bytes` is the size estimate of the row, from `latestRowBytesExpr`. The keys count
		//   two times, because the UNIQUE index holds a second copy of them. LATEST_ROW_BYTES_K gives the
		//   fixed part: the integer columns, the record header, the rowid, and the fixed part of the
		//   index entry. It is an approximate value for size accounting (promotion, split, batch and page
		//   budgets). A change of the formula needs no data migration, because each change of a key
		//   size estimate uses the value that the row stores.
		//
		// producer_fences:
		// - The highest epoch of each producer, by its DurableObjectId in hex. A call with a lower epoch
		//   applies nothing. A row is never deleted: an absent row has the epoch 0, so a late call of that
		//   producer would apply again.
		//
		// change_positions:
		// - The positions that all targets read in `seq` order. `latest_items.newest_seq` is the newest
		//   position of an item, and `prev_seq` links to the older position of the same item. It has no
		//   secondary index: the ingest finds the positions of an item through these two links.
		//
		// materialization_targets:
		// - One row for each target: its cursor (each position at or below it is done for the target),
		//   its producer epoch, and `is_batch_open` (1 while a batch of the target can be in flight).
		//
		// target_retry:
		// - The failed items of each target. A 'retry' row owns its item for its target: the cursor skips
		//   the item, and the row sends the latest image after its backoff. A 'violation' row records a
		//   permanent failure, and does not own its item. `is_inherited` is 1 on a row that a split copied.
		//
		// ancestor_fences:
		// - The ancestors of a split child whose late calls each receiver of a target must reject. Each
		//   call of the target carries all rows of the target. `until_id` is the highest copied position:
		//   the rows of a target go when its cursor reaches it and no inherited `retry` row of the target
		//   is left.
		sql: `
            CREATE TABLE IF NOT EXISTS latest_items (
                item_id       INTEGER PRIMARY KEY,
                base_hk       BLOB    NOT NULL,
                base_sk       BLOB    NOT NULL DEFAULT x'',
                revision      INTEGER NOT NULL,
                deleted_at_ms INTEGER,
                newest_seq    INTEGER,
                data_kind     INTEGER,
                ttl_at        INTEGER,
                est_row_bytes INTEGER NOT NULL DEFAULT 0,
                data          ANY,
                UNIQUE (base_hk, base_sk)
            ) STRICT;

            CREATE INDEX IF NOT EXISTS idx_latest_items_deleted_gc ON latest_items (deleted_at_ms) WHERE deleted_at_ms IS NOT NULL;

            CREATE TABLE IF NOT EXISTS producer_fences (
                producer_id TEXT    PRIMARY KEY,
                epoch       INTEGER NOT NULL
            ) WITHOUT ROWID, STRICT;

            CREATE TABLE IF NOT EXISTS change_positions (
                seq      INTEGER PRIMARY KEY,
                item_id  INTEGER NOT NULL,
                prev_seq INTEGER
            ) STRICT;

            CREATE TABLE IF NOT EXISTS materialization_targets (
                target_id     TEXT    PRIMARY KEY,
                kind          TEXT    NOT NULL,
                cursor_seq    INTEGER NOT NULL DEFAULT 0,
                epoch         INTEGER NOT NULL DEFAULT 1,
                is_batch_open INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID, STRICT;

            CREATE TABLE IF NOT EXISTS target_retry (
                target_id    TEXT    NOT NULL,
                item_id      INTEGER NOT NULL,
                kind         TEXT    NOT NULL,
                not_before   INTEGER NOT NULL,
                attempts     INTEGER NOT NULL DEFAULT 0,
                is_inherited INTEGER NOT NULL DEFAULT 0,
                last_error   TEXT,
                PRIMARY KEY (target_id, item_id)
            ) STRICT;

            CREATE INDEX IF NOT EXISTS idx_target_retry_due ON target_retry (target_id, not_before) WHERE kind = 'retry';

            CREATE INDEX IF NOT EXISTS idx_target_retry_inherited ON target_retry (target_id) WHERE is_inherited = 1 AND kind = 'retry';

            CREATE TABLE IF NOT EXISTS ancestor_fences (
                target_id   TEXT    NOT NULL,
                ancestor_id TEXT    NOT NULL,
                min_epoch   INTEGER NOT NULL,
                until_id    INTEGER NOT NULL,
                PRIMARY KEY (target_id, ancestor_id)
            ) WITHOUT ROWID, STRICT;`,
	},
	{
		idMonotonicInc: 2,
		description: "Create the GSI key sets",
		// target_index_keys:
		// - One row for each GSI key `(index_hk, index_sk)` of a GSI target that can have an entry of the
		//   item. `index_sk` is empty when the GSI has no sort key. No row means that the GSI has no entry
		//   of the item. An item has 1 key, 2 keys while a key moves, and more only after deletes that did
		//   not apply.
		// - A rowid table with an index on `(item_id, target_id)` only, because the keys can be multiple
		//   kilobytes, and an index on the keys would store them two times. The code keeps each key unique.
		sql: `
            CREATE TABLE IF NOT EXISTS target_index_keys (
                key_id    INTEGER PRIMARY KEY,
                item_id   INTEGER NOT NULL,
                target_id TEXT    NOT NULL,
                index_hk  BLOB    NOT NULL,
                index_sk  BLOB    NOT NULL
            ) STRICT;

            CREATE INDEX IF NOT EXISTS idx_target_index_keys_item ON target_index_keys (item_id, target_id);`,
	},
];

export class MaterializationStore {
	/** The size estimate of each base hash key. */
	readonly keySizes: KeySizeStats;
	#storage: DurableObjectStorage;
	#migrations: SQLSchemaMigrations;

	constructor(storage: DurableObjectStorage) {
		this.#storage = storage;
		this.keySizes = new KeySizeStats(storage);
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: storage,
			keyNameTrackingLastMigrationID: "__fokosdb/mat/sql_schema_version",
		});
	}

	runMigrations(): void {
		this.#migrations.runAllSync();
		this.keySizes.runMigrations();
	}

	get databaseSize(): number {
		return this.#storage.sql.databaseSize;
	}

	transactionSync<T>(fn: () => T): T {
		return this.#storage.transactionSync(fn);
	}

	/** The stored epoch of a producer, or 0 when the producer never called. */
	producerEpoch(producerId: string): number {
		return (
			tryOne(this.#storage.sql.exec<{ epoch: number }>(`SELECT epoch FROM producer_fences WHERE producer_id = ?`, producerId))?.epoch ?? 0
		);
	}

	/**
	 * Raises the stored epoch of a producer to `epoch` when `epoch` is higher. It never lowers it. Keep
	 * the `WHERE` of the update: without it, the update writes the row (one billed row) also when the
	 * value does not change, for example `SET epoch = MAX(epoch, excluded.epoch)`.
	 */
	raiseProducerEpoch(producerId: string, epoch: number): void {
		this.#storage.sql.exec(
			`INSERT INTO producer_fences (producer_id, epoch) VALUES (?, ?)
			 ON CONFLICT (producer_id) DO UPDATE SET epoch = excluded.epoch WHERE excluded.epoch > producer_fences.epoch`,
			producerId,
			epoch,
		);
	}

	/** The stored image of a base item, live or tombstone, or undefined when it has none. */
	storedImage(hk: KeyBytes, sk: KeyBytes): StoredImage | undefined {
		const row = tryOne(
			this.#storage.sql.exec<{ item_id: number; revision: number; est_row_bytes: number; newest_seq: number | null }>(
				`SELECT item_id, revision, est_row_bytes, newest_seq FROM latest_items WHERE base_hk = ? AND base_sk = ?`,
				hk,
				sk,
			),
		);
		return row && { itemId: row.item_id, revision: row.revision, estRowBytes: row.est_row_bytes, newestSeq: row.newest_seq };
	}

	/**
	 * Stores an image that is newer than the stored one (`current`, or undefined when there is none).
	 * A live image replaces the data. A delete keeps the row as a tombstone with no data, so the space
	 * of the payload is free at once, and the tombstone holds the revision that a late export must not
	 * go below. The size estimate of the base hash key changes by the difference of the two row sizes.
	 *
	 * When targets exist, the image gets a change position (see `#planPosition`), and the `retry` rows
	 * of the item become due, because the cursor skips the item while it has a `retry` row.
	 *
	 * Returns the new size estimate of the base hash key when the size of the row changed, or undefined.
	 */
	putLatestImage(
		image: LatestImage,
		nowMs: number,
		current: StoredImage | undefined,
		targets: readonly TargetPositions[],
	): number | undefined {
		const sql = this.#storage.sql;
		const position = this.#planPosition(current?.newestSeq ?? null, targets);
		const newestSeq = position === null ? (current?.newestSeq ?? null) : position.seq;
		const row = image.deleted
			? sql.exec<{ item_id: number; est_row_bytes: number }>(
					`INSERT INTO latest_items (base_hk, base_sk, revision, deleted_at_ms, newest_seq, data_kind, ttl_at, est_row_bytes, data)
					 VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, ${latestRowBytesExpr(null)}, NULL)
					 ON CONFLICT (base_hk, base_sk) DO UPDATE SET
					   revision = excluded.revision, deleted_at_ms = excluded.deleted_at_ms, newest_seq = excluded.newest_seq,
					   data_kind = NULL, ttl_at = NULL, est_row_bytes = excluded.est_row_bytes, data = NULL
					 RETURNING item_id, est_row_bytes`,
					image.hashKey,
					image.sortKey,
					image.revision,
					nowMs,
					newestSeq,
				)
			: sql.exec<{ item_id: number; est_row_bytes: number }>(
					`INSERT INTO latest_items (base_hk, base_sk, revision, deleted_at_ms, newest_seq, data_kind, ttl_at, est_row_bytes, data)
					 VALUES (?1, ?2, ?3, NULL, ?7, ?4, ?5, ${latestRowBytesExpr("?6")}, ?6)
					 ON CONFLICT (base_hk, base_sk) DO UPDATE SET
					   revision = excluded.revision, deleted_at_ms = NULL, newest_seq = excluded.newest_seq,
					   data_kind = excluded.data_kind, ttl_at = excluded.ttl_at, est_row_bytes = excluded.est_row_bytes, data = excluded.data
					 RETURNING item_id, est_row_bytes`,
					image.hashKey,
					image.sortKey,
					image.revision,
					DATA_KINDS.indexOf(image.kind),
					image.ttlAt,
					image.data,
					newestSeq,
				);
		const { item_id: itemId, est_row_bytes: newEst } = row.one();
		if (position?.kind === "insert") {
			sql.exec(`INSERT INTO change_positions (seq, item_id, prev_seq) VALUES (?, ?, ?)`, position.seq, itemId, position.prevSeq);
		} else if (position?.kind === "move") {
			sql.exec(`UPDATE change_positions SET seq = ? WHERE seq = ?`, position.seq, position.fromSeq);
		}
		// A new item has no `retry` row: each delete of an item row deletes its linked rows.
		if (current !== undefined && targets.length > 0) {
			sql.exec(
				`UPDATE target_retry SET not_before = 0
				 WHERE target_id IN (SELECT target_id FROM materialization_targets) AND item_id = ? AND kind = 'retry' AND not_before > 0`,
				itemId,
			);
		}
		// The ingest reads the new estimate only when the key grew, so an equal size needs no read.
		const oldEst = current?.estRowBytes ?? 0;
		return newEst === oldEst ? undefined : this.keySizes.change(image.hashKey, oldEst, newEst);
	}

	/**
	 * The change position of an accepted image of an item whose newest position is `e`, or null when
	 * the image needs no new position. A position at or below the lowest cursor counts as absent.
	 *
	 * 1. When `e` is absent, the item gets a new position with no older position.
	 * 2. When no target read `e` (each `read < e`), each target reads the new image at `e`.
	 * 3. When each target that has not finished `e` also has not finished the older position `p`, `e`
	 *    moves to the new position. Each such target reaches `p` first, and each other target finds
	 *    the item at the new position.
	 * 4. Otherwise the item gets a new position that links to `e`.
	 *
	 * Thus only the newest position of an item moves, and only when no target depends on it, so a hot
	 * item cannot move ahead of a cursor that lags. Step 2 uses `read`, because a batch in flight can
	 * hold the old image from `e`. Step 3 uses the cursor, because a failed batch reads again from it.
	 */
	#planPosition(
		e: number | null,
		targets: readonly TargetPositions[],
	): { kind: "insert"; seq: number; prevSeq: number | null } | { kind: "move"; seq: number; fromSeq: number } | null {
		if (targets.length === 0) {
			return null;
		}
		const minCursor = Math.min(...targets.map((t) => t.cursor));
		const row =
			e !== null && e > minCursor
				? tryOne(this.#storage.sql.exec<{ prev_seq: number | null }>(`SELECT prev_seq FROM change_positions WHERE seq = ?`, e))
				: undefined;
		if (e === null || row === undefined) {
			return { kind: "insert", seq: this.#nextPosition(targets), prevSeq: null };
		}
		if (targets.every((t) => t.read < e)) {
			return null;
		}
		const p = row.prev_seq !== null && row.prev_seq > minCursor ? row.prev_seq : null;
		if (p !== null && targets.every((t) => t.cursor >= e || t.cursor < p)) {
			return { kind: "move", seq: this.#nextPosition(targets), fromSeq: e };
		}
		return { kind: "insert", seq: this.#nextPosition(targets), prevSeq: e };
	}

	/**
	 * The next position: above each stored position and above each cursor. The cleanup deletes the
	 * positions at or below the lowest cursor, so the highest stored position alone can be below a
	 * cursor, and a position there would never be read.
	 */
	#nextPosition(targets: readonly TargetPositions[]): number {
		const maxSeq = one(this.#storage.sql.exec<{ m: number | null }>(`SELECT MAX(seq) AS m FROM change_positions`)).m ?? 0;
		return Math.max(maxSeq, ...targets.map((t) => Math.max(t.cursor, t.read))) + 1;
	}

	// ─── targets ──────────────────────────────────────────────────────────────

	/** The stored target list, or undefined before the partition stored one. */
	storedTargets(): FokosTargetDefinition[] | undefined {
		return this.#storage.kv.get<FokosTargetDefinition[]>(TARGETS_KV_KEY);
	}

	/** Stores the target list and a state row for each target, in one transaction. */
	storeTargets(targets: readonly FokosTargetDefinition[]): void {
		this.#storage.transactionSync(() => {
			this.#storage.kv.put(TARGETS_KV_KEY, targets);
			for (const target of targets) {
				this.#storage.sql.exec(
					`INSERT INTO materialization_targets (target_id, kind) VALUES (?, ?) ON CONFLICT (target_id) DO NOTHING`,
					target.targetId,
					target.kind,
				);
			}
		});
	}

	/** The state row of each target. A table has a few targets, so this reads a few rows. */
	targetRows(): TargetRow[] {
		const rows: TargetRow[] = [];
		for (const row of this.#storage.sql.exec<{ target_id: string; cursor_seq: number; epoch: number; is_batch_open: number }>(
			`SELECT target_id, cursor_seq, epoch, is_batch_open FROM materialization_targets`,
		)) {
			rows.push(toTargetRow(row));
		}
		return rows;
	}

	targetRow(targetId: string): TargetRow {
		return toTargetRow(
			one(
				this.#storage.sql.exec<{ target_id: string; cursor_seq: number; epoch: number; is_batch_open: number }>(
					`SELECT target_id, cursor_seq, epoch, is_batch_open FROM materialization_targets WHERE target_id = ?`,
					targetId,
				),
				`fokos/mat: the target ${targetId} has no state row`,
			),
		);
	}

	/** Raises the epoch of a target by 1, and returns the new epoch. */
	raiseTargetEpoch(targetId: string): number {
		return one(
			this.#storage.sql.exec<{ epoch: number }>(
				`UPDATE materialization_targets SET epoch = epoch + 1 WHERE target_id = ? RETURNING epoch`,
				targetId,
			),
		).epoch;
	}

	/** Records that a batch of the target can be in flight. */
	openTargetBatch(targetId: string): void {
		this.#storage.sql.exec(`UPDATE materialization_targets SET is_batch_open = 1 WHERE target_id = ?`, targetId);
	}

	/** Writes the cursor and the batch marker of a target in one row write. */
	updateTarget(targetId: string, cursorSeq: number, isBatchOpen: boolean): void {
		this.#storage.sql.exec(
			`UPDATE materialization_targets SET cursor_seq = ?, is_batch_open = ? WHERE target_id = ?`,
			cursorSeq,
			isBatchOpen ? 1 : 0,
			targetId,
		);
	}

	/** Raises the epoch of each target by 1. A split source calls it in its cutover transaction. */
	raiseAllTargetEpochs(): void {
		this.#storage.sql.exec(`UPDATE materialization_targets SET epoch = epoch + 1`);
	}

	// ─── cursor snapshots ─────────────────────────────────────────────────────

	/** Stores the cursor of each target as the snapshot of the promotion of `hk`. */
	storeCursorSnapshot(hk: KeyBytes): void {
		const snapshots = this.#cursorSnapshots();
		snapshots.set(snapshotKey(hk), new Map(this.targetRows().map((row) => [row.targetId, row.cursorSeq])));
		this.#storage.kv.put(CURSOR_SNAPSHOTS_KV_KEY, snapshots);
	}

	/** The cursor of each target in the snapshot of the promotion of `hk`, or undefined when it has none. */
	cursorSnapshot(hk: KeyBytes): ReadonlyMap<string, number> | undefined {
		return this.#cursorSnapshots().get(snapshotKey(hk));
	}

	deleteCursorSnapshot(hk: KeyBytes): void {
		const snapshots = this.#cursorSnapshots();
		snapshots.delete(snapshotKey(hk));
		if (snapshots.size === 0) {
			this.#storage.kv.delete(CURSOR_SNAPSHOTS_KV_KEY);
		} else {
			this.#storage.kv.put(CURSOR_SNAPSHOTS_KV_KEY, snapshots);
		}
	}

	/** True while a promotion of this partition is between its cutover and its completion. */
	hasCursorSnapshots(): boolean {
		return this.#storage.kv.get(CURSOR_SNAPSHOTS_KV_KEY) !== undefined;
	}

	#cursorSnapshots(): CursorSnapshots {
		return this.#storage.kv.get<CursorSnapshots>(CURSOR_SNAPSHOTS_KV_KEY) ?? new Map();
	}

	// ─── ancestor fences ──────────────────────────────────────────────────────

	/** The ancestor fences that each call of a target carries. Empty in normal operation. */
	targetFences(targetId: string): Array<{ ancestorId: string; minEpoch: number }> {
		const fences: Array<{ ancestorId: string; minEpoch: number }> = [];
		for (const row of this.#storage.sql.exec<{ ancestor_id: string; min_epoch: number }>(
			`SELECT ancestor_id, min_epoch FROM ancestor_fences WHERE target_id = ?1`,
			targetId,
		)) {
			fences.push({ ancestorId: row.ancestor_id, minEpoch: row.min_epoch });
		}
		return fences;
	}

	/**
	 * Deletes the ancestor fences of a target when no inherited work of the target is left: its cursor
	 * and its cursor in each promotion snapshot are at or above `until_id`, and it has no inherited
	 * `retry` row. Each transaction that moves the cursor or deletes a `retry` row runs it. With no
	 * fence row, it reads the snapshot key and an empty range.
	 *
	 * A promotion child starts from the snapshot cursor and copies the fences of this partition when
	 * its state page is built. The cursor of this partition moves past the moved positions after the
	 * cutover, so the cursor alone can drop a fence that the child still needs.
	 */
	dropDoneTargetFences(targetId: string): void {
		let snapshotBound: number | null = null;
		for (const cursors of this.#cursorSnapshots().values()) {
			const cursor = cursors.get(targetId) ?? 0;
			snapshotBound = snapshotBound === null ? cursor : Math.min(snapshotBound, cursor);
		}
		this.#storage.sql.exec(
			`DELETE FROM ancestor_fences
			  WHERE target_id = ?1
			    AND until_id <= (SELECT cursor_seq FROM materialization_targets WHERE target_id = ?1)
			    AND (?2 IS NULL OR until_id <= ?2)
			    AND NOT EXISTS (SELECT 1 FROM target_retry WHERE target_id = ?1 AND is_inherited = 1 AND kind = 'retry')`,
			targetId,
			snapshotBound,
		);
	}

	// ─── the reads of a target step ───────────────────────────────────────────

	/** The due `retry` items of a target, the oldest backoff first. */
	dueRetryItems(targetId: string, nowMs: number, limit: number): TargetItemRef[] {
		const items: TargetItemRef[] = [];
		for (const row of this.#storage.sql.exec<{ item_id: number; est_row_bytes: number }>(
			`SELECT r.item_id, li.est_row_bytes
			 FROM target_retry r JOIN latest_items li ON li.item_id = r.item_id
			 WHERE r.target_id = ? AND r.kind = 'retry' AND r.not_before <= ?
			 ORDER BY r.not_before LIMIT ?`,
			targetId,
			nowMs,
			limit,
		)) {
			items.push({ itemId: row.item_id, estRowBytes: row.est_row_bytes });
		}
		return items;
	}

	/**
	 * The positions above `cursor` in `seq` order, with the item size and no item data. `inRetry`
	 * is true when the item has a `retry` row for the target. The rows are read one at a time.
	 */
	*positionsAfter(targetId: string, cursor: number, limit: number): Generator<TargetPosition> {
		const rows = this.#storage.sql.exec<{
			seq: number;
			item_id: number;
			est_row_bytes: number;
			in_retry: number;
		}>(
			`SELECT p.seq, p.item_id, li.est_row_bytes,
			   EXISTS (SELECT 1 FROM target_retry r WHERE r.target_id = ?2 AND r.item_id = p.item_id AND r.kind = 'retry') AS in_retry
			 FROM change_positions p JOIN latest_items li ON li.item_id = p.item_id
			 WHERE p.seq > ?1 ORDER BY p.seq LIMIT ?3`,
			cursor,
			targetId,
			limit,
		);
		for (const row of rows) {
			yield {
				seq: row.seq,
				itemId: row.item_id,
				estRowBytes: row.est_row_bytes,
				inRetry: row.in_retry === 1,
			};
		}
	}

	/** The keys and the latest image of an item, with json data as text. */
	targetImage(itemId: number): TargetImage | undefined {
		const row = tryOne(
			this.#storage.sql.exec<{
				base_hk: ArrayBuffer;
				base_sk: ArrayBuffer;
				revision: number;
				deleted: number;
				data_kind: number | null;
				ttl_at: number | null;
				data: string | ArrayBuffer | null;
			}>(
				`SELECT base_hk, base_sk, revision, deleted_at_ms IS NOT NULL AS deleted, data_kind, ttl_at,
				   CASE WHEN data_kind = ${JSON_KIND_CODE} THEN json(data) ELSE data END AS data
				 FROM latest_items WHERE item_id = ?`,
				itemId,
			),
		);
		if (!row) {
			return undefined;
		}
		const hk = new Uint8Array(row.base_hk) as KeyBytes;
		const sk = new Uint8Array(row.base_sk) as KeyBytes;
		if (row.deleted === 1 || row.data === null || row.data_kind === null) {
			return { hk, sk, revision: row.revision, deleted: true };
		}
		const data = typeof row.data === "string" ? row.data : new Uint8Array(row.data);
		return { hk, sk, revision: row.revision, deleted: false, kind: DATA_KINDS[row.data_kind], data, ttlAt: row.ttl_at };
	}

	/** The revision of an item, or undefined when it has no row. */
	itemRevision(itemId: number): number | undefined {
		return tryOne(this.#storage.sql.exec<{ revision: number }>(`SELECT revision FROM latest_items WHERE item_id = ?`, itemId))?.revision;
	}

	/** True when a position above `seq` exists. A seek on the primary key that reads at most 1 row. */
	hasPositionAfter(seq: number): boolean {
		return tryOne(this.#storage.sql.exec(`SELECT 1 FROM change_positions WHERE seq > ? LIMIT 1`, seq)) !== undefined;
	}

	/** The earliest backoff end of the `retry` rows of a target, or null when it has none. */
	earliestRetryAt(targetId: string): number | null {
		return one(
			this.#storage.sql.exec<{ m: number | null }>(
				`SELECT MIN(not_before) AS m FROM target_retry WHERE target_id = ? AND kind = 'retry'`,
				targetId,
			),
		).m;
	}

	// ─── the writes of a target step ──────────────────────────────────────────

	/** Deletes the `retry` or `violation` row of an item for a target. Returns true when a row existed. */
	deleteTargetRetry(targetId: string, itemId: number): boolean {
		return this.#storage.sql.exec(`DELETE FROM target_retry WHERE target_id = ? AND item_id = ?`, targetId, itemId).rowsWritten > 0;
	}

	/**
	 * Inserts or updates the `retry` row of an item for a target. The backoff starts at `minMs`, and
	 * doubles after each failure up to `maxMs`.
	 */
	recordTargetRetry(targetId: string, itemId: number, nowMs: number, minMs: number, maxMs: number, code: string | null): void {
		this.#storage.sql.exec(
			`INSERT INTO target_retry (target_id, item_id, kind, not_before, attempts, last_error) VALUES (?1, ?2, 'retry', ?3 + ?4, 1, ?6)
			 ON CONFLICT (target_id, item_id) DO UPDATE SET
			   kind = 'retry', attempts = attempts + 1, not_before = ?3 + MIN(?5, ?4 << MIN(attempts, 20)), last_error = excluded.last_error`,
			targetId,
			itemId,
			nowMs,
			minMs,
			maxMs,
			code,
		);
	}

	/** Moves the backoff end of the `retry` row of an item for a target to `notBefore`. */
	deferTargetRetry(targetId: string, itemId: number, notBefore: number): void {
		this.#storage.sql.exec(`UPDATE target_retry SET not_before = ?3 WHERE target_id = ?1 AND item_id = ?2`, targetId, itemId, notBefore);
	}

	/** Records a permanent failure of an item for a target. The row does not own the item. */
	recordTargetViolation(targetId: string, itemId: number, code: string): void {
		this.#storage.sql.exec(
			`INSERT INTO target_retry (target_id, item_id, kind, not_before, last_error) VALUES (?, ?, 'violation', 0, ?)
			 ON CONFLICT (target_id, item_id) DO UPDATE SET kind = 'violation', last_error = excluded.last_error`,
			targetId,
			itemId,
			code,
		);
	}

	// ─── GSI key sets ─────────────────────────────────────────────────────────

	/** The GSI keys of an item for a GSI target, in insert order. */
	indexKeys(targetId: string, itemId: number): GsiKey[] {
		const keys: GsiKey[] = [];
		for (const row of this.#storage.sql.exec<{ index_hk: ArrayBuffer; index_sk: ArrayBuffer }>(
			`SELECT index_hk, index_sk FROM target_index_keys WHERE item_id = ? AND target_id = ? ORDER BY key_id`,
			itemId,
			targetId,
		)) {
			keys.push({ hk: asKey(row.index_hk), sk: asKey(row.index_sk) });
		}
		return keys;
	}

	/** Adds a GSI key of an item for a GSI target. The caller checks that the item does not have the key. */
	addIndexKey(targetId: string, itemId: number, key: GsiKey): void {
		this.#storage.sql.exec(
			`INSERT INTO target_index_keys (item_id, target_id, index_hk, index_sk) VALUES (?, ?, ?, ?)`,
			itemId,
			targetId,
			key.hk,
			key.sk,
		);
	}

	/** Removes GSI keys of an item for a GSI target. Returns true when the item has no key left. */
	removeIndexKeys(targetId: string, itemId: number, keys: readonly GsiKey[]): boolean {
		for (const key of keys) {
			this.#storage.sql.exec(
				`DELETE FROM target_index_keys WHERE item_id = ? AND target_id = ? AND index_hk = ? AND index_sk = ?`,
				itemId,
				targetId,
				key.hk,
				key.sk,
			);
		}
		return !exists(this.#storage.sql.exec(`SELECT 1 FROM target_index_keys WHERE item_id = ? AND target_id = ? LIMIT 1`, itemId, targetId));
	}

	#hasIndexKeys(itemId: number): boolean {
		return exists(
			this.#storage.sql.exec(
				`SELECT 1 FROM target_index_keys WHERE item_id = ? AND target_id IN (SELECT target_id FROM materialization_targets) LIMIT 1`,
				itemId,
			),
		);
	}

	/**
	 * Evaluates a compiled projection against the latest image of one item. The subquery gives the
	 * columns of `latest_items` the names of the items table, so the compiled SQL reads the columns
	 * that it expects. Undefined when the item has no row.
	 */
	evaluateProjection(plan: CompiledProjectionPlan, hk: KeyBytes, sk: KeyBytes): ProjectedWireRow | undefined {
		const columns = plan.valueSql.flatMap((valueSql, index) => [`${valueSql} AS p${index}`, `${plan.typeSql[index]} AS t${index}`]);
		const row = tryOne(
			this.#storage.sql.exec<Record<string, SqlStorageValue>>(
				`SELECT ${columns.join(", ")}
				 FROM (SELECT base_hk AS hk, base_sk AS sk, revision AS v, ttl_at AS ttl_epoch_utc_seconds, data_kind, data
				       FROM latest_items WHERE base_hk = ?2 AND base_sk = ?3) AS i
				 LIMIT 1`,
				...materializedPlanBindings(plan, "pool"),
				hk,
				sk,
			),
		);
		return row === undefined ? undefined : decodeProjectedRow(row, plan.names.length);
	}

	// ─── cleanup ──────────────────────────────────────────────────────────────

	/** The lowest cursor of all targets, or null when the partition has no target. */
	minCursor(): number | null {
		return one(this.#storage.sql.exec<{ m: number | null }>(`SELECT MIN(cursor_seq) AS m FROM materialization_targets`)).m;
	}

	/**
	 * The position at or below which each target is done on each partition that can still need the
	 * rows: the lowest cursor, and each cursor of each promotion snapshot. A promotion child gets the
	 * cursors of its snapshot, so the positions above them stay until the promotion completes. Null
	 * when the partition has no target.
	 */
	cleanupBound(): number | null {
		let bound = this.minCursor();
		for (const cursors of this.#cursorSnapshots().values()) {
			for (const cursor of cursors.values()) {
				bound = bound === null ? cursor : Math.min(bound, cursor);
			}
		}
		return bound;
	}

	/** Deletes up to `limit` positions at or below `seq`, the lowest first. Returns the count. */
	deletePositionsUpTo(seq: number, limit: number): number {
		return this.#storage.sql.exec(
			`DELETE FROM change_positions WHERE seq IN (SELECT seq FROM change_positions WHERE seq <= ? ORDER BY seq LIMIT ?)`,
			seq,
			limit,
		).rowsWritten;
	}

	/** Up to `limit` tombstones after `after`, in the order of the garbage collection. */
	tombstonesAfter(after: TombstoneCursor | null, limit: number): Array<TombstoneCursor & { newestSeq: number | null }> {
		const tombstones: Array<TombstoneCursor & { newestSeq: number | null }> = [];
		for (const row of this.#storage.sql.exec<{ item_id: number; deleted_at_ms: number; newest_seq: number | null }>(
			`SELECT item_id, deleted_at_ms, newest_seq FROM latest_items
			 WHERE deleted_at_ms IS NOT NULL AND (deleted_at_ms, item_id) > (?, ?)
			 ORDER BY deleted_at_ms, item_id LIMIT ?`,
			after?.deletedAtMs ?? Number.MIN_SAFE_INTEGER,
			after?.itemId ?? Number.MIN_SAFE_INTEGER,
			limit,
		)) {
			tombstones.push({ itemId: row.item_id, deletedAtMs: row.deleted_at_ms, newestSeq: row.newest_seq });
		}
		return tombstones;
	}

	/**
	 * Deletes a tombstone and its positions, and subtracts its size from the estimate of its hash key.
	 * It deletes nothing while a target has a `retry` or `violation` row for the item. The caller
	 * checks that each target read the newest position of the item, so each position of the item is
	 * at or below each cursor. Returns true when it deleted the tombstone.
	 */
	deleteTombstone(itemId: number): boolean {
		const sql = this.#storage.sql;
		const hasRetry = tryOne(
			sql.exec(
				`SELECT 1 FROM target_retry WHERE target_id IN (SELECT target_id FROM materialization_targets) AND item_id = ? LIMIT 1`,
				itemId,
			),
		);
		if (hasRetry !== undefined || this.#hasIndexKeys(itemId)) {
			return false;
		}
		const row = tryOne(
			sql.exec<{ base_hk: ArrayBuffer; est_row_bytes: number; newest_seq: number | null }>(
				`DELETE FROM latest_items WHERE item_id = ? AND deleted_at_ms IS NOT NULL RETURNING base_hk, est_row_bytes, newest_seq`,
				itemId,
			),
		);
		if (row === undefined) {
			return false;
		}
		this.#deletePositions(row.newest_seq);
		this.keySizes.subtract(new Uint8Array(row.base_hk) as KeyBytes, row.est_row_bytes);
		return true;
	}

	/**
	 * Deletes up to `limit` items of the base hash key `hk`, with their positions and their
	 * `target_retry` rows. Returns true when no item of the key is left. The size estimate of the key
	 * is not changed: the caller deletes it.
	 */
	deleteItemsOfHashKey(hk: KeyBytes, limit: number): boolean {
		const sql = this.#storage.sql;
		// The rows are read before the deletes, because the loop deletes from the table that it reads.
		// `limit` bounds the array.
		const rows = sql
			.exec<{
				item_id: number;
				newest_seq: number | null;
			}>(`SELECT item_id, newest_seq FROM latest_items WHERE base_hk = ?1 LIMIT ?2`, hk, limit)
			.toArray();
		for (const row of rows) {
			this.#deletePositions(row.newest_seq);
			sql.exec(`DELETE FROM target_retry WHERE target_id IN (SELECT target_id FROM materialization_targets) AND item_id = ?1`, row.item_id);
			sql.exec(`DELETE FROM target_index_keys WHERE item_id = ?1`, row.item_id);
			sql.exec(`DELETE FROM latest_items WHERE item_id = ?1`, row.item_id);
		}
		return !exists(sql.exec(`SELECT 1 FROM latest_items WHERE base_hk = ?1 LIMIT 1`, hk));
	}

	/**
	 * Deletes the positions of an item from its newest position `seq`. The positions of an item link
	 * from the newest to the oldest. The cleanup can have deleted the older part of the chain already.
	 */
	#deletePositions(seq: number | null): void {
		while (seq !== null) {
			seq =
				tryOne(this.#storage.sql.exec<{ prev_seq: number | null }>(`DELETE FROM change_positions WHERE seq = ?1 RETURNING prev_seq`, seq))
					?.prev_seq ?? null;
		}
	}

	// ─── split decisions ──────────────────────────────────────────────────────

	/** The first base hash key, and whether another one follows it. Two seeks of the unique key index. */
	probeHashKeys(): { first: KeyBytes; more: boolean } | null {
		const sql = this.#storage.sql;
		const first = tryOne(sql.exec<{ base_hk: ArrayBuffer }>(`SELECT base_hk FROM latest_items ORDER BY base_hk LIMIT 1`));
		if (!first) {
			return null;
		}
		return { first: asKey(first.base_hk), more: exists(sql.exec(`SELECT 1 FROM latest_items WHERE base_hk > ?1 LIMIT 1`, first.base_hk)) };
	}

	/** The items of `hk` in `[start, end)` in sort-key order, each with its `est_row_bytes`. It reads one row at a time. */
	*sortKeySizes(hk: KeyBytes, start: KeyBytes | null, end: KeyBytes | null): Generator<{ sk: KeyBytes; bytes: number }> {
		const lower = start ?? KeyCodec.encodeOptional(undefined);
		const rows =
			end === null
				? this.#storage.sql.exec<{ base_sk: ArrayBuffer; est_row_bytes: number }>(
						`SELECT base_sk, est_row_bytes FROM latest_items WHERE base_hk = ?1 AND base_sk >= ?2 ORDER BY base_sk`,
						hk,
						lower,
					)
				: this.#storage.sql.exec<{ base_sk: ArrayBuffer; est_row_bytes: number }>(
						`SELECT base_sk, est_row_bytes FROM latest_items WHERE base_hk = ?1 AND base_sk >= ?2 AND base_sk < ?3 ORDER BY base_sk`,
						hk,
						lower,
						end,
					);
		for (const row of rows) {
			yield { sk: asKey(row.base_sk), bytes: row.est_row_bytes };
		}
	}

	// ─── migration ────────────────────────────────────────────────────────────

	/**
	 * Pages `latest_items` in `(base_hk, base_sk)` order, strictly after `cursor`, each item with its
	 * positions above `minSeq` and its `target_retry` rows. A position at or below `minSeq` is done for
	 * each target, so the target needs no copy. `range` limits the read to one hash key and one
	 * sort-key interval, with the cursor rules of `itemsPageStatement`. The generator reads one item
	 * each time the caller asks for the next one.
	 */
	*queryMigrationItems(cursor: ScanCursor | null, limit: number, range: KeyRange | null, minSeq: number): Generator<MigratedLatestItem> {
		const conds: string[] = [];
		const params: unknown[] = [];
		if (range === null) {
			if (cursor) {
				conds.push("(base_hk, base_sk) > (?, ?)");
				params.push(cursor.hk, cursor.sk);
			}
		} else {
			conds.push("base_hk = ?", cursor ? "base_sk > ?" : "base_sk >= ?");
			params.push(range.hk, cursor ? cursor.sk : range.start);
			if (range.end !== null) {
				conds.push("base_sk < ?");
				params.push(range.end);
			}
		}
		const where = conds.length > 0 ? `WHERE ${conds.join(" AND ")} ` : "";
		const rows = this.#storage.sql.exec<{
			item_id: number;
			base_hk: ArrayBuffer;
			base_sk: ArrayBuffer;
			revision: number;
			deleted_at_ms: number | null;
			newest_seq: number | null;
			data_kind: number | null;
			ttl_at: number | null;
			est_row_bytes: number;
			data: string | ArrayBuffer | null;
		}>(
			`SELECT item_id, base_hk, base_sk, revision, deleted_at_ms, newest_seq, data_kind, ttl_at, est_row_bytes, data
			 FROM latest_items ${where}ORDER BY base_hk, base_sk LIMIT ?`,
			...params,
			limit,
		);
		for (const { base_hk, base_sk, data, ...row } of rows) {
			yield {
				...row,
				hk: asKey(base_hk),
				sk: asKey(base_sk),
				data: data === null || typeof data === "string" ? data : new Uint8Array(data),
				positions: this.#positionsAbove(row.newest_seq, minSeq),
				retries: this.#retryRows(row.item_id),
				indexKeys: this.#indexKeyRows(row.item_id),
			};
		}
	}

	/** The hash-key walk of `latest_items` for a hash child, with the rules of `walkByHashKey`. */
	walkMigrationItems(
		cursor: HashKeyWalkCursor<ScanCursor> | null,
		limit: number,
		owns: (hk: KeyBytes, firstSk: KeyBytes) => boolean,
		minSeq: number,
	): Generator<HashKeyWalkEntry<MigratedLatestItem>> {
		const firstRowAfter = (after: KeyBytes | null) => {
			const row = tryOne(
				after === null
					? this.#storage.sql.exec<{ base_hk: ArrayBuffer; base_sk: ArrayBuffer }>(
							`SELECT base_hk, base_sk FROM latest_items ORDER BY base_hk, base_sk LIMIT 1`,
						)
					: this.#storage.sql.exec<{ base_hk: ArrayBuffer; base_sk: ArrayBuffer }>(
							`SELECT base_hk, base_sk FROM latest_items WHERE base_hk > ?1 ORDER BY base_hk, base_sk LIMIT 1`,
							after,
						),
			);
			return row && { hk: asKey(row.base_hk), sk: asKey(row.base_sk) };
		};
		return walkByHashKey(firstRowAfter, cursor, limit, owns, (after, n, range) => this.queryMigrationItems(after, n, range, minSeq));
	}

	/** The positions of an item from its newest position `seq`, while they are above `minSeq`. */
	#positionsAbove(seq: number | null, minSeq: number): Array<{ seq: number; prev_seq: number | null }> {
		const positions: Array<{ seq: number; prev_seq: number | null }> = [];
		while (seq !== null && seq > minSeq) {
			const row = tryOne(this.#storage.sql.exec<{ prev_seq: number | null }>(`SELECT prev_seq FROM change_positions WHERE seq = ?1`, seq));
			if (row === undefined) {
				break;
			}
			positions.push({ seq, prev_seq: row.prev_seq });
			seq = row.prev_seq;
		}
		return positions;
	}

	/** The `target_retry` rows of an item. One seek of the primary key for each target. */
	#retryRows(itemId: number): MigratedRetryRow[] {
		const rows: MigratedRetryRow[] = [];
		for (const row of this.#storage.sql.exec<MigratedRetryRow>(
			`SELECT target_id, kind, not_before, attempts, last_error FROM target_retry
			 WHERE target_id IN (SELECT target_id FROM materialization_targets) AND item_id = ?1`,
			itemId,
		)) {
			rows.push(row);
		}
		return rows;
	}

	/** The GSI keys of an item, in insert order. */
	#indexKeyRows(itemId: number): MigratedIndexKey[] {
		const rows: MigratedIndexKey[] = [];
		for (const row of this.#storage.sql.exec<{ target_id: string; index_hk: ArrayBuffer; index_sk: ArrayBuffer }>(
			`SELECT target_id, index_hk, index_sk FROM target_index_keys
			 WHERE item_id = ?1 AND target_id IN (SELECT target_id FROM materialization_targets) ORDER BY key_id`,
			itemId,
		)) {
			rows.push({ target_id: row.target_id, index_hk: asKey(row.index_hk), index_sk: asKey(row.index_sk) });
		}
		return rows;
	}

	/**
	 * Inserts a migrated item with its positions and its `target_retry` rows, which it marks as
	 * inherited. A target takes no write while it imports, so an item that is present came from an
	 * earlier try of the same page, and the insert keeps it. The conflict target stays `(base_hk,
	 * base_sk)`, for the reason of `PartitionStore.insertItemIfAbsent`. Returns true when it inserted
	 * the item.
	 */
	insertMigratedItem(item: MigratedLatestItem): boolean {
		const sql = this.#storage.sql;
		const inserted = tryOne(
			sql.exec(
				`INSERT INTO latest_items (item_id, base_hk, base_sk, revision, deleted_at_ms, newest_seq, data_kind, ttl_at, est_row_bytes, data)
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
				 ON CONFLICT (base_hk, base_sk) DO NOTHING
				 RETURNING item_id`,
				item.item_id,
				item.hk,
				item.sk,
				item.revision,
				item.deleted_at_ms,
				item.newest_seq,
				item.data_kind,
				item.ttl_at,
				item.est_row_bytes,
				item.data,
			),
		);
		if (inserted === undefined) {
			return false;
		}
		for (const position of item.positions) {
			sql.exec(`INSERT INTO change_positions (seq, item_id, prev_seq) VALUES (?1, ?2, ?3)`, position.seq, item.item_id, position.prev_seq);
		}
		for (const retry of item.retries) {
			sql.exec(
				`INSERT INTO target_retry (target_id, item_id, kind, not_before, attempts, is_inherited, last_error) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)`,
				retry.target_id,
				item.item_id,
				retry.kind,
				retry.not_before,
				retry.attempts,
				retry.last_error,
			);
		}
		// A page of an older version has no GSI keys.
		for (const key of item.indexKeys ?? []) {
			sql.exec(
				`INSERT INTO target_index_keys (item_id, target_id, index_hk, index_sk) VALUES (?1, ?2, ?3, ?4)`,
				item.item_id,
				key.target_id,
				key.index_hk,
				key.index_sk,
			);
		}
		return true;
	}

	/** Up to `limit` producer fences after `after`, in `producer_id` order. */
	producerFencesAfter(after: string | null, limit: number): Array<{ producerId: string; epoch: number }> {
		const fences: Array<{ producerId: string; epoch: number }> = [];
		for (const row of this.#storage.sql.exec<{ producer_id: string; epoch: number }>(
			`SELECT producer_id, epoch FROM producer_fences WHERE producer_id > ?1 ORDER BY producer_id LIMIT ?2`,
			after ?? "",
			limit,
		)) {
			fences.push({ producerId: row.producer_id, epoch: row.epoch });
		}
		return fences;
	}

	/** Each ancestor fence of each target. */
	allTargetFences(): MigratedFence[] {
		const fences: MigratedFence[] = [];
		for (const row of this.#storage.sql.exec<{ target_id: string; ancestor_id: string; min_epoch: number }>(
			`SELECT target_id, ancestor_id, min_epoch FROM ancestor_fences`,
		)) {
			fences.push({ targetId: row.target_id, ancestorId: row.ancestor_id, minEpoch: row.min_epoch });
		}
		return fences;
	}

	/**
	 * Stores the target state of a split child at the end of its migration: the target list, a state
	 * row for each target with the cursor of the source and the epoch 1, and the ancestor fences.
	 *
	 * The inherited work of a target is each copied position above its cursor and each copied `retry`
	 * row of the target. A target with inherited work gets the fences of the source and the source
	 * itself at its epoch in `epochs`, with `until_id` at the highest copied position. A target with no
	 * inherited work gets no fence. The child takes no write while it imports, so the copied positions
	 * are all positions of the child.
	 */
	importMigrationState(state: MigratedState): void {
		const sql = this.#storage.sql;
		if (state.targetList === undefined) {
			return;
		}
		this.#storage.kv.put(TARGETS_KV_KEY, state.targetList);
		const maxSeq = one(sql.exec<{ m: number | null }>(`SELECT MAX(seq) AS m FROM change_positions`)).m;
		for (const target of state.targetList) {
			const cursor = state.cursors.get(target.targetId) ?? 0;
			sql.exec(
				`INSERT INTO materialization_targets (target_id, kind, cursor_seq) VALUES (?1, ?2, ?3) ON CONFLICT (target_id) DO NOTHING`,
				target.targetId,
				target.kind,
				cursor,
			);
			const inherited =
				(maxSeq !== null && maxSeq > cursor) ||
				exists(
					sql.exec(`SELECT 1 FROM target_retry WHERE target_id = ?1 AND is_inherited = 1 AND kind = 'retry' LIMIT 1`, target.targetId),
				);
			if (!inherited) {
				continue;
			}
			const untilId = Math.max(maxSeq ?? 0, cursor);
			const fences = state.fences.filter((fence) => fence.targetId === target.targetId);
			for (const fence of [...fences, { ancestorId: state.sourceId, minEpoch: state.epochs.get(target.targetId) ?? 1 }]) {
				sql.exec(
					`INSERT INTO ancestor_fences (target_id, ancestor_id, min_epoch, until_id) VALUES (?1, ?2, ?3, ?4)
					 ON CONFLICT (target_id, ancestor_id) DO UPDATE SET min_epoch = MAX(min_epoch, excluded.min_epoch), until_id = excluded.until_id`,
					target.targetId,
					fence.ancestorId,
					fence.minEpoch,
					untilId,
				);
			}
		}
	}
}

/** The stored image of a base item. `newestSeq` is its newest change position, or null. */
export type StoredImage = { itemId: number; revision: number; estRowBytes: number; newestSeq: number | null };

/**
 * The positions of one target that the ingest reads: its cursor, and the highest position that its
 * batch in flight read. `read` is the cursor while no batch is in flight.
 */
export type TargetPositions = { cursor: number; read: number };

export type TargetRow = { targetId: string; cursorSeq: number; epoch: number; isBatchOpen: boolean };

/** An item that a target step can send: its ID and its size, and no data. */
export type TargetItemRef = { itemId: number; estRowBytes: number };

export type TargetPosition = TargetItemRef & { seq: number; inRetry: boolean };

export type TargetImage = { hk: KeyBytes; sk: KeyBytes; revision: number } & (
	| { deleted: true }
	| { deleted: false; kind: DataKind; data: string | Uint8Array; ttlAt: number | null }
);

export type TombstoneCursor = { deletedAtMs: number; itemId: number };

/** One `target_retry` row as a migration page carries it. The target marks it as inherited. */
export type MigratedRetryRow = { target_id: string; kind: string; not_before: number; attempts: number; last_error: string | null };

/**
 * One `latest_items` row as a migration page carries it, with its stored data (a json item as its
 * JSONB blob), its positions from the newest, and its `target_retry` rows. `item_id` and each `seq`
 * stay unchanged.
 */
export type MigratedLatestItem = {
	item_id: number;
	hk: KeyBytes;
	sk: KeyBytes;
	revision: number;
	deleted_at_ms: number | null;
	newest_seq: number | null;
	data_kind: number | null;
	ttl_at: number | null;
	est_row_bytes: number;
	data: string | Uint8Array | null;
	positions: Array<{ seq: number; prev_seq: number | null }>;
	retries: MigratedRetryRow[];
	indexKeys?: MigratedIndexKey[];
};

/** One `target_index_keys` row as a migration page carries it. */
export type MigratedIndexKey = { target_id: string; index_hk: KeyBytes; index_sk: KeyBytes };

/** One GSI key: the GSI hash key and the GSI sort key (empty when the GSI has no sort key). */
export type GsiKey = { hk: KeyBytes; sk: KeyBytes };

export type MigratedFence = { targetId: string; ancestorId: string; minEpoch: number };

/** The target state that the last migration page gives a split child. */
export type MigratedState = {
	/** The `DurableObjectId` of the source, in hex. */
	sourceId: string;
	/** The stored target list of the source, or undefined when it has none. */
	targetList: FokosTargetDefinition[] | undefined;
	/** The cursor of each target that the child starts from, by `targetId`. */
	cursors: ReadonlyMap<string, number>;
	/** The epoch of each target of the source after its cutover raise, by `targetId`. */
	epochs: ReadonlyMap<string, number>;
	/** The ancestor fences of the source. */
	fences: MigratedFence[];
};

/** The KV field name of the snapshot of a promoted hash key. */
function snapshotKey(hk: KeyBytes): string {
	return hk.toBase64({ alphabet: "base64url" });
}

function asKey(value: ArrayBuffer): KeyBytes {
	return KeyCodec.asKeyBytes(new Uint8Array(value));
}

function toTargetRow(row: { target_id: string; cursor_seq: number; epoch: number; is_batch_open: number }): TargetRow {
	return { targetId: row.target_id, cursorSeq: row.cursor_seq, epoch: row.epoch, isBatchOpen: row.is_batch_open === 1 };
}
