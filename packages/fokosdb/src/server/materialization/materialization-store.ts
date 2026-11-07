/**
 * The SQLite storage of `FokosDBMaterializationDO`: the latest image of each base item that the partition
 * accepted, the highest epoch of each producer, the change positions that the targets read, the state
 * of each target, and the failed items of each target. The item facts of each routingHash belong to
 * `itemStats`, which this store updates in the transaction of each image write.
 */
import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import { DATA_KINDS, type DataKind } from "../../shared/types.js";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "../../sharding/key-codec.js";
import { asRoutingHash, type RoutingHash } from "../../sharding/hash-primitives.js";
import { exists, one, tryOne } from "../../shared/sql-cursor.js";
import { itemBytesExpr, JSON_KIND_CODE } from "../../shared/partition/item-bytes.js";
import { itemFacts, LATEST_ITEMS_MODEL, type ItemFacts } from "../../shared/partition/item-size.js";
import { BUCKET_ITEMS, ItemStats } from "../../shared/partition/item-stats.js";
import { intervalTerms, type KeyRange, type StorageCursor } from "../../shared/partition/partition-store.js";
import type { FokosTargetDefinition } from "./targets.js";
import { materializedPlanBindings } from "../../shared/expression/bindings.js";
import type { CompiledProjectionPlan } from "../../shared/expression/plan.js";
import { decodeProjectedRow, type ProjectedWireRow } from "../../shared/expression/projection.js";

/** The KV key of the target list. The first valid list is stored, and the partition uses only it. */
const TARGETS_KV_KEY = "__fokosdb/mat/targets";

/**
 * The KV key of the cursor snapshots of the promotions of this partition that are between their
 * cutover and their completion: the cursor of each target at the cutover, by the promoted
 * routingHash in hex.
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

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create the latest images, the producer fences, the change positions, the target state, and the ancestor fences",
		// latest_items:
		// - One row for each base item: its newest live image, or a tombstone (`deleted_at_ms` is not
		//   NULL, `data` is NULL). `revision` is the base revision of the image.
		// - A WITHOUT ROWID table with `data` last, as the items table of PartitionStore: the rows are in
		//   primary-key order, and a read of the metadata columns reads no overflow page.
		// - `routing_hash` is the routingHash of `base_hk` in the shard group of this partition,
		//   which the runtime gives (`routingHashForKey`). The store treats it as opaque bytes. It is not an
		//   item identity: `base_hk` and `base_sk` keep the items apart. The primary key leads with it, so
		//   the rows of one hash child or of one promoted routingHash are one key range of the table.
		// - `item_id` links the rows of other tables to the item, so `(base_hk, base_sk)` is stored one
		//   time. `UNIQUE (item_id)` gives the linked reads a seek. The counter in `item_id_counter`
		//   assigns it (`#nextItemId`), and the mark only increases, so a deleted id never comes back.
		//   Each statement that deletes a row must still delete its linked rows in the same transaction.
		//   Migration copies `item_id` without change: a copied item whose id another item holds fails
		//   the insert, and the child raises its mark above each copied id.
		// - No rule reads the value of `deleted_at_ms`. It orders the garbage collection of tombstones.
		//   The tombstone index declares `item_id`, because a WITHOUT ROWID index holds only the primary
		//   key after its own columns, and the collection seeks on `(deleted_at_ms, item_id)`.
		// - `item_bytes` is the exact size of the item, from `itemBytesExpr`: the stored data plus the key
		//   bytes. A tombstone has no data, so its `item_bytes` is its key bytes. The batch budget of a
		//   target uses it. The storage estimate of promotion and range splits prices the facts of
		//   `item_stats`, which `itemStats` keeps from this value. It is before `data`, so a size read
		//   stops before `data`. The table needs no other index for the size reads: it is in storage order.
		//
		// item_id_counter:
		// - One row (id = 1). Every `item_id` below `hwm` is reserved.
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
                routing_hash BLOB    NOT NULL,
                base_hk            BLOB    NOT NULL,
                base_sk            BLOB    NOT NULL DEFAULT x'',
                item_id            INTEGER NOT NULL,
                revision           INTEGER NOT NULL,
                deleted_at_ms      INTEGER,
                newest_seq         INTEGER,
                data_kind          INTEGER,
                ttl_at             INTEGER,
                item_bytes         INTEGER NOT NULL,
                data               ANY,

                PRIMARY KEY (routing_hash, base_hk, base_sk),
                UNIQUE (item_id)
            ) WITHOUT ROWID, STRICT;

            CREATE INDEX IF NOT EXISTS idx_latest_items_deleted_gc ON latest_items (deleted_at_ms, item_id) WHERE deleted_at_ms IS NOT NULL;

            CREATE TABLE IF NOT EXISTS item_id_counter (
                id  INTEGER PRIMARY KEY CHECK (id = 1),
                hwm INTEGER NOT NULL
            ) STRICT;
            INSERT OR IGNORE INTO item_id_counter (id, hwm) VALUES (1, 1);

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

/** The count of item ids that one reservation of `item_id_counter` takes. */
const ITEM_ID_BLOCK = 1024;

export class MaterializationStore {
	/** The item facts of each routingHash. */
	readonly itemStats: ItemStats;
	#storage: DurableObjectStorage;
	#migrations: SQLSchemaMigrations;
	/**
	 * The reserved block of item ids `[#nextId, #idLimit)`. Both are 0 until the first reservation, and
	 * an ingest sets both to 0 again. A rollback restores the values from before its transaction
	 * (`transactionSync`), so memory never uses a block that storage did not commit.
	 */
	#nextId = 0;
	#idLimit = 0;

	constructor(
		storage: DurableObjectStorage,
		opts: {
			promotionFloor: () => number | null;
			rangeLeaf: () => boolean;
			logParams: () => Record<string, unknown>;
			/** The bucket size `K` of a range leaf. Default: `BUCKET_ITEMS`. */
			bucketItems?: number;
		} = {
			promotionFloor: () => null,
			rangeLeaf: () => false,
			logParams: () => ({}),
		},
	) {
		this.#storage = storage;
		this.itemStats = new ItemStats(storage, {
			table: "latest_items",
			hkColumn: "base_hk",
			skColumn: "base_sk",
			indexedCondition: "deleted_at_ms IS NOT NULL",
			model: LATEST_ITEMS_MODEL,
			promotionFloor: opts.promotionFloor,
			rangeLeaf: opts.rangeLeaf,
			bucketItems: opts.bucketItems ?? BUCKET_ITEMS,
			logName: "fokos/materialization",
			logParams: opts.logParams,
		});
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: storage,
			keyNameTrackingLastMigrationID: "__fokosdb/mat/sql_schema_version",
		});
	}

	runMigrations(): void {
		this.#migrations.runAllSync();
		this.itemStats.runMigrations();
	}

	get databaseSize(): number {
		return this.#storage.sql.databaseSize;
	}

	/**
	 * A write that creates an item can reserve a block of item ids inside `fn`. When `fn` throws, the
	 * storage transaction rolls the reservation back, so this restores the block from before `fn`.
	 * Every write that can create an item must run in this method or outside any storage transaction,
	 * never inside another `transactionSync` of the same storage.
	 */
	transactionSync<T>(fn: () => T): T {
		const nextId = this.#nextId;
		const idLimit = this.#idLimit;
		try {
			return this.#storage.transactionSync(fn);
		} catch (error) {
			this.#nextId = nextId;
			this.#idLimit = idLimit;
			throw error;
		}
	}

	/**
	 * The `item_id` of a new row. It reserves a block of `ITEM_ID_BLOCK` ids when the block in memory is
	 * used up: one row written for each block. See `transactionSync` for the rollback rule.
	 */
	#nextItemId(): number {
		if (this.#nextId === this.#idLimit) {
			const { hwm } = one(
				this.#storage.sql.exec<{ hwm: number }>(`UPDATE item_id_counter SET hwm = hwm + ?1 WHERE id = 1 RETURNING hwm`, ITEM_ID_BLOCK),
			);
			this.#nextId = hwm - ITEM_ID_BLOCK;
			this.#idLimit = hwm;
		}
		return this.#nextId++;
	}

	/**
	 * Raises the high-water mark of the item ids above `maxItemId`, after a migration ingest copied rows
	 * with their ids. Memory drops its block, so the next new row reserves above every copied id. The
	 * drop is safe also when the ingest rolls back: the next reservation reads the mark again.
	 */
	raiseItemIdMark(maxItemId: number): void {
		this.#storage.sql.exec(`UPDATE item_id_counter SET hwm = ?1 WHERE id = 1 AND hwm < ?1`, maxItemId + 1);
		this.#nextId = 0;
		this.#idLimit = 0;
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
	storedImage(routingHash: RoutingHash, hk: KeyBytes, sk: KeyBytes): StoredImage | undefined {
		const row = tryOne(
			this.#storage.sql.exec<{ item_id: number; revision: number; item_bytes: number; deleted: number; newest_seq: number | null }>(
				`SELECT item_id, revision, item_bytes, deleted_at_ms IS NOT NULL AS deleted, newest_seq
				   FROM latest_items WHERE routing_hash = ? AND base_hk = ? AND base_sk = ?`,
				routingHash,
				hk,
				sk,
			),
		);
		return (
			row && {
				itemId: row.item_id,
				revision: row.revision,
				facts: this.#facts(hk, sk, row.item_bytes, row.deleted === 1),
				newestSeq: row.newest_seq,
			}
		);
	}

	/** The facts of one item of `latest_items`. A tombstone is an indexed item. */
	#facts(hk: KeyBytes, sk: KeyBytes, itemBytes: number, tombstone: boolean): ItemFacts {
		return itemFacts(this.itemStats.model, { itemBytes, keyBytes: hk.byteLength + sk.byteLength, indexed: tombstone });
	}

	/**
	 * Stores an image that is newer than the stored one (`current`, or undefined when there is none).
	 * A live image replaces the data. A delete keeps the row as a tombstone with no data, so the space
	 * of the payload is free at once, and the tombstone holds the revision that a late export must not
	 * go below. The facts of the routingHash change by the difference of the facts of the two rows.
	 *
	 * When targets exist, the image gets a change position (see `#planPosition`), and the `retry` rows
	 * of the item become due, because the cursor skips the item while it has a `retry` row.
	 *
	 * Returns the rounded estimate of the routingHash after the write, or undefined when it has no bucket.
	 */
	putLatestImage(
		routingHash: RoutingHash,
		image: LatestImage,
		nowMs: number,
		current: StoredImage | undefined,
		targets: readonly TargetPositions[],
	): number | undefined {
		const sql = this.#storage.sql;
		const position = this.#planPosition(current?.newestSeq ?? null, targets);
		const newestSeq = position === null ? (current?.newestSeq ?? null) : position.seq;
		// An overwrite keeps its id, and the insert branch does not run for it, so it binds -1, which no
		// row holds.
		const newItemId = current === undefined ? this.#nextItemId() : -1;
		const row = image.deleted
			? sql.exec<{ item_id: number; item_bytes: number }>(
					`INSERT INTO latest_items (routing_hash, base_hk, base_sk, item_id, revision, deleted_at_ms, newest_seq, data_kind, ttl_at, item_bytes, data)
					 VALUES (?6, ?1, ?2, ?7, ?3, ?4, ?5, NULL, NULL, ${itemBytesExpr(null, "?1", "?2")}, NULL)
					 ON CONFLICT (routing_hash, base_hk, base_sk) DO UPDATE SET
					   revision = excluded.revision, deleted_at_ms = excluded.deleted_at_ms, newest_seq = excluded.newest_seq,
					   data_kind = NULL, ttl_at = NULL, item_bytes = excluded.item_bytes, data = NULL
					 RETURNING item_id, item_bytes`,
					image.hashKey,
					image.sortKey,
					image.revision,
					nowMs,
					newestSeq,
					routingHash,
					newItemId,
				)
			: sql.exec<{ item_id: number; item_bytes: number }>(
					`INSERT INTO latest_items (routing_hash, base_hk, base_sk, item_id, revision, deleted_at_ms, newest_seq, data_kind, ttl_at, item_bytes, data)
					 VALUES (?8, ?1, ?2, ?9, ?3, NULL, ?7, ?4, ?5, ${itemBytesExpr("?6", "?1", "?2")}, ?6)
					 ON CONFLICT (routing_hash, base_hk, base_sk) DO UPDATE SET
					   revision = excluded.revision, deleted_at_ms = NULL, newest_seq = excluded.newest_seq,
					   data_kind = excluded.data_kind, ttl_at = excluded.ttl_at, item_bytes = excluded.item_bytes, data = excluded.data
					 RETURNING item_id, item_bytes`,
					image.hashKey,
					image.sortKey,
					image.revision,
					DATA_KINDS.indexOf(image.kind),
					image.ttlAt,
					image.data,
					newestSeq,
					routingHash,
					newItemId,
				);
		const { item_id: itemId, item_bytes: itemBytes } = row.one();
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
		const newFacts = this.#facts(image.hashKey, image.sortKey, itemBytes, image.deleted);
		return this.itemStats.change(routingHash, image.hashKey, image.sortKey, current?.facts ?? null, newFacts) ?? undefined;
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

	/** Stores the cursor of each target as the snapshot of the promotion of `routingHash`. */
	storeCursorSnapshot(routingHash: RoutingHash): void {
		const snapshots = this.#cursorSnapshots();
		snapshots.set(routingHash.toHex(), new Map(this.targetRows().map((row) => [row.targetId, row.cursorSeq])));
		this.#storage.kv.put(CURSOR_SNAPSHOTS_KV_KEY, snapshots);
	}

	/** The cursor of each target in the snapshot of the promotion of `routingHash`, or undefined when it has none. */
	cursorSnapshot(routingHash: RoutingHash): ReadonlyMap<string, number> | undefined {
		return this.#cursorSnapshots().get(routingHash.toHex());
	}

	deleteCursorSnapshot(routingHash: RoutingHash): void {
		const snapshots = this.#cursorSnapshots();
		snapshots.delete(routingHash.toHex());
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
		for (const row of this.#storage.sql.exec<{ item_id: number; item_bytes: number }>(
			`SELECT r.item_id, li.item_bytes
			 FROM target_retry r JOIN latest_items li ON li.item_id = r.item_id
			 WHERE r.target_id = ? AND r.kind = 'retry' AND r.not_before <= ?
			 ORDER BY r.not_before LIMIT ?`,
			targetId,
			nowMs,
			limit,
		)) {
			items.push({ itemId: row.item_id, itemBytes: row.item_bytes });
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
			item_bytes: number;
			in_retry: number;
		}>(
			`SELECT p.seq, p.item_id, li.item_bytes,
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
				itemBytes: row.item_bytes,
				inRetry: row.in_retry === 1,
			};
		}
	}

	/** The keys and the latest image of an item, with json data as text. */
	targetImage(itemId: number): TargetImage | undefined {
		const row = tryOne(
			this.#storage.sql.exec<{
				routing_hash: ArrayBuffer;
				base_hk: ArrayBuffer;
				base_sk: ArrayBuffer;
				revision: number;
				deleted: number;
				data_kind: number | null;
				ttl_at: number | null;
				data: string | ArrayBuffer | null;
			}>(
				`SELECT routing_hash, base_hk, base_sk, revision, deleted_at_ms IS NOT NULL AS deleted, data_kind, ttl_at,
				   CASE WHEN data_kind = ${JSON_KIND_CODE} THEN json(data) ELSE data END AS data
				 FROM latest_items WHERE item_id = ?`,
				itemId,
			),
		);
		if (!row) {
			return undefined;
		}
		const routingHash = asRoutingHash(new Uint8Array(row.routing_hash));
		const hk = new Uint8Array(row.base_hk) as KeyBytes;
		const sk = new Uint8Array(row.base_sk) as KeyBytes;
		if (row.deleted === 1 || row.data === null || row.data_kind === null) {
			return { routingHash, hk, sk, revision: row.revision, deleted: true };
		}
		const data = typeof row.data === "string" ? row.data : new Uint8Array(row.data);
		return { routingHash, hk, sk, revision: row.revision, deleted: false, kind: DATA_KINDS[row.data_kind], data, ttlAt: row.ttl_at };
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
	evaluateProjection(plan: CompiledProjectionPlan, routingHash: RoutingHash, hk: KeyBytes, sk: KeyBytes): ProjectedWireRow | undefined {
		const columns = plan.valueSql.flatMap((valueSql, index) => [`${valueSql} AS p${index}`, `${plan.typeSql[index]} AS t${index}`]);
		const row = tryOne(
			this.#storage.sql.exec<Record<string, SqlStorageValue>>(
				`SELECT ${columns.join(", ")}
				 FROM (SELECT base_hk AS hk, base_sk AS sk, revision AS v, ttl_at AS ttl_epoch_utc_seconds, data_kind, data
				       FROM latest_items WHERE routing_hash = ?4 AND base_hk = ?2 AND base_sk = ?3) AS i
				 LIMIT 1`,
				...materializedPlanBindings(plan, "pool"),
				hk,
				sk,
				routingHash,
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
	 * Deletes a tombstone and its positions, and subtracts its facts from the bucket of its routingHash.
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
			sql.exec<{ routing_hash: ArrayBuffer; base_hk: ArrayBuffer; base_sk: ArrayBuffer; item_bytes: number; newest_seq: number | null }>(
				`DELETE FROM latest_items WHERE item_id = ? AND deleted_at_ms IS NOT NULL
				 RETURNING routing_hash, base_hk, base_sk, item_bytes, newest_seq`,
				itemId,
			),
		);
		if (row === undefined) {
			return false;
		}
		this.#deletePositions(row.newest_seq);
		const [hk, sk] = [asKey(row.base_hk), asKey(row.base_sk)];
		this.itemStats.change(asRoutingHash(new Uint8Array(row.routing_hash)), hk, sk, this.#facts(hk, sk, row.item_bytes, true), null);
		return true;
	}

	/**
	 * Deletes the first `limit` items of `routingHash` in primary-key order, with their
	 * positions, their `target_retry` rows, and their GSI keys. The items are one key range, so one
	 * statement deletes them. Returns true when no item of the routingHash is left. The size estimate of the
	 * routingHash is not changed: the caller deletes it.
	 */
	deleteItemsOfRoutingHash(routingHash: RoutingHash, limit: number): boolean {
		const sql = this.#storage.sql;
		// The rows are read before the deletes, because the loop deletes from the table that it reads.
		// `limit` bounds the array.
		const rows = sql
			.exec<{
				base_hk: ArrayBuffer;
				base_sk: ArrayBuffer;
				item_id: number;
				newest_seq: number | null;
			}>(
				`SELECT base_hk, base_sk, item_id, newest_seq FROM latest_items WHERE routing_hash = ?1 ORDER BY base_hk, base_sk LIMIT ?2`,
				routingHash,
				limit,
			)
			.toArray();
		if (rows.length === 0) {
			return true;
		}
		for (const row of rows) {
			this.#deletePositions(row.newest_seq);
			sql.exec(`DELETE FROM target_retry WHERE target_id IN (SELECT target_id FROM materialization_targets) AND item_id = ?1`, row.item_id);
			sql.exec(`DELETE FROM target_index_keys WHERE item_id = ?1`, row.item_id);
		}
		const last = rows[rows.length - 1];
		sql.exec(
			`DELETE FROM latest_items WHERE routing_hash = ?1 AND (base_hk, base_sk) <= (?2, ?3)`,
			routingHash,
			last.base_hk,
			last.base_sk,
		);
		return rows.length < limit;
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

	/**
	 * One base hash key of the first routingHash, and whether another routingHash follows it. Two seeks
	 * of the primary key.
	 */
	probeRoutingHashes(): { first: KeyBytes; more: boolean } | null {
		const sql = this.#storage.sql;
		const first = tryOne(
			sql.exec<{ routing_hash: ArrayBuffer; base_hk: ArrayBuffer }>(
				`SELECT routing_hash, base_hk FROM latest_items ORDER BY routing_hash, base_hk LIMIT 1`,
			),
		);
		if (!first) {
			return null;
		}
		return {
			first: asKey(first.base_hk),
			more: exists(sql.exec(`SELECT 1 FROM latest_items WHERE routing_hash > ?1 LIMIT 1`, first.routing_hash)),
		};
	}

	/**
	 * The items of `routingHash` in the composite interval `[start, end)` of `(base_hk, base_sk)`, in that
	 * order (or in reverse order for `desc`), each with its size facts. A null edge is unbounded. It reads one row at a time, and stops
	 * before `data` in each row.
	 */
	*compositeKeyFacts(
		routingHash: RoutingHash,
		start: FokosCompositeKey | null,
		end: FokosCompositeKey | null,
		order: "asc" | "desc" = "asc",
	): Generator<{ hk: KeyBytes; sk: KeyBytes; itemBytes: number; indexed: boolean }> {
		const { conds, params } = latestSliceTerms(routingHash, start, end);
		for (const row of this.#storage.sql.exec<{ base_hk: ArrayBuffer; base_sk: ArrayBuffer; item_bytes: number; indexed: number }>(
			`SELECT base_hk, base_sk, item_bytes, deleted_at_ms IS NOT NULL AS indexed FROM latest_items WHERE ${conds} ORDER BY base_hk ${order}, base_sk ${order}`,
			...params,
		)) {
			yield { hk: asKey(row.base_hk), sk: asKey(row.base_sk), itemBytes: row.item_bytes, indexed: row.indexed === 1 };
		}
	}

	/** The count of the items of `routingHash` in `[start, end)`, at most `limit`. It reads at most `limit` rows of the primary key. */
	countItems(routingHash: RoutingHash, start: FokosCompositeKey | null, end: FokosCompositeKey | null, limit: number): number {
		const { conds, params } = latestSliceTerms(routingHash, start, end);
		return one(
			this.#storage.sql.exec<{ n: number }>(
				`SELECT COUNT(*) AS n FROM (SELECT 1 FROM latest_items WHERE ${conds} LIMIT ?)`,
				...params,
				limit,
			),
		).n;
	}

	// ─── migration ────────────────────────────────────────────────────────────

	/**
	 * Pages `latest_items` over `interval` in storage order, strictly after `cursor`, each item with its
	 * positions above `minSeq` and its `target_retry` rows. A position at or below `minSeq` is done for
	 * each target, so the target needs no copy. The cursor and interval rules of `itemsPageStatement`
	 * apply. The generator reads one item each time the caller asks for the next one.
	 */
	*queryMigrationItems(cursor: StorageCursor | null, limit: number, interval: KeyRange, minSeq: number): Generator<MigratedLatestItem> {
		const { conds, params } = intervalTerms(
			["routing_hash", "base_hk", "base_sk"],
			interval,
			cursor && [cursor.routingHash, cursor.hk, cursor.sk],
		);
		const rows = this.#storage.sql.exec<{
			item_id: number;
			routing_hash: ArrayBuffer;
			base_hk: ArrayBuffer;
			base_sk: ArrayBuffer;
			revision: number;
			deleted_at_ms: number | null;
			newest_seq: number | null;
			data_kind: number | null;
			ttl_at: number | null;
			item_bytes: number;
			data: string | ArrayBuffer | null;
		}>(
			`SELECT item_id, routing_hash, base_hk, base_sk, revision, deleted_at_ms, newest_seq, data_kind, ttl_at, item_bytes, data
			 FROM latest_items WHERE ${conds.join(" AND ")} ORDER BY routing_hash, base_hk, base_sk LIMIT ?`,
			...params,
			limit,
		);
		for (const { routing_hash, base_hk, base_sk, data, ...row } of rows) {
			yield {
				...row,
				routingHash: asRoutingHash(new Uint8Array(routing_hash)),
				hk: asKey(base_hk),
				sk: asKey(base_sk),
				data: data === null || typeof data === "string" ? data : new Uint8Array(data),
				positions: this.#positionsAbove(row.newest_seq, minSeq),
				retries: this.#retryRows(row.item_id),
				indexKeys: this.#indexKeyRows(row.item_id),
			};
		}
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
	 * earlier try of the same page, and the insert keeps it. The conflict target stays
	 * `(routing_hash, base_hk, base_sk)`, so an `item_id` that another item holds fails the
	 * statement, as in `PartitionStore.insertItemIfAbsent`. The caller raises the item-id mark after the
	 * page (`raiseItemIdMark`). Returns the facts of the item when it inserted it, so the caller adds
	 * them to `item_stats` in the same transaction, or undefined when the item was present.
	 */
	insertMigratedItem(item: MigratedLatestItem): ItemFacts | undefined {
		const sql = this.#storage.sql;
		// A tombstone has no data.
		const itemBytes = `CASE WHEN ?9 IS NULL THEN ${itemBytesExpr(null, "?2", "?3")} ELSE ${itemBytesExpr("?9", "?2", "?3")} END`;
		const inserted = tryOne(
			sql.exec<{ item_bytes: number }>(
				`INSERT INTO latest_items (item_id, routing_hash, base_hk, base_sk, revision, deleted_at_ms, newest_seq, data_kind, ttl_at, item_bytes, data)
				 VALUES (?1, ?10, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ${itemBytes}, ?9)
				 ON CONFLICT (routing_hash, base_hk, base_sk) DO NOTHING
				 RETURNING item_bytes`,
				item.item_id,
				item.hk,
				item.sk,
				item.revision,
				item.deleted_at_ms,
				item.newest_seq,
				item.data_kind,
				item.ttl_at,
				item.data,
				item.routingHash,
			),
		);
		if (inserted === undefined) {
			return undefined;
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
		return this.#facts(item.hk, item.sk, inserted.item_bytes, item.deleted_at_ms !== null);
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
export type StoredImage = { itemId: number; revision: number; facts: ItemFacts; newestSeq: number | null };

/**
 * The positions of one target that the ingest reads: its cursor, and the highest position that its
 * batch in flight read. `read` is the cursor while no batch is in flight.
 */
export type TargetPositions = { cursor: number; read: number };

export type TargetRow = { targetId: string; cursorSeq: number; epoch: number; isBatchOpen: boolean };

/** An item that a target step can send: its ID and its size, and no data. */
export type TargetItemRef = { itemId: number; itemBytes: number };

export type TargetPosition = TargetItemRef & { seq: number; inRetry: boolean };

export type TargetImage = { routingHash: RoutingHash; hk: KeyBytes; sk: KeyBytes; revision: number } & (
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
	/** The routingHash of `hk`. */
	routingHash: RoutingHash;
	hk: KeyBytes;
	sk: KeyBytes;
	revision: number;
	deleted_at_ms: number | null;
	newest_seq: number | null;
	data_kind: number | null;
	ttl_at: number | null;
	/** The `item_bytes` of the source row. The page budget reads it; the insert measures the row again. */
	item_bytes: number;
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

/** The `WHERE` terms of the items of `routingHash` in the composite interval `[start, end)`. A null edge is unbounded. */
function latestSliceTerms(
	routingHash: RoutingHash,
	start: FokosCompositeKey | null,
	end: FokosCompositeKey | null,
): { conds: string; params: unknown[] } {
	const conds = ["routing_hash = ?"];
	const params: unknown[] = [routingHash];
	if (start !== null) {
		conds.push("(base_hk, base_sk) >= (?, ?)");
		params.push(start.hk, start.sk);
	}
	if (end !== null) {
		conds.push("(base_hk, base_sk) < (?, ?)");
		params.push(end.hk, end.sk);
	}
	return { conds: conds.join(" AND "), params };
}

function asKey(value: ArrayBuffer): KeyBytes {
	return KeyCodec.asKeyBytes(new Uint8Array(value));
}

function toTargetRow(row: { target_id: string; cursor_seq: number; epoch: number; is_batch_open: number }): TargetRow {
	return { targetId: row.target_id, cursorSeq: row.cursor_seq, epoch: row.epoch, isBatchOpen: row.is_batch_open === 1 };
}
