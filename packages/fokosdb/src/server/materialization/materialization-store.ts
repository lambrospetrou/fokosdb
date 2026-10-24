/**
 * The SQLite storage of `MaterializationDO`: the latest image of each base item that the partition
 * accepted, the highest epoch of each producer, the change positions that the targets read, the state
 * of each target, and the failed items of each target. The size estimate of each base hash key
 * belongs to `keySizes`, which this store updates in the transaction of each image write.
 */
import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import { DATA_KINDS, type DataKind } from "../../shared/types.js";
import type { KeyBytes } from "../../sharding/key-codec.js";
import { one, tryOne } from "../../shared/sql-cursor.js";
import { EST_ROW_BYTES_K, estRowBytesExpr, JSON_KIND_CODE } from "../../shared/partition/item-size.js";
import { KeySizeStats } from "../../shared/partition/key-size-stats.js";
import type { FokosTargetDefinition } from "./targets.js";

/** The KV key of the target list. The first valid list is stored, and the partition uses only it. */
const TARGETS_KV_KEY = "__fokosdb/mat/targets";

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
		description: "Create the latest images, the key size estimates, the producer fences, the change positions, and the target state",
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

            CREATE INDEX IF NOT EXISTS idx_target_retry_inherited ON target_retry (target_id) WHERE is_inherited = 1 AND kind = 'retry';`,
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
	 */
	putLatestImage(image: LatestImage, nowMs: number, current: StoredImage | undefined, targets: readonly TargetPositions[]): void {
		const sql = this.#storage.sql;
		const position = this.#planPosition(current?.newestSeq ?? null, targets);
		const newestSeq = position === null ? (current?.newestSeq ?? null) : position.seq;
		const row = image.deleted
			? sql.exec<{ item_id: number; est_row_bytes: number }>(
					`INSERT INTO latest_items (base_hk, base_sk, revision, deleted_at_ms, newest_seq, data_kind, ttl_at, est_row_bytes, data)
					 VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, octet_length(?1) + octet_length(?2) + ${EST_ROW_BYTES_K}, NULL)
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
					 VALUES (?1, ?2, ?3, NULL, ?7, ?4, ?5, ${estRowBytesExpr("?6", "?1", "?2")}, ?6)
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
		// The ingest does not use the new estimate, so an equal size needs no read.
		const oldEst = current?.estRowBytes ?? 0;
		if (newEst !== oldEst) {
			this.keySizes.change(image.hashKey, oldEst, newEst);
		}
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

	// ─── cleanup ──────────────────────────────────────────────────────────────

	/** The lowest cursor of all targets, or null when the partition has no target. */
	minCursor(): number | null {
		return one(this.#storage.sql.exec<{ m: number | null }>(`SELECT MIN(cursor_seq) AS m FROM materialization_targets`)).m;
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
		if (hasRetry !== undefined) {
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
		// The positions of an item link from the newest to the oldest. The cleanup can have deleted the
		// older part of the chain already.
		for (let seq = row.newest_seq; seq !== null; ) {
			seq =
				tryOne(sql.exec<{ prev_seq: number | null }>(`DELETE FROM change_positions WHERE seq = ? RETURNING prev_seq`, seq))?.prev_seq ??
				null;
		}
		this.keySizes.subtract(new Uint8Array(row.base_hk) as KeyBytes, row.est_row_bytes);
		return true;
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

function toTargetRow(row: { target_id: string; cursor_seq: number; epoch: number; is_batch_open: number }): TargetRow {
	return { targetId: row.target_id, cursorSeq: row.cursor_seq, epoch: row.epoch, isBatchOpen: row.is_batch_open === 1 };
}
