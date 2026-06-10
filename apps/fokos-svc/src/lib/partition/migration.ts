import { isHashPartition, type PartitionContextResolved } from "../partition-topology/partition-context.js";
import type { PartitionStore, MigrationCursor, PendingTransactionCursor, PromotedKeyCursor, PromotedKeyStatus } from "./partition-store.js";
import type { PartitionPeer } from "./partition-peer.js";
import invariant from "../invariant.js";

export type PartitionSplitMigrationStatus = "migration_initialized" | "migration_migrating" | "migration_completed";

// The migration-owned KV keys. The status key is also read/written by PartitionDO's request-path
// gates (initFromSplit, ensureMigration) — those are request-path concerns and stay in the DO.
export const MIGRATION_KV_KEYS = {
	SPLIT_MIGRATION_STATUS: "__split_migration_status",
	SPLIT_MIGRATION_CURSOR: "__split_migration_cursor",
} as const;

export type SplitMigrationDeps = {
	store: PartitionStore;
	/** For the migration KV keys (status transition + cursor checkpoints). */
	storage: DurableObjectStorage;
	/** Gateway to the parent PartitionDO — the DO resolves the stub and hands it down. */
	parent: PartitionPeer;
	/** Structured-log context (the DO's logParams), so extracted logs keep their shape. */
	logParams: () => Record<string, unknown>;
	/** Keeps the DO's in-memory promoted-keys cache in sync with inherited entries. */
	onPromotedKeyInherited: (hashKey: string, status: PromotedKeyStatus) => void;
	/** Test hook: awaited just before the final status transition + parent acknowledgement. */
	beforeComplete?: () => Promise<void>;
};

/**
 * Child-side migration PULL DRIVER: pulls this partition's share of data from its parent after a
 * split (hash or range) or a promotion, with crash/resume via the SPLIT_MIGRATION_CURSOR KV
 * checkpoint. Parent-side batch serving deliberately stays as thin PartitionDO methods (an
 * authorization invariant + a store page query through collectBatch).
 */
export class SplitMigration {
	constructor(private readonly deps: SplitMigrationDeps) {}

	async runMigration(pCtx: PartitionContextResolved, parentCtx: PartitionContextResolved): Promise<void> {
		const migrationStatus = this.deps.storage.kv.get<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS);
		if (migrationStatus !== "migration_migrating") {
			console.log({
				...this.deps.logParams(),
				message: "fokos/partition.runMigration: migration not migrating.",
				migrationStatus,
			});
			return;
		}

		if (isHashPartition(pCtx)) {
			await this.runHashChildMigration(pCtx);
		} else {
			await this.runRangeChildMigration(pCtx, parentCtx);
		}
	}

	private async runHashChildMigration(pCtx: PartitionContextResolved): Promise<void> {
		const { store, storage, parent } = this.deps;

		let cursor = storage.kv.get<MigrationCursor>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_CURSOR) ?? null;

		while (true) {
			const { items, nextCursor } = await parent.getItemsBatch({
				childPartitionContext: pCtx,
				cursor,
			});

			if (items.length > 0) {
				for (const item of items) {
					// INSERT OR IGNORE rather than OR REPLACE: all writes to this partition are rejected
					// with 503 while migration_migrating, so no user write can have arrived yet.
					// IGNORE is safer for retries — if a batch was already written before a crash we
					// skip re-inserting those items rather than overwriting them unnecessarily.
					store.insertItemIfAbsent(item);
				}
			}

			// Checkpoint cursor after each batch so we can resume if interrupted.
			cursor = nextCursor;
			storage.kv.put<MigrationCursor | null>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_CURSOR, cursor);

			if (!nextCursor) break;
		}
		invariant(cursor === null, "fokos/partition.runHashChildMigration: loop exited with non-null cursor — data may be incomplete");

		// Migrate transaction metadata: pending locks and deletion high-water mark.
		let txCursor: PendingTransactionCursor | null = null;
		while (true) {
			const { maxDeletedTs, pendingTransactions, nextCursor } = await parent.getPartitionTransactionMetadata({
				childPartitionContext: pCtx,
				cursor: txCursor,
			});

			store.transactionSync(() => {
				for (const row of pendingTransactions) {
					store.insertPendingLock(row);
				}
				store.bumpMaxDeletedTs(maxDeletedTs);
			});

			if (!nextCursor) break;
			txCursor = nextCursor;
		}

		// Inherit promoted-key entries for the keys this child now owns. Only the forward-pointer entry
		// transfers — the data lives in the range structure, and hash-child item migration already excluded
		// promoted keys. Mutual exclusion guarantees every such key is 'promoted' at hash-split time.
		let pkCursor: PromotedKeyCursor | null = null;
		while (true) {
			const { rows, nextCursor } = await parent.getPromotedKeysBatch({
				childPartitionContext: pCtx,
				cursor: pkCursor,
			});
			if (rows.length > 0) {
				store.transactionSync(() => {
					for (const row of rows) {
						store.insertPromotedKey(row.hash_key, row.status, Date.now());
						this.deps.onPromotedKeyInherited(row.hash_key, row.status);
					}
				});
			}
			if (!nextCursor) break;
			pkCursor = nextCursor;
		}

		await this.deps.beforeComplete?.();
		store.rebuildKeySizeEstimates();
		storage.kv.put<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_completed");
		storage.kv.delete(MIGRATION_KV_KEYS.SPLIT_MIGRATION_CURSOR);
		await parent.acknowledgeChildMigrationComplete(pCtx.doName);

		console.log({
			...this.deps.logParams(),
			message: "fokos/partition: Hash child migration completed.",
		});
	}

	private async runRangeChildMigration(pCtx: PartitionContextResolved, parentCtx: PartitionContextResolved): Promise<void> {
		const { store, storage, parent } = this.deps;

		// Migrate items for this range DO's owned slice.
		// For a promotion root the parent is a hash DO (filter by hk only);
		// for a range-split child the parent is a range DO (filter by hk and sk range).
		let cursor = storage.kv.get<MigrationCursor>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_CURSOR) ?? null;

		while (true) {
			const { items, nextCursor } = await parent.getItemsBatch({
				childPartitionContext: pCtx,
				cursor,
			});

			if (items.length > 0) {
				for (const item of items) {
					store.insertItemIfAbsent(item);
				}
			}

			cursor = nextCursor;
			storage.kv.put<MigrationCursor | null>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_CURSOR, cursor);

			if (!nextCursor) break;
		}
		invariant(cursor === null, "fokos/partition.runRangeChildMigration: loop exited with non-null cursor");

		// Migrate transaction metadata: pending locks and the deletion high-water mark.
		// A promotion root's parent returns no pending locks (lock-free cutover), so this only syncs the watermark;
		// a range-split child's parent returns the locks in the child's [start, end) slice so commit/cancel can follow.
		let txCursor: PendingTransactionCursor | null = null;
		while (true) {
			const { maxDeletedTs, pendingTransactions, nextCursor } = await parent.getPartitionTransactionMetadata({
				childPartitionContext: pCtx,
				cursor: txCursor,
			});

			store.transactionSync(() => {
				for (const row of pendingTransactions) {
					store.insertPendingLock(row);
				}
				store.bumpMaxDeletedTs(maxDeletedTs);
			});

			if (!nextCursor) break;
			txCursor = nextCursor;
		}

		await this.deps.beforeComplete?.();
		store.rebuildKeySizeEstimates();
		storage.kv.put<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_completed");
		storage.kv.delete(MIGRATION_KV_KEYS.SPLIT_MIGRATION_CURSOR);

		// Notify the parent: a promotion root calls acknowledgePromotionComplete; a range-split child calls acknowledgeChildMigrationComplete.
		if (isHashPartition(parentCtx)) {
			await parent.acknowledgePromotionComplete(pCtx.rangePartition!.hashKey);
		} else {
			await parent.acknowledgeChildMigrationComplete(pCtx.doName);
		}

		console.log({
			...this.deps.logParams(),
			message: "fokos/partition: Range child migration completed.",
		});
	}
}
