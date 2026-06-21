import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../do-partition.js";
import { PartitionContextCreator, type PartitionContext, type PartitionContextResolved } from "../partition-topology/partition-context.js";
import { PartitionIdHelper } from "../partition-topology/partition-id.js";
import { MIGRATION_KV_KEYS, SplitMigration, type PartitionSplitMigrationStatus } from "./migration.js";
import type {
	GetItemsBatchResult,
	GetPartitionTransactionMetadataResult,
	GetPromotedKeysBatchResult,
	PartitionPeer,
} from "./partition-peer.js";
import { PartitionStore, type MigratedItem, type ScanCursor, type PromotedKeyStatus } from "./partition-store.js";
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";

const kb = (s: string) => KeyCodec.encode(s);

// ─── Tests ────────────────────────────────────────────────────────────────────

describe("SplitMigration — hash child", () => {
	it("pulls all batches, completes, cleans up the cursor, and acks the parent by doName", async () => {
		const base = makeBase();
		const pCtx = hashCtx(base, [0, 1]);
		const parentCtx = hashCtx(base, [0]);
		const all = [item("a", "1"), item("a", "2"), item("b", "1"), item("b", "2"), item("c", "1")];

		await withMigrationEnv(async (menv) => {
			const { peer, calls } = makeFakePeer({ items: pagedItemBatches(all, 2) });
			await menv.makeMigration(peer).runMigration(pCtx, parentCtx);

			// All items ingested exactly once, in resumable batches.
			expect(menv.store.queryItemsPage(null, 100)).toHaveLength(all.length);
			expect(calls.itemCursors).toEqual([null, { hk: kb("a"), sk: kb("2") }, { hk: kb("b"), sk: kb("2") }]);

			// Completion: status transitioned, cursor checkpoint removed, parent acked with this child's doName.
			expect(menv.status()).toBe("migration_completed");
			expect(menv.cursor()).toBeUndefined();
			expect(calls.childAcks).toEqual([pCtx.doName]);
			expect(calls.promotionAcks).toEqual([]);
		});
	});

	it("resumes from the checkpointed cursor after a crash mid-migration", async () => {
		const base = makeBase();
		const pCtx = hashCtx(base, [0, 1]);
		const parentCtx = hashCtx(base, [0]);
		const all = [item("a", "1"), item("a", "2"), item("b", "1"), item("b", "2"), item("c", "1")];

		await withMigrationEnv(async (menv) => {
			// Run 1: crashes on the second batch — after batch 1's cursor checkpoint landed.
			const run1 = makeFakePeer({ items: pagedItemBatches(all, 2), failItemsCall: 2 });
			await expect(menv.makeMigration(run1.peer).runMigration(pCtx, parentCtx)).rejects.toThrow(/simulated parent crash/);
			expect(menv.status()).toBe("migration_migrating");
			expect(menv.cursor()).toEqual({ hk: kb("a"), sk: kb("2") });
			expect(run1.calls.childAcks).toEqual([]);

			// Run 2 (fresh driver, same storage): resumes strictly after the checkpoint — batch 1 is
			// NOT re-fetched — and completes.
			const run2 = makeFakePeer({ items: pagedItemBatches(all, 2) });
			await menv.makeMigration(run2.peer).runMigration(pCtx, parentCtx);
			expect(run2.calls.itemCursors[0]).toEqual({ hk: kb("a"), sk: kb("2") });
			expect(menv.store.queryItemsPage(null, 100)).toHaveLength(all.length);
			expect(menv.status()).toBe("migration_completed");
			expect(run2.calls.childAcks).toEqual([pCtx.doName]);
		});
	});

	it("re-ingesting an already-written batch is idempotent (INSERT OR IGNORE keeps the original rows)", async () => {
		const base = makeBase();
		const pCtx = hashCtx(base, [0, 1]);
		const parentCtx = hashCtx(base, [0]);

		await withMigrationEnv(async (menv) => {
			// The first row is already present locally (written by a previous, partially-crashed run
			// whose cursor checkpoint did NOT land) with a higher version.
			menv.store.insertItemIfAbsent(item("a", "1", "already-written", 7));

			const { peer } = makeFakePeer({ items: pagedItemBatches([item("a", "1"), item("a", "2")], 10) });
			await menv.makeMigration(peer).runMigration(pCtx, parentCtx);

			const rows = menv.store.queryItemsPage(null, 100);
			expect(rows).toHaveLength(2);
			// The pre-existing row was NOT overwritten.
			expect(rows[0]).toMatchObject({ hk: kb("a"), sk: kb("1"), data: "already-written", v: 7 });
		});
	});

	it("syncs pending locks and the deletion watermark from the parent", async () => {
		const base = makeBase();
		const pCtx = hashCtx(base, [0, 1]);
		const parentCtx = hashCtx(base, [0]);
		const lock = {
			hk: kb("a"),
			sk: kb("1"),
			transaction_id: "tx1",
			transaction_ts: 123,
			operation: "put",
			data: "d",
			conditions_json: null,
			coordinator_do_id: "tc-1",
			created_at: 1000,
		};

		await withMigrationEnv(async (menv) => {
			const { peer } = makeFakePeer({
				txBatches: [{ maxDeletedTs: 4567, pendingTransactions: [lock], nextCursor: null }],
			});
			await menv.makeMigration(peer).runMigration(pCtx, parentCtx);

			expect(menv.store.pendingLockFor(kb("a"), kb("1"))?.transaction_id).toBe("tx1");
			expect(menv.store.getMaxDeletedTs()).toBe(4567);
		});
	});

	it("inherits promoted-key entries and reports each to the cache callback", async () => {
		const base = makeBase();
		const pCtx = hashCtx(base, [0, 1]);
		const parentCtx = hashCtx(base, [0]);

		await withMigrationEnv(async (menv) => {
			const { peer } = makeFakePeer({
				pkBatches: [{ rows: [{ hash_key: kb("big-key"), status: "promoted" }], nextCursor: null }],
			});
			await menv.makeMigration(peer).runMigration(pCtx, parentCtx);

			expect(menv.store.getPromotedKeyStatus(kb("big-key"))).toBe("promoted");
			expect(menv.inherited).toEqual([[kb("big-key"), "promoted"]]);
		});
	});
});

describe("SplitMigration — range child ack routing", () => {
	it("a promotion root (HASH parent) acks via acknowledgePromotionComplete with the hashKey", async () => {
		const base = makeBase();
		const pCtx = rangeCtx(base, "big-key", null, null);
		const parentCtx = hashCtx(base, [0]); // hash DO parent ⇒ promotion

		await withMigrationEnv(async (menv) => {
			const { peer, calls } = makeFakePeer({ items: pagedItemBatches([item("big-key", "1")], 10) });
			await menv.makeMigration(peer).runMigration(pCtx, parentCtx);

			expect(calls.promotionAcks).toEqual([kb("big-key")]);
			expect(calls.childAcks).toEqual([]);
			// Range children never pull promoted-key entries.
			expect(calls.pkCalls).toBe(0);
			expect(menv.status()).toBe("migration_completed");
		});
	});

	it("a range-split child (RANGE parent) acks via acknowledgeChildMigrationComplete with its doName", async () => {
		const base = makeBase();
		const pCtx = rangeCtx(base, "big-key", null, "m");
		const parentCtx = rangeCtx(base, "big-key", null, null); // range DO parent ⇒ range split

		await withMigrationEnv(async (menv) => {
			const { peer, calls } = makeFakePeer({ items: pagedItemBatches([item("big-key", "a")], 10) });
			await menv.makeMigration(peer).runMigration(pCtx, parentCtx);

			expect(calls.childAcks).toEqual([pCtx.doName]);
			expect(calls.promotionAcks).toEqual([]);
			expect(calls.pkCalls).toBe(0);
		});
	});
});

describe("SplitMigration — status gate", () => {
	it("is a no-op (no peer calls) unless status is migration_migrating", async () => {
		const base = makeBase();
		const pCtx = hashCtx(base, [0, 1]);
		const parentCtx = hashCtx(base, [0]);

		await withMigrationEnv(async (menv) => {
			menv.storage.kv.put<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_completed");
			const { peer, calls } = makeFakePeer({ items: pagedItemBatches([item("a", "1")], 10) });
			await menv.makeMigration(peer).runMigration(pCtx, parentCtx);

			expect(calls.itemCursors).toEqual([]);
			expect(calls.txCalls).toBe(0);
			expect(calls.childAcks).toEqual([]);
		});
	});
});

// ─── Harness ──────────────────────────────────────────────────────────────────

function makeBase(): PartitionContext {
	return PartitionContextCreator.create({
		ns: "PARTITION_DO",
		tableName: `migration-${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
	});
}

function hashCtx(base: PartitionContext, idxs: number[]): PartitionContextResolved {
	const { opaque, doName } = PartitionIdHelper.fromHashIdxs(base, idxs).encode(true);
	return { ...base, doName: doName!, primaryDoIdStr: "", partitionId: opaque };
}

function rangeCtx(base: PartitionContext, hashKey: string, start: string | null, end: string | null): PartitionContextResolved {
	const startKey = start === null ? undefined : start;
	const endKey = end === null ? undefined : end;
	const { opaque, doName } = PartitionIdHelper.fromRangePartition(
		base,
		kb(hashKey),
		KeyCodec.encodeOptional(startKey),
		KeyCodec.encodeOptional(endKey),
	).encode(true);
	return {
		...base,
		doName: doName!,
		primaryDoIdStr: "",
		partitionId: opaque,
		rangePartition: {
			hashKey: kb(hashKey),
			startBoundary: KeyCodec.encodeOptional(startKey),
			endBoundary: KeyCodec.encodeOptional(endKey),
		},
	};
}

function item(hk: string, sk: string, data = `data-${hk}-${sk}`, v = 1): MigratedItem {
	return { hk: kb(hk), sk: kb(sk), data, ttl_epoch_utc_seconds: null, v, last_transaction_ts: 0 };
}

// Serves `all` (already in (hk, sk) order) in pages of `batchSize`, honoring the resume cursor —
// mirrors the parent's real batch-serving contract (nextCursor non-null only when more remains).
function pagedItemBatches(all: MigratedItem[], batchSize: number) {
	return (cursor: ScanCursor | null): GetItemsBatchResult => {
		const start =
			cursor === null
				? 0
				: all.findIndex(
						(i) =>
							KeyCodec.compare(i.hk, cursor.hk) > 0 || (KeyCodec.compare(i.hk, cursor.hk) === 0 && KeyCodec.compare(i.sk, cursor.sk) > 0),
					);
		const items = start === -1 ? [] : all.slice(start, start + batchSize);
		const hasMore = start !== -1 && start + batchSize < all.length;
		const last = items[items.length - 1];
		return { items, nextCursor: hasMore && last ? { hk: last.hk, sk: last.sk } : null };
	};
}

type FakePeerOptions = {
	items?: (cursor: ScanCursor | null) => GetItemsBatchResult;
	txBatches?: GetPartitionTransactionMetadataResult[];
	pkBatches?: GetPromotedKeysBatchResult[];
	/** 1-based getItemsBatch call number that throws once (simulated crash mid-migration). */
	failItemsCall?: number;
};

function makeFakePeer(opts: FakePeerOptions = {}) {
	const calls = {
		itemCursors: [] as (ScanCursor | null)[],
		txCalls: 0,
		pkCalls: 0,
		childAcks: [] as string[],
		promotionAcks: [] as KeyBytes[],
	};
	let txIdx = 0;
	let pkIdx = 0;
	let failItemsCall = opts.failItemsCall ?? 0;
	const peer: PartitionPeer = {
		async getItemsBatch({ cursor }) {
			calls.itemCursors.push(cursor);
			if (failItemsCall > 0 && calls.itemCursors.length === failItemsCall) {
				failItemsCall = 0; // fail once, then recover
				throw new Error("simulated parent crash");
			}
			return opts.items?.(cursor) ?? { items: [], nextCursor: null };
		},
		async getPartitionTransactionMetadata() {
			calls.txCalls++;
			return opts.txBatches?.[txIdx++] ?? { maxDeletedTs: 0, pendingTransactions: [], nextCursor: null };
		},
		async getPromotedKeysBatch() {
			calls.pkCalls++;
			return opts.pkBatches?.[pkIdx++] ?? { rows: [], nextCursor: null };
		},
		async acknowledgeChildMigrationComplete(childDoName) {
			calls.childAcks.push(childDoName);
		},
		async acknowledgePromotionComplete(hashKey) {
			calls.promotionAcks.push(hashKey);
		},
		async initFromSplit() {},
		async triggerMigration() {},
	};
	return { peer, calls };
}

type MigrationEnv = {
	store: PartitionStore;
	storage: DurableObjectStorage;
	inherited: [KeyBytes, PromotedKeyStatus][];
	makeMigration: (peer: PartitionPeer) => SplitMigration;
	status: () => PartitionSplitMigrationStatus | undefined;
	cursor: () => ScanCursor | null | undefined;
};

// Real DO storage (vitest-pool-workers); the peer is the only fake — exactly what the gateway
// pattern buys: migration is testable without spinning up two real DOs.
async function withMigrationEnv(fn: (menv: MigrationEnv) => Promise<void>): Promise<void> {
	const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(`migration-test.${crypto.randomUUID()}`));
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		const inherited: [KeyBytes, PromotedKeyStatus][] = [];
		state.storage.kv.put<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_migrating");
		await fn({
			store,
			storage: state.storage,
			inherited,
			makeMigration: (peer) =>
				new SplitMigration({
					store,
					storage: state.storage,
					parent: peer,
					logParams: () => ({ test: "migration.test.ts" }),
					onPromotedKeyInherited: (hk, status) => inherited.push([hk, status]),
				}),
			status: () => state.storage.kv.get<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS),
			cursor: () => state.storage.kv.get<ScanCursor | null>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_CURSOR),
		});
	});
}
