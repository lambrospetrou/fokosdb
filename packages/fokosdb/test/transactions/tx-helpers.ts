/**
 * Shared setup for the transaction suites: a client over an isolated table, and the two key
 * selections the tests need — keys that fan out across partitions, and keys that all land in one.
 */
import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { expect, vi } from "vitest";
import { FokosDB } from "../../src/client/db.js";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { type FokosDbRouteContext, PartitionContextCreator } from "../../src/shared/partition-context.js";
import { txOrderTimestampNow } from "../../src/shared/transaction-limits.js";
import type { TransactionItem } from "../../src/shared/transaction-wire-types.js";
import type { ControlledPartitionDO, TxOp, TxRequest } from "../controlled-partition-do.js";
import type { ControlledTransactionCoordinatorDO } from "../controlled-transaction-coordinator-do.js";
import { openedRpc } from "../partition-do/helpers.js";
import { testPartitionStub } from "../stub-helpers.js";
import { FokosRouter } from "../../src/sharding/router.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import type {
	TransactWriteItemsOptions,
	TransactWriteItemsResult,
	TransactWriteOperationResult,
} from "../../src/shared/transaction-api-types.js";

export type Key = { hashKey: string; sortKey: string };

/**
 * The outcome of a transaction write: the committed result, or the fields of the cancel it raised.
 * The tag is this helper's own. `transactWriteItems` returns the committed result alone and raises on
 * a cancel, so only a value that holds both needs one.
 */
export type WriteOutcome =
	| ({ outcome: "committed" } & TransactWriteItemsResult)
	| {
			outcome: "cancelled";
			transactionId: string;
			idempotencyToken: string;
			results: TransactWriteOperationResult[];
	  };

/**
 * Awaits a transaction write that can commit or cancel. A cancelled `transactWriteItems` raises
 * FokosTransactionCancelledError, and a test that checks either outcome reads it back as a value here.
 */
export async function writeOutcome(write: Promise<TransactWriteItemsResult>): Promise<WriteOutcome> {
	try {
		return { outcome: "committed", ...(await write) };
	} catch (e) {
		if (!FokosTransactionCancelledError.is(e)) throw e;
		return {
			outcome: "cancelled",
			transactionId: e.attributes.transactionId as string,
			idempotencyToken: e.attributes.idempotencyToken as string,
			results: e.results,
		};
	}
}

/**
 * Sends a transaction write, and sends it again after a cancel with `timestamp_conflict`.
 *
 * On the two-phase path, the coordinator stamps the transaction with its own clock. A partition
 * refuses a stamp that is not above the stamp of the item, or above the last delete of the partition.
 * A write of the same test, or of another test on a shared table, can have a stamp in the same
 * millisecond, and the clock can go back in local workerd/miniflare.
 * A cancelled transaction applies nothing, thus a new attempt is safe.
 * A cancel for any other reason returns at once.
 *
 * A request with a `clientRequestToken` is refused: a replay of that token returns the same cancel.
 */
export async function writeOutcomeWithClockRetry(db: FokosDB, request: TransactWriteItemsOptions): Promise<WriteOutcome> {
	expect(request.clientRequestToken, "a replay of a token returns the same cancel").toBeUndefined();
	let outcome = await writeOutcome(db.transactWriteItems(request));
	for (let attempt = 1; attempt < 5 && isTimestampConflict(outcome); attempt++) {
		await new Promise((resolve) => setTimeout(resolve, 2));
		outcome = await writeOutcome(db.transactWriteItems(request));
	}
	return outcome;
}

function isTimestampConflict(outcome: WriteOutcome): boolean {
	return (
		outcome.outcome === "cancelled" && outcome.results.some((op) => op.outcome === "rejected" && op.reason.code === "timestamp_conflict")
	);
}

export type MakeDBOptions = {
	singlePartitionFastPath?: boolean;
	maxSizeMb?: number;
	rootTreesN?: number;
	numTxCoordinators?: number;
	/** Fixes the table name. Routing is a pure function of it, so two clients built with the same
	 *  name share one topology. Omit it for a table no other test touches. */
	tableName?: string;
	/**
	 * Puts the table on `ControlledPartitionDO` and `ControlledTransactionCoordinatorDO`, so that a
	 * test can use their test controls. The coordinator pool defaults to one coordinator, which
	 * `controlledCoordinator` reaches.
	 */
	controlled?: boolean;
};

/** A client over its own table, so no two tests share partitions. */
export function makeDB(opts?: MakeDBOptions) {
	const { maxSizeMb, rootTreesN, tableName, controlled, ...dbOptions } = opts ?? {};
	const base = PartitionContextCreator.create({
		ns: controlled ? "CONTROLLED_PARTITION_DO" : "PARTITION_DO",
		nsTx: controlled ? "CONTROLLED_TRANSACTION_COORDINATOR_DO" : "TRANSACTION_COORDINATOR_DO",
		tableName: tableName ?? `txtest.${crypto.randomUUID()}`,
		rootTreesN: rootTreesN ?? 100,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: maxSizeMb ?? 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
	});
	const topology = new FokosRouter(base.topology, base.rangeConfig, base.policy);
	return new FokosDB({ topology, ...(controlled ? { numTxCoordinators: 1 } : {}), ...dbOptions });
}

export function partitionNameOf(db: FokosDB, key: { hashKey: string; sortKey?: string }): string {
	const topology = db.options().topology;
	return topology.rootContext(KeyCodec.encode(key.hashKey)).doName;
}

export function countDistinctPartitions(db: FokosDB, keys: Array<{ hashKey: string; sortKey?: string }>): number {
	const names = new Set<string>();
	for (const k of keys) {
		names.add(partitionNameOf(db, k));
	}
	return names.size;
}

/** Return `count` keys that resolve to DISTINCT partitions, so a transaction over them fans out. */
export function keysAcrossPartitions(db: FokosDB, count: number, prefix: string): Key[] {
	const byPartition = new Map<string, Key>();
	for (let i = 0; byPartition.size < count; i++) {
		const key = { hashKey: `${prefix}-${i}`, sortKey: "sk" };
		byPartition.set(partitionNameOf(db, key), key);
	}
	return [...byPartition.values()];
}

/** Return `count` keys that all route to the SAME partition, so the fast path applies to the whole set. */
export function keysInOnePartition(db: FokosDB, count: number, prefix: string): Key[] {
	const MAX_CANDIDATES = 2_000;
	const buckets = new Map<string, Key[]>();
	for (let i = 0; ; i++) {
		expect(i, `no partition held ${count} keys within ${MAX_CANDIDATES} candidates`).toBeLessThan(MAX_CANDIDATES);
		const key = { hashKey: `${prefix}-${i}`, sortKey: "sk" };
		const bucket = buckets.get(partitionNameOf(db, key)) ?? [];
		bucket.push(key);
		buckets.set(partitionNameOf(db, key), bucket);
		if (bucket.length === count) return bucket;
	}
}

/** The stub and route context of the partition that owns `key`, in the namespace of the table. */
export function owningPartition(db: FokosDB, key: Key) {
	const { topology } = db.options();
	const pCtx = topology.rootContext(KeyCodec.encode(key.hashKey));
	const stub = testPartitionStub(pCtx.doName, topology.policy.ns);
	return { stub, rpc: openedRpc(stub), pCtx };
}

/** The test controls of the partition that owns `key`. The table must be `controlled`. */
export function controlledPartition(db: FokosDB, key: Key): DurableObjectStub<ControlledPartitionDO> {
	const { topology } = db.options();
	expect(topology.policy.ns, "a test control needs a table made with { controlled: true }").toBe("CONTROLLED_PARTITION_DO");
	return env.CONTROLLED_PARTITION_DO.getByName(partitionNameOf(db, key));
}

/**
 * The requests of `op` that the partition of each key received, in one list. A partition that owns
 * two of the keys counts once.
 */
export async function txCalls<Op extends TxOp>(db: FokosDB, keys: Key[], op: Op): Promise<TxRequest<Op>[]> {
	const partitions = new Map(keys.map((key) => [partitionNameOf(db, key), controlledPartition(db, key)]));
	// The RPC stub type drops the type parameter of `testTxCalls`, so the cast restores it.
	return (await Promise.all([...partitions.values()].map((p) => p.testTxCalls(op)))).flat() as TxRequest<Op>[];
}

/** The test controls of the one coordinator of a `controlled` table. */
export function controlledCoordinator(db: FokosDB): DurableObjectStub<ControlledTransactionCoordinatorDO> {
	const { topology, numTxCoordinators } = db.options();
	expect(topology.policy.nsTx, "a test control needs a table made with { controlled: true }").toBe("CONTROLLED_TRANSACTION_COORDINATOR_DO");
	expect(numTxCoordinators, "the coordinator test controls need a pool of one coordinator").toBe(1);
	// The pool names each coordinator `<shardGroupName>-<shard>`, and FokosDB sets the group name.
	return env.CONTROLLED_TRANSACTION_COORDINATOR_DO.getByName(`fokos_tc.${topology.topology.shardGroup}-0`);
}

/**
 * Holds a two-phase transaction lock on `key` and returns the release. The prepare stamps the
 * partition's own clock, so it always orders above the writes that seeded the item.
 */
export async function holdPendingLock(
	db: FokosDB,
	key: Key,
	item: Omit<TransactionItem, "opIndex" | "hashKey" | "sortKey">,
): Promise<() => Promise<unknown>> {
	const { rpc, pCtx } = owningPartition(db, key);
	const transactionId = crypto.randomUUID();
	const keys = { hashKey: KeyCodec.encode(key.hashKey), sortKey: KeyCodec.encode(key.sortKey) };
	const res = await rpc.txPrepare(pCtx, {
		transactionId,
		coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
		transactionTimestamp: txOrderTimestampNow(),
		items: [{ opIndex: 0, ...keys, ...item }],
	});
	expect(res.outcome).toBe("accepted");
	return () => rpc.txCancel(pCtx, { transactionId, items: [keys] });
}

/**
 * Runs `read`, a two-phase `transactGetItems`, and runs `between` inside the partition that owns
 * `key` after its phase-one read and before that answer returns. `between` calls the DO instance
 * directly, so the second phase sees a real committed mutation. The table must be `controlled`.
 */
export async function betweenPhases<T>(
	db: FokosDB,
	key: Key,
	between: (instance: PartitionDO, state: DurableObjectState, pCtx: FokosDbRouteContext) => Promise<void>,
	read: () => Promise<T>,
): Promise<T> {
	const partition = controlledPartition(db, key);
	const { pCtx } = owningPartition(db, key);
	await partition.testHoldReadPhase();
	try {
		const result = read();
		// The caller gets a failure of the read from the `await` below.
		result.catch(() => {});
		await vi.waitFor(async () => expect(await partition.testReadPhaseParked()).toBe(true), { timeout: 5000, interval: 10 });
		await runInDurableObject(partition, (instance: PartitionDO, state: DurableObjectState) => between(instance, state, pCtx));
		await partition.testReleaseReadPhase();
		return await result;
	} finally {
		await partition.testReleaseReadPhase();
	}
}
