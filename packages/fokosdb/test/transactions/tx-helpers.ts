/**
 * Shared setup for the transaction suites: a client over an isolated table, and the two key
 * selections the tests need — keys that fan out across partitions, and keys that all land in one.
 */
import { env } from "cloudflare:workers";
import { expect } from "vitest";
import { FokosDB } from "../../src/client/db.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { PartitionContextCreator } from "../../src/sharding/partition-context.js";
import { PartitionTopologyRouterImpl } from "../../src/sharding/router.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import type { TransactWriteItemsResult, TransactWriteOperationResult } from "../../src/shared/transaction-api-types.js";

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

export type MakeDBOptions = {
	singlePartitionFastPath?: boolean;
	maxSizeMb?: number;
	rootTreesN?: number;
	numTxCoordinators?: number;
	/** Fixes the table name. Routing is a pure function of it, so two clients built with the same
	 *  name share one topology. Omit it for a table no other test touches. */
	tableName?: string;
};

/** A client over its own table, so no two tests share partitions. */
export function makeDB(opts?: MakeDBOptions) {
	const { maxSizeMb, rootTreesN, tableName, ...dbOptions } = opts ?? {};
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: tableName ?? `txtest.${crypto.randomUUID()}`,
		rootTreesN: rootTreesN ?? 100,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: maxSizeMb ?? 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
	});
	const topology = new PartitionTopologyRouterImpl(base);
	return new FokosDB({ topology, ...dbOptions });
}

export function partitionNameOf(db: FokosDB, key: { hashKey: string; sortKey?: string }): string {
	const topology = db.options().topology as PartitionTopologyRouterImpl;
	return topology.pickPartition(KeyCodec.encode(key.hashKey), KeyCodec.encodeOptional(key.sortKey)).partitionContext.doName;
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
