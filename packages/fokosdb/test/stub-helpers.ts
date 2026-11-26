/**
 * Stub accessors for tests. A test holds a concrete binding in `env`, so a stub needs no
 * partition context and no jurisdiction.
 */
import { env } from "cloudflare:workers";
import type { FokosDBPartitionDO } from "../src/server/do-partition.js";
import type { FokosDBTransactionCoordinatorDO } from "../src/server/do-transaction-coordinator.js";
import { tableFingerprint } from "../src/shared/do-stubs.js";
import {
	coordinatorShardGroup,
	createTableConfig,
	type FokosDBRouteContext,
	type FokosDBTableConfig,
	type PartitionNamespaceKey,
	type TransactionCoordinatorNamespaceKey,
} from "../src/shared/partition-context.js";
import { FokosRouter } from "../src/sharding/router.js";
import { COORDINATOR_REF_VERSION, type CoordinatorRef } from "../src/shared/transaction-wire-types.js";
import type { ControlledPartitionDO } from "./controlled-partition-do.js";
import type { ControlledTransactionCoordinatorDO } from "./controlled-transaction-coordinator-do.js";
import { testRoutingHashSeedResolver } from "./routing-hash.js";

/** A name is a string and an id is an object, so one function serves both. */
export function testPartitionStub(
	nameOrId: string | DurableObjectId,
	ns: PartitionNamespaceKey = "PARTITION_DO",
): DurableObjectStub<FokosDBPartitionDO> {
	const namespace: DurableObjectNamespace<FokosDBPartitionDO> = env[ns];
	return typeof nameOrId === "string" ? namespace.getByName(nameOrId) : namespace.get(nameOrId);
}

/** A partition of `ControlledPartitionDO`, with the RPCs of its test controls. */
export function testControlledPartitionStub(name: string): DurableObjectStub<ControlledPartitionDO> {
	return env.CONTROLLED_PARTITION_DO.getByName(name);
}

export function testCoordinatorStubByName(doName: string): DurableObjectStub<FokosDBTransactionCoordinatorDO> {
	return env.TRANSACTION_COORDINATOR_DO.getByName(doName);
}

/** A coordinator of `ControlledTransactionCoordinatorDO`, with the RPCs of its test controls. */
export function testControlledCoordinatorStub(doName: string): DurableObjectStub<ControlledTransactionCoordinatorDO> {
	return env.CONTROLLED_TRANSACTION_COORDINATOR_DO.getByName(doName);
}

/**
 * The table of a new coordinator group: one hash root of `PARTITION_DO` with two children for each
 * split.
 */
export function testCoordinatorTable(nsTx: TransactionCoordinatorNamespaceKey = "TRANSACTION_COORDINATOR_DO"): FokosDBTableConfig {
	return createTableConfig({
		table: { name: `tc-test.${crypto.randomUUID()}`, ns: "PARTITION_DO", nsTx, rootTreesN: 1, hashSplitN: 2 },
		hashSplitConditions: { maxSizeMb: 100 },
	});
}

/**
 * The route context of the root coordinator of the coordinator group of `table`, named as FokosDB
 * names it: `fokos.tc.<tableName>.<fp>`.
 */
export function testCoordinatorContext(
	nsTx: TransactionCoordinatorNamespaceKey = "TRANSACTION_COORDINATOR_DO",
	table: FokosDBTableConfig = testCoordinatorTable(nsTx),
): FokosDBRouteContext {
	const shardGroup = coordinatorShardGroup(table.topology, tableFingerprint(env, table));
	return new FokosRouter(
		{ ...table.topology, shardGroup },
		table.rangeConfig,
		table.policy,
		undefined,
		testRoutingHashSeedResolver,
	).allRoots()[0];
}

/**
 * A coordinator reference. A prepare request carries one, and the partition stores it in its lock. A
 * test that never recovers the transaction needs only a valid reference.
 */
export function testCoordinatorRef(idempotencyToken = "test-token"): CoordinatorRef {
	return { v: COORDINATOR_REF_VERSION, doName: testCoordinatorContext().doName, idempotencyToken };
}
