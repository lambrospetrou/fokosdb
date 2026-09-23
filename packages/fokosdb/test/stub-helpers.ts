/**
 * Stub accessors for tests. A test holds a concrete binding in `env`, so a stub needs no
 * partition context and no jurisdiction.
 */
import { env } from "cloudflare:workers";
import type { PartitionDO } from "../src/server/do-partition.js";
import type { TransactionCoordinatorDO } from "../src/server/do-transaction-coordinator.js";
import { PartitionContextCreator, type FokosDbRouteContext, type PartitionNamespaceKey } from "../src/shared/partition-context.js";
import { FokosRouter } from "../src/sharding/router.js";
import { COORDINATOR_REF_VERSION, type CoordinatorRef } from "../src/shared/transaction-wire-types.js";
import type { ControlledPartitionDO } from "./controlled-partition-do.js";

/** A name is a string and an id is an object, so one function serves both. */
export function testPartitionStub(
	nameOrId: string | DurableObjectId,
	ns: PartitionNamespaceKey = "PARTITION_DO",
): DurableObjectStub<PartitionDO> {
	const namespace: DurableObjectNamespace<PartitionDO> = env[ns];
	return typeof nameOrId === "string" ? namespace.getByName(nameOrId) : namespace.get(nameOrId);
}

/** A partition of `ControlledPartitionDO`, with the RPCs of its test controls. */
export function testControlledPartitionStub(name: string): DurableObjectStub<ControlledPartitionDO> {
	return env.CONTROLLED_PARTITION_DO.getByName(name);
}

export function testCoordinatorStubByName(doName: string): DurableObjectStub<TransactionCoordinatorDO> {
	return env.TRANSACTION_COORDINATOR_DO.getByName(doName);
}

/** The route context of a root coordinator in a new coordinator group. */
export function testCoordinatorContext(): FokosDbRouteContext {
	const table = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: `tc-test.${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
	});
	return new FokosRouter(table.topology, table.rangeConfig, table.policy).allRoots()[0];
}

/**
 * A coordinator reference. A prepare request carries one, and the partition stores it in its lock. A
 * test that never recovers the transaction needs only a valid reference.
 */
export function testCoordinatorRef(idempotencyToken = "test-token"): CoordinatorRef {
	return { v: COORDINATOR_REF_VERSION, doName: testCoordinatorContext().doName, idempotencyToken };
}
