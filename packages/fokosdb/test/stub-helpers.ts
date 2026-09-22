/**
 * Stub accessors for tests. A test holds a concrete binding in `env`, so a stub needs no
 * partition context and no jurisdiction.
 */
import { env } from "cloudflare:workers";
import type { PartitionDO } from "../src/server/do-partition.js";
import type { TransactionCoordinatorDO } from "../src/server/do-transaction-coordinator.js";
import type { PartitionNamespaceKey } from "../src/shared/partition-context.js";
import type { ControlledPartitionDO } from "./controlled-partition-do.js";

/** A name is a string and an id is an object, so one function serves both. */
export function testPartitionStub(
	nameOrId: string | DurableObjectId,
	ns: PartitionNamespaceKey = "PARTITION_DO",
): DurableObjectStub<PartitionDO> {
	const namespace: DurableObjectNamespace<PartitionDO> = env[ns];
	return typeof nameOrId === "string" ? namespace.getByName(nameOrId) : namespace.get(nameOrId);
}

/** A partition of `ControlledPartitionDO`, with the RPCs of its seams. */
export function testControlledPartitionStub(name: string): DurableObjectStub<ControlledPartitionDO> {
	return env.CONTROLLED_PARTITION_DO.getByName(name);
}

export function testCoordinatorStubByName(doName: string): DurableObjectStub<TransactionCoordinatorDO> {
	return env.TRANSACTION_COORDINATOR_DO.getByName(doName);
}

/** A string is the stringified form of an id, never a name. A name takes `testCoordinatorStubByName`. */
export function testCoordinatorStub(id: DurableObjectId | string): DurableObjectStub<TransactionCoordinatorDO> {
	const ns = env.TRANSACTION_COORDINATOR_DO;
	return ns.get(typeof id === "string" ? ns.idFromString(id) : id);
}
