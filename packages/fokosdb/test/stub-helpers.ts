/**
 * Stub accessors for tests. A test holds a concrete binding in `env`, so a stub needs no
 * partition context and no jurisdiction.
 */
import { env } from "cloudflare:workers";
import type { PartitionDO } from "../src/server/do-partition.js";
import type { TransactionCoordinatorDO } from "../src/server/do-transaction-coordinator.js";

/** A name is a string and an id is an object, so one function serves both. */
export function testPartitionStub(nameOrId: string | DurableObjectId): DurableObjectStub<PartitionDO> {
	return typeof nameOrId === "string" ? env.PARTITION_DO.getByName(nameOrId) : env.PARTITION_DO.get(nameOrId);
}

export function testCoordinatorStubByName(doName: string): DurableObjectStub<TransactionCoordinatorDO> {
	return env.TRANSACTION_COORDINATOR_DO.getByName(doName);
}

/** A string is the stringified form of an id, never a name. A name takes `testCoordinatorStubByName`. */
export function testCoordinatorStub(id: DurableObjectId | string): DurableObjectStub<TransactionCoordinatorDO> {
	const ns = env.TRANSACTION_COORDINATOR_DO;
	return ns.get(typeof id === "string" ? ns.idFromString(id) : id);
}
