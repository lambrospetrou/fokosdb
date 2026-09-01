/**
 * Typed accessors for the partition Durable Object.
 *
 * A binding key resolves to a union of namespace types (`PARTITION_DO` and any subclass binding),
 * so `env[ctx.ns].get(id)` yields a union of stub types. Each helper below pins the type parameter
 * and collapses that union to a single stub, exactly as the equivalent static on the class does.
 *
 * The class import is type-only, so this module carries no runtime dependency on the Durable
 * Object implementation and the client never pulls it into its bundle.
 */
import type { PartitionDO } from "./do-partition.js";

export function partitionStub(ns: DurableObjectNamespace<PartitionDO>, id: DurableObjectId): DurableObjectStub<PartitionDO> {
	return ns.get(id);
}

export function partitionStubByName(ns: DurableObjectNamespace<PartitionDO>, doName: string): DurableObjectStub<PartitionDO> {
	return ns.getByName(doName);
}
