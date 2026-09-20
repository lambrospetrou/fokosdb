/**
 * The one module that resolves a Durable Object namespace from a partition context and that
 * acquires a stub from it. Every caller passes a context, never a namespace, so the jurisdiction
 * of the table applies at every resolution and no call site decides whether it is needed.
 *
 * A binding key resolves to a union of namespace types (`PARTITION_DO` and any subclass binding),
 * so `env[ctx.ns].get(id)` yields a union of stub types. Each helper below pins the type parameter
 * and collapses that union to a single stub.
 *
 * The class imports are type-only, so this module carries no runtime dependency on the Durable
 * Object implementations and the client never pulls them into its bundle.
 */
import type { PartitionDO } from "../server/do-partition.js";
import type { TransactionCoordinatorDO } from "../server/do-transaction-coordinator.js";
import type { PartitionContext } from "../sharding/partition-context.js";

/**
 * The namespace the context names, with its jurisdiction applied, if any is provided.
 */
export function partitionNamespace(env: Env, ctx: PartitionContext): DurableObjectNamespace<PartitionDO> {
	const ns: DurableObjectNamespace<PartitionDO> = env[ctx.ns];
	return ctx.jurisdiction === undefined ? ns : ns.jurisdiction(ctx.jurisdiction);
}

/**
 * The coordinator namespace the context names, with its jurisdiction applied.
 */
export function txCoordinatorNamespace(env: Env, ctx: PartitionContext): DurableObjectNamespace<TransactionCoordinatorDO> {
	const ns: DurableObjectNamespace<TransactionCoordinatorDO> = env[ctx.nsTx];
	return ctx.jurisdiction === undefined ? ns : ns.jurisdiction(ctx.jurisdiction);
}

function stubOptions(ctx: PartitionContext): DurableObjectNamespaceGetDurableObjectOptions | undefined {
	return ctx.locationHint === undefined ? undefined : { locationHint: ctx.locationHint };
}

export function partitionStub(env: Env, ctx: PartitionContext, id: DurableObjectId): DurableObjectStub<PartitionDO> {
	const options = stubOptions(ctx);
	return options === undefined ? partitionNamespace(env, ctx).get(id) : partitionNamespace(env, ctx).get(id, options);
}

export function partitionStubByName(env: Env, ctx: PartitionContext, doName: string): DurableObjectStub<PartitionDO> {
	const options = stubOptions(ctx);
	return options === undefined ? partitionNamespace(env, ctx).getByName(doName) : partitionNamespace(env, ctx).getByName(doName, options);
}

export function txCoordinatorStub(
	env: Env,
	ctx: PartitionContext,
	doId: DurableObjectId | string,
): DurableObjectStub<TransactionCoordinatorDO> {
	const ns = txCoordinatorNamespace(env, ctx);
	const targetId = typeof doId === "string" ? ns.idFromString(doId) : doId;
	const options = stubOptions(ctx);
	// A string is the stringified form of an id, never a name: no caller resolves a coordinator by name.
	return options === undefined ? ns.get(targetId) : ns.get(targetId, options);
}
