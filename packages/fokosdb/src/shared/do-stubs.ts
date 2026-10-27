/**
 * The one module that resolves a Durable Object namespace from a route context and that acquires a
 * stub from it. Every caller passes a context, never a namespace, so the jurisdiction of the table
 * applies at every resolution and no call site decides whether it is needed.
 *
 * A binding key resolves to a union of namespace types (`PARTITION_DO` and any subclass binding),
 * so `env[ctx.policy.ns].get(id)` yields a union of stub types. Each helper below pins the type
 * parameter and collapses that union to a single stub.
 *
 * The class imports are type-only, so this module carries no runtime dependency on the Durable
 * Object implementations and the client never pulls them into its bundle.
 */
import type { FokosDBPartitionDO } from "../server/do-partition.js";
import type { FokosDBTransactionCoordinatorDO } from "../server/do-transaction-coordinator.js";
import type { FokosDBMaterializationDO } from "../server/materialization/do-materialization.js";
import invariant from "./invariant.js";
import type { FokosDBAutoAdminDO } from "../server/do-auto-admin.js";
import { topologyAdminName } from "../sharding/topology-codec.js";
import type { AdminNamespaceKey, FokosDBStubContext, TransactionCoordinatorNamespaceKey } from "./partition-context.js";

/**
 * The namespace the context names, with its jurisdiction applied, if any is provided.
 */
export function partitionNamespace(env: Env, ctx: FokosDBStubContext): DurableObjectNamespace<FokosDBPartitionDO> {
	const ns: DurableObjectNamespace<FokosDBPartitionDO> = env[ctx.policy.ns];
	return ctx.topology.jurisdiction === undefined ? ns : ns.jurisdiction(ctx.topology.jurisdiction);
}

/**
 * The coordinator namespace the context names, with its jurisdiction applied.
 */
export function txCoordinatorNamespace(env: Env, ctx: FokosDBStubContext): DurableObjectNamespace<FokosDBTransactionCoordinatorDO> {
	const ns: DurableObjectNamespace<FokosDBTransactionCoordinatorDO> = env[ctx.policy.nsTx];
	return ctx.topology.jurisdiction === undefined ? ns : ns.jurisdiction(ctx.topology.jurisdiction);
}

/**
 * The materialization namespace the context names, with its jurisdiction applied. Only a table with
 * `nsMat` has one.
 */
export function materializationNamespace(env: Env, ctx: FokosDBStubContext): DurableObjectNamespace<FokosDBMaterializationDO> {
	invariant(ctx.policy.nsMat !== undefined, "fokos/do-stubs: the table has no materialization namespace (nsMat)");
	const ns: DurableObjectNamespace<FokosDBMaterializationDO> = env[ctx.policy.nsMat];
	return ctx.topology.jurisdiction === undefined ? ns : ns.jurisdiction(ctx.topology.jurisdiction);
}

function stubOptions(ctx: FokosDBStubContext): DurableObjectNamespaceGetDurableObjectOptions | undefined {
	return ctx.policy.locationHint === undefined ? undefined : { locationHint: ctx.policy.locationHint };
}

export function partitionStub(env: Env, ctx: FokosDBStubContext, id: DurableObjectId): DurableObjectStub<FokosDBPartitionDO> {
	return partitionNamespace(env, ctx).get(id, stubOptions(ctx));
}

export function partitionStubByName(env: Env, ctx: FokosDBStubContext, doName: string): DurableObjectStub<FokosDBPartitionDO> {
	return partitionNamespace(env, ctx).getByName(doName, stubOptions(ctx));
}

export function materializationStubByName(env: Env, ctx: FokosDBStubContext, doName: string): DurableObjectStub<FokosDBMaterializationDO> {
	return materializationNamespace(env, ctx).getByName(doName, stubOptions(ctx));
}

export function txCoordinatorStubByName(
	env: Env,
	ctx: FokosDBStubContext,
	doName: string,
): DurableObjectStub<FokosDBTransactionCoordinatorDO> {
	return txCoordinatorNamespace(env, ctx).getByName(doName, stubOptions(ctx));
}

/**
 * The fields that a partition uses to reach a coordinator of its table. They come from the route
 * context of the partition, never from a coordinator context: the coordinators of a table use the
 * `nsTx` namespace and the jurisdiction of the table.
 */
export type ParticipantCoordinatorAddress = {
	nsTx: TransactionCoordinatorNamespaceKey;
	jurisdiction?: DurableObjectJurisdiction;
};

/**
 * The stub of a coordinator that a partition calls back from a lock. The coordinator exists already,
 * so no location hint applies.
 */
export function txCoordinatorStubForParticipant(
	env: Env,
	address: ParticipantCoordinatorAddress,
	doName: string,
): DurableObjectStub<FokosDBTransactionCoordinatorDO> {
	const ns: DurableObjectNamespace<FokosDBTransactionCoordinatorDO> = env[address.nsTx];
	return (address.jurisdiction === undefined ? ns : ns.jurisdiction(address.jurisdiction)).getByName(doName);
}

/**
 * The stub of a push target: the service binding `binding` of the Worker env, or the Durable Object
 * `objectName` of the namespace binding `binding`. A push target is not a FokosDB class, so no
 * jurisdiction and no location hint of the table apply.
 *
 * Throws when the Worker env has no such binding, or when the stub has no function `method`. Each
 * property of a stub is a function, so the check finds only a binding of the wrong kind, for example
 * a namespace with no `objectName`. A method that the target does not implement fails at the call.
 */
export function pushTargetStub(
	env: Env,
	target: { binding: string; method: string; objectName?: string },
): Record<string, (batch: unknown) => Promise<unknown>> {
	const binding: unknown = (env as unknown as Record<string, unknown>)[target.binding];
	invariant(typeof binding === "object" && binding !== null, () => `fokos/do-stubs: the Worker env has no binding ${target.binding}`);
	let stub: unknown = binding;
	if (target.objectName !== undefined) {
		const ns = binding as Partial<DurableObjectNamespace>;
		invariant(typeof ns.getByName === "function", () => `fokos/do-stubs: the binding ${target.binding} is not a Durable Object namespace`);
		stub = ns.getByName(target.objectName);
	}
	invariant(
		typeof (stub as Record<string, unknown>)[target.method] === "function",
		() => `fokos/do-stubs: the binding ${target.binding} has no method ${target.method}`,
	);
	return stub as Record<string, (batch: unknown) => Promise<unknown>>;
}

/**
 * The fields that select the admin Durable Objects of a table: the `nsAdmin` binding and the
 * jurisdiction of the table. The `FokosDBTopologyCache` entrypoint receives them as its `ctx.props`.
 */
export type TopologyAdminAddress = {
	nsAdmin: AdminNamespaceKey;
	jurisdiction?: DurableObjectJurisdiction;
};

/** The admin address of the table of `ctx`, or undefined when the table has no `nsAdmin`. */
export function topologyAdminAddress(ctx: FokosDBStubContext): TopologyAdminAddress | undefined {
	const { nsAdmin } = ctx.policy;
	if (nsAdmin === undefined) {
		return undefined;
	}
	return ctx.topology.jurisdiction === undefined ? { nsAdmin } : { nsAdmin, jurisdiction: ctx.topology.jurisdiction };
}

/** The stub of the admin Durable Object that holds the topology of `shardGroup`, `topologyAdminName(shardGroup)`. */
export function topologyAdminStub(env: Env, address: TopologyAdminAddress, shardGroup: string): DurableObjectStub<FokosDBAutoAdminDO> {
	const ns: DurableObjectNamespace<FokosDBAutoAdminDO> = env[address.nsAdmin];
	return (address.jurisdiction === undefined ? ns : ns.jurisdiction(address.jurisdiction)).getByName(topologyAdminName(shardGroup));
}
