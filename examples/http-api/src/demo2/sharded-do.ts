import { DurableObject } from "cloudflare:workers";
import type {
	FokosOperationSpec,
	FokosShardingAction,
	FokosShardingActions,
	FokosShardingRpc,
	FokosShardingRuntime,
} from "fokosdb/sharding/server";

/**
 * The base class of a Durable Object that hosts `FokosShardingRuntime`.
 *
 * The runtime calls `fokosRpc` on the other partitions of the same class, and the method sends the
 * call to the runtime. A host class extends this class, creates `fokos` in its
 * constructor, and adds its own operations.
 */
export abstract class ShardedDurableObject<TPolicy, Ops extends FokosOperationSpec> extends DurableObject<Env> implements FokosShardingRpc {
	abstract readonly fokos: FokosShardingRuntime<TPolicy, Ops>;

	async fokosRpc<A extends FokosShardingAction>(action: A, req: FokosShardingActions[A]["req"]): Promise<FokosShardingActions[A]["res"]> {
		return this.fokos.fokosRpc(action, req);
	}

	async alarm(info: AlarmInvocationInfo): Promise<void> {
		return this.fokos.alarm(info);
	}
}
