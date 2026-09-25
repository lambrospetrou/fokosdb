import { DurableObject } from "cloudflare:workers";
import type {
	FokosEnvelope,
	FokosExecuteLocalRequest,
	FokosInitRequest,
	FokosMigrationAckRequest,
	FokosMigrationPage,
	FokosMigrationPullRequest,
	FokosOperationSpec,
	FokosPrepareDestroyRequest,
	FokosRequestPromotionRequest,
	FokosRequestPromotionResult,
	FokosShardingRpc,
	FokosShardingRuntime,
	FokosStartImportRequest,
	FokosStatusPage,
	FokosStatusRequest,
} from "fokosdb/sharding";

/**
 * The base class of a Durable Object that hosts `FokosShardingRuntime`.
 *
 * The runtime calls these RPC methods on the other partitions of the same class, and each method
 * sends the call to the runtime. A host class extends this class, creates `fokos` in its
 * constructor, and adds its own operations.
 */
export abstract class ShardedDurableObject<TPolicy, Ops extends FokosOperationSpec> extends DurableObject<Env> implements FokosShardingRpc {
	abstract readonly fokos: FokosShardingRuntime<TPolicy, Ops>;

	async fokosInit(req: FokosInitRequest): Promise<void> {
		return this.fokos.fokosInit(req);
	}

	async fokosStartImport(req: FokosStartImportRequest): Promise<void> {
		return this.fokos.fokosStartImport(req);
	}

	async fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage> {
		return this.fokos.fokosMigrationPull(req);
	}

	async fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void> {
		return this.fokos.fokosMigrationAck(req);
	}

	async fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<FokosEnvelope<unknown>> {
		return this.fokos.fokosExecuteLocal(req);
	}

	async fokosRequestPromotion(req: FokosRequestPromotionRequest): Promise<FokosRequestPromotionResult> {
		return this.fokos.fokosRequestPromotion(req);
	}

	async fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage> {
		return this.fokos.fokosStatus(req);
	}

	async fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		return this.fokos.fokosPrepareDestroy(req);
	}

	async fokosDestroy(): Promise<void> {
		return this.fokos.fokosDestroy();
	}

	async alarm(info: AlarmInvocationInfo): Promise<void> {
		return this.fokos.alarm(info);
	}
}
