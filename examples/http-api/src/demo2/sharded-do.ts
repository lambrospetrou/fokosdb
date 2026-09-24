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

/** The message of the `ctx.abort()` in `resetAll`. The caller sees an error with this text. */
export const RESET_ABORT_MESSAGE = "demo reset";

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

	/**
	 * Demo control. Deletes all storage, then evicts the instance, so the next call starts empty.
	 * With a compatibility date of 2026-02-24 or later, `deleteAll()` also deletes the alarm.
	 * `retryAlarm: false` prevents a new run of an alarm that the abort interrupts.
	 *
	 * The reset uses this method, not `fokosDestroy`. `fokosDestroy` logs an object before
	 * `ctx.abort()`, and then the vitest pool for Workers does not exit after the tests.
	 */
	async resetAll(): Promise<void> {
		await this.ctx.storage.deleteAll();
		this.ctx.abort(RESET_ABORT_MESSAGE, { retryAlarm: false });
	}
}
