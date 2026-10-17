/**
 * PROTOTYPE. The `db.ts` side: two routers (partitions and coordinators), the envelope unwrap, and the public
 * `meta`. Only the paths that touch the sharding surface are written.
 */
import { DEFAULT_LIMITS, encodeHashKey } from "../../src/shared/transaction-limits.js";
import { answerMeta } from "../../src/shared/partition-meta.js";
import { FokosRouter, FokosShardingClient, todo } from "./api.js";
import type { FokosRangeConfig, FokosTopology, KeyBytes } from "./api.js";
import type { CoordinatorRpc, InitiateWriteReq } from "./fokosdb-coordinator-host.js";
import type { FokosDBPolicy, FokosDBRouteContext, PartitionRpc, QueryReq } from "./fokosdb-partition-host.js";

export type FokosDBOptions = {
	tableName: string;
	rootTreesN: number;
	hashSplitN: number;
	jurisdiction?: DurableObjectJurisdiction;
	rangeConfig: FokosRangeConfig;
	policy: FokosDBPolicy;
	/** Root count of the coordinator pool. It grows by hash splits from there, so retries need no fixed pool size. */
	coordinatorRootsN?: number;
};

export class FokosDB {
	readonly partitions: FokosRouter<FokosDBPolicy>;
	readonly coordinators: FokosRouter<FokosDBPolicy>;

	constructor(opts: FokosDBOptions) {
		const topology: FokosTopology = {
			shardGroup: opts.tableName,
			rootTreesN: opts.rootTreesN,
			hashSplitN: opts.hashSplitN,
			jurisdiction: opts.jurisdiction,
		};
		const tcTopology: FokosTopology = {
			...topology,
			shardGroup: `fokos.tc.${opts.tableName}`,
			rootTreesN: opts.coordinatorRootsN ?? 2 * opts.rootTreesN,
		};
		// Both hosts share one policy type. A router is cheap, so one per request is also fine.
		this.partitions = new FokosRouter(topology, opts.rangeConfig, opts.policy);
		this.coordinators = new FokosRouter(tcTopology, opts.rangeConfig, opts.policy);
	}

	partitionStub(ctx: FokosDBRouteContext): PartitionRpc {
		return todo(`partitionStubByName(${ctx.policy.ns}, ${ctx.doName})`);
	}
	coordinatorStub(ctx: FokosDBRouteContext): CoordinatorRpc {
		return todo(`txCoordinatorStubByName(${ctx.policy.nsTx}, ${ctx.doName})`);
	}

	/** `meta` is the entry that the partition wrote in `partitions`, with the name from the routing of the envelope. */
	async getItem(hashKey: KeyBytes, sortKey: KeyBytes) {
		const ctx = this.partitions.rootContext(hashKey);
		const { value, routing } = this.partitions.unwrap(await this.partitionStub(ctx).apiGetItem(ctx, { hashKey, sortKey }));
		const { partitions, ...rest } = value;
		return { ...rest, meta: answerMeta(partitions, routing) };
	}

	/**
	 * One sub-query page. Each leaf wrote its own entry, so the byte cap of the routing cannot drop a leaf. A leaf
	 * whose node the cap dropped has no name.
	 */
	async queryPage(req: QueryReq) {
		const ctx = this.partitions.rootContext(req.hashKey);
		const { value, routing } = this.partitions.unwrap(await this.partitionStub(ctx).apiQueryItems(ctx, req));
		const { partitions, ...rest } = value;
		return { ...rest, meta: answerMeta(partitions, routing) };
	}

	/** The coordinator is chosen by the token, as before, and reached through its root. */
	async transactWrite(req: InitiateWriteReq & { clientRequestToken: string }) {
		const ctx = this.coordinators.rootContext(encodeHashKey(req.clientRequestToken, DEFAULT_LIMITS));
		const { value } = this.coordinators.unwrap(await this.coordinatorStub(ctx).initiateWrite(ctx, req));
		return value;
	}

	/** Destroy walks both shard groups. The fence comes first in each. */
	async destroy(): Promise<void> {
		const { topology, rangeConfig, policy } = this.partitions;
		await new FokosShardingClient({
			topology,
			rangeConfig,
			policy,
			stub: (ctx, doName) => todo(`partitionStubByName(${ctx.policy.ns}, ${doName})`),
		}).destroy();
		const coordinators = this.coordinators;
		await new FokosShardingClient({
			topology: coordinators.topology,
			rangeConfig: coordinators.rangeConfig,
			policy: coordinators.policy,
			stub: (ctx, doName) => todo(`txCoordinatorStubByName(${ctx.policy.nsTx}, ${doName})`),
		}).destroy();
	}
}
