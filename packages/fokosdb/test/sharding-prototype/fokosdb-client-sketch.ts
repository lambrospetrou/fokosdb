/**
 * PROTOTYPE. The `db.ts` side: two routers (partitions and coordinators), the envelope unwrap, and the public
 * `meta`. Only the paths that touch the sharding surface are written.
 */
import { encodeHashKey } from "../../src/shared/transaction-limits.js";
import type { OperationMetrics, PartitionInfo } from "../../src/shared/types.js";
import { FokosRouter, todo } from "./api.js";
import type { FokosPublicRoute, FokosPublicRouting, FokosRangeConfig, FokosTopology, KeyBytes } from "./api.js";
import type { CoordinatorRpc, InitiateWriteReq } from "./fokosdb-coordinator-host.js";
import type { FokosDbPolicy, FokosDbRouteContext, LeafMetrics, PartitionRpc, QueryReq } from "./fokosdb-partition-host.js";

export type FokosDBOptions = {
	tableName: string;
	rootTreesN: number;
	hashSplitN: number;
	jurisdiction?: DurableObjectJurisdiction;
	rangeConfig: FokosRangeConfig;
	policy: FokosDbPolicy;
	/** Root count of the coordinator pool. It grows by hash splits from there, so retries need no fixed pool size. */
	coordinatorRootsN?: number;
};

/** What `meta` becomes: the operation metrics plus the public routing of the envelope. */
export type PublicMeta = OperationMetrics & PartitionInfo;

export class FokosDB {
	readonly partitions: FokosRouter<FokosDbPolicy>;
	readonly coordinators: FokosRouter<FokosDbPolicy>;

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

	partitionStub(ctx: FokosDbRouteContext): PartitionRpc {
		return todo(`partitionStubByName(${ctx.policy.ns}, ${ctx.doName})`);
	}
	coordinatorStub(ctx: FokosDbRouteContext): CoordinatorRpc {
		return todo(`txCoordinatorStubByName(${ctx.policy.nsTx}, ${ctx.doName})`);
	}

	/** `meta` combines the serving partition of the envelope with the operation metrics of the value. */
	static meta(metrics: OperationMetrics, routing: FokosPublicRouting): PublicMeta {
		return {
			...metrics,
			servedByActorId: routing.summary.servedByActorId,
			servedByActorName: routing.summary.servedBy.doName,
			servedByPartitionId: routing.summary.servedBy.partitionId,
			hashDepth: routing.summary.hashDepth,
			rangeDepth: routing.summary.rangeDepth,
			forwardCount: routing.forwardCount,
		};
	}

	async getItem(hashKey: KeyBytes, sortKey: KeyBytes) {
		const ctx = this.partitions.rootContext(hashKey);
		const { value, routing } = this.partitions.unwrap(await this.partitionStub(ctx).apiGetItem(ctx, { hashKey, sortKey }));
		return { ...value, meta: FokosDB.meta(value.meta, routing) };
	}

	/** One sub-query page. The leaf metrics pair with the route list by partition id. */
	async queryPage(req: QueryReq) {
		const ctx = this.partitions.rootContext(req.hashKey);
		const { value, routing } = this.partitions.unwrap(await this.partitionStub(ctx).apiQueryItems(ctx, req));
		const byPartition = new Map(routing.routes.map((r) => [r.servedBy.partitionId, r]));
		const partitionMetas = value.partitionMetas.map((leaf: LeafMetrics) => {
			const route = byPartition.get(leaf.partitionId) ?? todo<FokosPublicRoute>("a leaf that scanned rows is always in the route list");
			return FokosDB.meta(leaf, { summary: route, routes: [], forwardCount: 0 });
		});
		return { ...value, partitionMetas, meta: FokosDB.meta({ rowsRead: 0, rowsWritten: 0, databaseSize: 0 }, routing) };
	}

	/** The coordinator is chosen by the token, as before, and reached through its root. */
	async transactWrite(req: InitiateWriteReq & { clientRequestToken: string }) {
		const ctx = this.coordinators.rootContext(encodeHashKey(req.clientRequestToken));
		const { value } = this.coordinators.unwrap(await this.coordinatorStub(ctx).initiateWrite(ctx, req));
		return value;
	}

	/** Destroy walks both shard groups. The fence comes first in each. */
	async destroy(): Promise<void> {
		await this.partitions.walk(
			(ctx, doName) => todo(`partitionStubByName(${ctx.policy.ns}, ${doName})`),
			async (_ctx, stub) => (stub as unknown as PartitionRpc).fokosDestroy(),
		);
		await this.coordinators.walk(
			(ctx, doName) => todo(`txCoordinatorStubByName(${ctx.policy.nsTx}, ${doName})`),
			async (_ctx, stub) => (stub as unknown as CoordinatorRpc).fokosDestroy(),
		);
	}
}
