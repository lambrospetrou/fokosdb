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

	/** `meta` combines the partition that executed the request with the operation metrics of the value. */
	static meta(metrics: OperationMetrics, node: FokosPublicRoute, forwardCount: number): PublicMeta {
		return {
			...metrics,
			servedByActorId: node.actorId,
			servedByActorName: node.ref.doName,
			servedByPartitionId: node.ref.partitionId,
			hashDepth: node.hashDepth,
			rangeDepth: node.rangeDepth,
			forwardCount,
		};
	}

	static executor(routing: FokosPublicRouting): FokosPublicRoute {
		return routing.servedBy.find((node) => node.role === "executed") ?? todo("an item RPC has one executing partition");
	}

	async getItem(hashKey: KeyBytes, sortKey: KeyBytes) {
		const ctx = this.partitions.rootContext(hashKey);
		const { value, routing } = this.partitions.unwrap(await this.partitionStub(ctx).apiGetItem(ctx, { hashKey, sortKey }));
		return { ...value, meta: FokosDB.meta(value.meta, FokosDB.executor(routing), routing.forwardCount) };
	}

	/** One sub-query page. The leaf metrics pair with the list by partition id; a leaf the cap dropped is skipped. */
	async queryPage(req: QueryReq) {
		const ctx = this.partitions.rootContext(req.hashKey);
		const { value, routing } = this.partitions.unwrap(await this.partitionStub(ctx).apiQueryItems(ctx, req));
		const byPartition = new Map(routing.servedBy.map((node) => [node.ref.partitionId, node]));
		const partitionMetas = value.partitionMetas.flatMap((leaf: LeafMetrics) => {
			const node = byPartition.get(leaf.partitionId);
			return node ? [FokosDB.meta(leaf, node, 0)] : [];
		});
		return { ...value, partitionMetas, forwardCount: routing.forwardCount };
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
			async (_ctx, stub) => await (stub as unknown as PartitionRpc).fokosDestroy(),
		);
		await this.coordinators.walk(
			(ctx, doName) => todo(`txCoordinatorStubByName(${ctx.policy.nsTx}, ${doName})`),
			async (_ctx, stub) => await (stub as unknown as CoordinatorRpc).fokosDestroy(),
		);
	}
}
