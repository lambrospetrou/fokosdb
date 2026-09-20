/**
 * The public `PartitionInfo` of a result, built from the routing of its envelope. This is where
 * partition-to-partition routing state stops: the envelope lists every partition that served the
 * request with the hints a router caches, and a client needs the partition that ran the work.
 */
import type { OperationMetrics, PartitionInfo } from "../shared/types.js";
import type { FokosPublicRoute, FokosPublicRouting } from "../sharding/runtime-types.js";

/**
 * The partition that executed an item RPC, with the RPC count of the whole response tree. A point
 * path has one executor; on a read-through it is the source, which scanned the rows for the owner.
 */
export function partitionInfoOf(routing: FokosPublicRouting): PartitionInfo {
	const executor = routing.servedBy.find((node) => node.role === "executed") ?? routing.servedBy[0];
	return routeInfo(executor, routing.forwardCount);
}

/**
 * The info of one leaf that scanned rows, paired by `partitionId` with the list of the envelope. A
 * leaf forwarded nothing, so its count is 0. The list is a bounded hint, so it can miss a leaf; the
 * caller then skips it instead of reporting a partition it cannot name.
 */
export function leafPartitionInfo<M extends OperationMetrics & { partitionId: string }>(
	leaf: M,
	routing: FokosPublicRouting,
): (OperationMetrics & PartitionInfo) | undefined {
	const { partitionId, ...metrics } = leaf;
	const node = routing.servedBy.find((n) => n.ref.partitionId === partitionId);
	return node && { ...metrics, ...routeInfo(node, 0) };
}

function routeInfo(node: FokosPublicRoute | undefined, forwardCount: number): PartitionInfo {
	return {
		servedByActorId: node?.actorId ?? "",
		servedByActorName: node?.ref.doName ?? "",
		servedByPartitionId: node?.ref.partitionId ?? "",
		forwardCount,
		hashDepth: node?.hashDepth ?? 0,
		rangeDepth: node?.rangeDepth ?? 0,
	};
}
