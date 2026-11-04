import { PartitionIdHelper, topologyEventToJson, type FokosTopologyEvent, type KeyBytes } from "../../src/sharding/index-server.js";
import { routingHashOf, type RoutingHash } from "../../src/sharding/hash-primitives.js";
import type { FokosCompositeKey } from "../../src/sharding/key-codec.js";

/**
 * Topology reports as a partition sends them, in their JSON form. A test registers them with a keeper
 * without a real repartition.
 */

let nextId = 0;

function wire(event: FokosTopologyEvent): unknown {
	return JSON.parse(topologyEventToJson(event));
}

function common(shardGroup: string, sourceDoName: string, completedAt: number, rootTreesN: number, hashSplitN: number) {
	return {
		schema: 1 as const,
		id: `${sourceDoName}/r${++nextId}`,
		shardGroup,
		rootTreesN,
		hashSplitN,
		actorId: `actor-of-${sourceDoName}`,
		completedAt,
	};
}

/** The report of a hash split of the partition at `root` and `path`. */
export function hashSplitReport(
	shardGroup: string,
	topology: { rootTreesN: number; hashSplitN: number },
	root: number,
	path: number[],
	completedAt = Date.now(),
): unknown {
	const source = PartitionIdHelper.hashId(shardGroup, root, path);
	return wire({
		...common(shardGroup, source.doName, completedAt, topology.rootTreesN, topology.hashSplitN),
		kind: "hash_split",
		source: { partitionId: source.opaque, doName: source.doName },
		targets: Array.from({ length: topology.hashSplitN }, (_, c) => {
			const target = PartitionIdHelper.hashId(shardGroup, root, [...path, c]);
			return {
				ref: { partitionId: target.opaque, doName: target.doName },
				slice: { kind: "hash_child", childIndex: c, depth: path.length + 1 },
				actorId: `actor-of-${target.doName}`,
			};
		}),
	});
}

function rangeRef(shardGroup: string, routingHash: RoutingHash, start: FokosCompositeKey | null, end: FokosCompositeKey | null) {
	const { opaque, doName } = PartitionIdHelper.fromRangePartition(shardGroup, routingHash, start, end).encode(true);
	return { partitionId: opaque, doName: doName! };
}

/** The report of the promotion of the routingHash of `hashKey` from the hash root 0. */
export function promotionReport(shardGroup: string, hashKey: KeyBytes, completedAt = Date.now(), rootTreesN = 1, hashSplitN = 4): unknown {
	return routingHashPromotionReport(shardGroup, routingHashOf(hashKey, shardGroup), completedAt, rootTreesN, hashSplitN);
}

/** The report of the promotion of `routingHash` from the hash root 0. */
export function routingHashPromotionReport(
	shardGroup: string,
	routingHash: RoutingHash,
	completedAt = Date.now(),
	rootTreesN = 1,
	hashSplitN = 4,
): unknown {
	const source = PartitionIdHelper.hashId(shardGroup, 0);
	const target = rangeRef(shardGroup, routingHash, null, null);
	return wire({
		...common(shardGroup, source.doName, completedAt, rootTreesN, hashSplitN),
		kind: "key_promotion",
		source: { partitionId: source.opaque, doName: source.doName },
		targets: [{ ref: target, slice: { kind: "promoted_routing_hash", routingHash }, actorId: `actor-of-${target.doName}` }],
	});
}

/** The report of the range split of `[start, end)` of the routingHash of `hashKey` at the inner `boundaries`. */
export function rangeSplitReport(
	shardGroup: string,
	hashKey: KeyBytes,
	start: FokosCompositeKey | null,
	end: FokosCompositeKey | null,
	boundaries: FokosCompositeKey[],
	completedAt = Date.now(),
): unknown {
	const routingHash = routingHashOf(hashKey, shardGroup);
	const source = rangeRef(shardGroup, routingHash, start, end);
	const edges = [start, ...boundaries, end];
	return wire({
		...common(shardGroup, source.doName, completedAt, 1, 4),
		kind: "range_split",
		source,
		targets: Array.from({ length: boundaries.length + 1 }, (_, i) => {
			const ref = rangeRef(shardGroup, routingHash, edges[i], edges[i + 1]);
			return { ref, slice: { kind: "range", routingHash, start: edges[i], end: edges[i + 1] }, actorId: `actor-of-${ref.doName}` };
		}),
	});
}
