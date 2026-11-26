import { PartitionIdHelper, topologyEventToJson, type FokosTopologyEvent, type KeyBytes } from "../../src/sharding/index-server.js";
import { routingHashOf, type RoutingHash } from "../../src/sharding/hash-primitives.js";
import type { FokosCompositeKey } from "../../src/sharding/key-codec.js";
import { testRoutingHashSeedResolver } from "../routing-hash.js";

/**
 * Topology reports as a partition sends them, in their JSON form. A test registers them with a keeper
 * without a real repartition.
 */

let nextId = 0;

function wire(event: FokosTopologyEvent): unknown {
	return JSON.parse(topologyEventToJson(event));
}

function common(shardGroup: string, sourcePartitionId: string, completedAt: number, rootTreesN: number, hashSplitN: number) {
	return {
		schema: 1 as const,
		id: `${sourcePartitionId}/r${++nextId}`,
		shardGroup,
		rootTreesN,
		hashSplitN,
		sourcePartitionId,
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
		...common(shardGroup, source.opaque, completedAt, topology.rootTreesN, topology.hashSplitN),
		kind: "hash_split",
		targets: Array.from({ length: topology.hashSplitN }, (_, c) => ({
			partitionId: PartitionIdHelper.hashId(shardGroup, root, [...path, c]).opaque,
			slice: { kind: "hash_child", childIndex: c, depth: path.length + 1 },
		})),
	});
}

function rangePartitionId(shardGroup: string, routingHash: RoutingHash, start: FokosCompositeKey | null, end: FokosCompositeKey | null) {
	return PartitionIdHelper.fromRangePartition(shardGroup, routingHash, start, end).encode(false).opaque;
}

/** The report of the promotion of the routingHash of `hashKey` from the hash root 0. */
export function promotionReport(shardGroup: string, hashKey: KeyBytes, completedAt = Date.now(), rootTreesN = 1, hashSplitN = 4): unknown {
	return routingHashPromotionReport(
		shardGroup,
		routingHashOf(hashKey, shardGroup, testRoutingHashSeedResolver),
		completedAt,
		rootTreesN,
		hashSplitN,
	);
}

/** The report of the promotion of `routingHash` from the hash root 0. */
export function routingHashPromotionReport(
	shardGroup: string,
	routingHash: RoutingHash,
	completedAt = Date.now(),
	rootTreesN = 1,
	hashSplitN = 4,
): unknown {
	return wire({
		...common(shardGroup, PartitionIdHelper.hashId(shardGroup, 0).opaque, completedAt, rootTreesN, hashSplitN),
		kind: "key_promotion",
		targets: [
			{ partitionId: rangePartitionId(shardGroup, routingHash, null, null), slice: { kind: "promoted_routing_hash", routingHash } },
		],
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
	const routingHash = routingHashOf(hashKey, shardGroup, testRoutingHashSeedResolver);
	const edges = [start, ...boundaries, end];
	return wire({
		...common(shardGroup, rangePartitionId(shardGroup, routingHash, start, end), completedAt, 1, 4),
		kind: "range_split",
		targets: Array.from({ length: boundaries.length + 1 }, (_, i) => ({
			partitionId: rangePartitionId(shardGroup, routingHash, edges[i], edges[i + 1]),
			slice: { kind: "range", routingHash, start: edges[i], end: edges[i + 1] },
		})),
	});
}
