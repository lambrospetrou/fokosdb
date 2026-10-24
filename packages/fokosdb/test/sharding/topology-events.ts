import { PartitionIdHelper, topologyEventToJson, type FokosTopologyEvent, type KeyBytes } from "../../src/sharding/index-server.js";

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

function rangeRef(shardGroup: string, hashKey: KeyBytes, start: KeyBytes | null, end: KeyBytes | null) {
	const { opaque, doName } = PartitionIdHelper.fromRangePartition(shardGroup, hashKey, start, end).encode(true);
	return { partitionId: opaque, doName: doName! };
}

/** The report of the promotion of `hashKey` from the hash root 0. */
export function promotionReport(shardGroup: string, hashKey: KeyBytes, completedAt = Date.now(), rootTreesN = 1, hashSplitN = 4): unknown {
	const source = PartitionIdHelper.hashId(shardGroup, 0);
	const target = rangeRef(shardGroup, hashKey, null, null);
	return wire({
		...common(shardGroup, source.doName, completedAt, rootTreesN, hashSplitN),
		kind: "key_promotion",
		source: { partitionId: source.opaque, doName: source.doName },
		targets: [{ ref: target, slice: { kind: "promoted_key", hashKey }, actorId: `actor-of-${target.doName}` }],
	});
}

/** The report of the range split of `[start, end)` of `hashKey` at the inner `boundaries`. */
export function rangeSplitReport(
	shardGroup: string,
	hashKey: KeyBytes,
	start: KeyBytes | null,
	end: KeyBytes | null,
	boundaries: KeyBytes[],
	completedAt = Date.now(),
): unknown {
	const source = rangeRef(shardGroup, hashKey, start, end);
	const edges = [start, ...boundaries, end];
	return wire({
		...common(shardGroup, source.doName, completedAt, 1, 4),
		kind: "range_split",
		source,
		targets: Array.from({ length: boundaries.length + 1 }, (_, i) => {
			const ref = rangeRef(shardGroup, hashKey, edges[i], edges[i + 1]);
			return { ref, slice: { kind: "range", hashKey, start: edges[i], end: edges[i + 1] }, actorId: `actor-of-${ref.doName}` };
		}),
	});
}
