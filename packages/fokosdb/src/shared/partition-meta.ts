/**
 * The public `meta` entries of the partitions that ran a request. A partition writes its own entry
 * into its answer, with no name. The name comes from the routing of the envelope, when the node of
 * that actor is still in the list: the byte cap of the list can drop it. This is where the routing
 * state stops: the internal hints of the routing never reach a caller.
 */
import type { PartitionMeta, WirePartitionMeta } from "./types.js";
import { isRangePartition } from "../sharding/partition-id.js";
import type { FokosPublicRoute, FokosPublicRouting } from "../sharding/runtime-types.js";

/**
 * The public `meta` of an answer from the entries that its partitions wrote, and the routing of its
 * envelope. An entry gets its name when the routing still has the node of the same actor.
 */
export function answerMeta<E extends WirePartitionMeta>(
	partitions: readonly E[],
	routing: FokosPublicRouting,
): { forwardCount: number; partitions: Array<Omit<E, "actorId"> & PartitionMeta> } {
	const names = new Map(routing.servedBy.map((node) => [node.actorId, node.ref.doName]));
	return {
		forwardCount: routing.forwardCount,
		partitions: partitions.map(({ actorId, ...rest }) => {
			const name = names.get(actorId);
			return { servedByActorId: actorId, ...(name === undefined ? {} : { servedByActorName: name }), ...rest };
		}),
	};
}

/**
 * The entry of the partition that ran a request with no answer list: the executor of a result, or
 * the raiser of an error. `RouteCollector.build` always keeps that node, so the entry always has the
 * name. The node carries both depths, and the entry keeps only the depth of the tree of the partition.
 */
export function routeNodeMeta(node: FokosPublicRoute): PartitionMeta {
	const depth = isRangePartition(node.ref) ? { rangeDepth: node.rangeDepth } : { hashDepth: node.hashDepth };
	return { servedByActorId: node.actorId, servedByActorName: node.ref.doName, ...depth };
}

/**
 * Joins the entries of many answers into one list with one entry for each actor ID. The counts of
 * the answers of one partition add up. `databaseSize` is a size and not a count, so the latest value
 * stays. An entry keeps its first index, so a later answer never moves an earlier entry.
 */
export class PartitionMetaJoin {
	readonly partitions: PartitionMeta[] = [];
	readonly #indexOf = new Map<string, number>();

	/** Adds one entry and returns its index in `partitions`. */
	add(entry: PartitionMeta): number {
		const index = this.#indexOf.get(entry.servedByActorId);
		if (index === undefined) {
			this.#indexOf.set(entry.servedByActorId, this.partitions.length);
			this.partitions.push({ ...entry });
			return this.partitions.length - 1;
		}
		// Neither entry holds an `undefined` value, so the spread keeps each field that one of them has.
		const held = this.partitions[index];
		const joined: PartitionMeta = { ...entry, ...held };
		for (const count of ["rowsRead", "rowsWritten"] as const) {
			if (held[count] !== undefined && entry[count] !== undefined) {
				joined[count] = held[count] + entry[count];
			}
		}
		if (entry.databaseSize !== undefined) {
			joined.databaseSize = entry.databaseSize;
		}
		this.partitions[index] = joined;
		return index;
	}
}
