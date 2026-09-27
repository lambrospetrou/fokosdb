/**
 * The response envelope: the partitions that served one request, collected while the request runs,
 * and the same list on an error.
 *
 * A partition that serves a scope adds its own node once. A partition that forwards merges the list
 * of the child envelope into its own by `ref.partitionId`, and adds one to `forwardCount` for the RPC.
 * The list is a cache hint for the caller, so a byte cap can drop nodes and change latency only;
 * `forwardCount` is never dropped.
 */
import { FokosError } from "../shared/errors.js";
import type { FokosEnvelope, FokosRouteNode, FokosRouting, FokosServedRole } from "./runtime-types.js";

/** The cap over the serialized `servedBy` list of one envelope, measured with `routeNodeBytes`. */
export const ROUTE_EVIDENCE_MAX_BYTES = 10 * 1024;

/**
 * A conservative serialized size for one node: fixed overhead for the field names and the small
 * numbers, 2 bytes for every character of the three identifiers, and the bytes of every ancestor
 * boundary. It must never under-count.
 */
export function routeNodeBytes(node: FokosRouteNode): number {
	let bytes = 128 + 2 * (node.ref.doName.length + node.ref.partitionId.length + node.actorId.length);
	for (const ancestor of node._rangeAncestors ?? []) {
		bytes += 24 + ancestor.startBoundary.byteLength + ancestor.endBoundary.byteLength;
	}
	return bytes;
}

/**
 * How much one role says about a partition. `executed` names the partition that ran the handler, which
 * is what a caller reports as the server of the request, so it outranks the two roles that a partition
 * takes when the rows were read somewhere else.
 */
const ROLE_RANK: Record<FokosServedRole, number> = { merged: 0, read_through: 1, executed: 2 };

/**
 * Collects the nodes of one request on one partition. One collector per `dispatch`, so this
 * partition's own node and the nodes of every forwarded envelope end in one list.
 */
export class RouteCollector {
	readonly #nodes = new Map<string, FokosRouteNode>();
	/** The partition ID of the node that `addRaiser` put at the head of the list. */
	#raiserId: string | undefined;
	#forwardCount = 0;
	#truncated = false;

	/**
	 * Adds one node. A partition is listed once, under the most informative role it took: a partition
	 * that merges remote parts and also runs the handler for its own scope is an executor, whichever
	 * of the two happened first. A weaker role never overwrites a stronger one.
	 */
	add(node: FokosRouteNode): void {
		const held = this.#nodes.get(node.ref.partitionId);
		if (!held || ROLE_RANK[node.role] > ROLE_RANK[held.role]) {
			this.#nodes.set(node.ref.partitionId, node);
		}
	}

	/** True while the list names no partition, so an error can still name the partition that raised it. */
	get isEmpty(): boolean {
		return this.#nodes.size === 0;
	}

	/**
	 * Lists `node` before every other, and keeps the stronger role when the list already holds this
	 * partition. The error path uses it: a caller reads the partition that raised the error out of the
	 * head of the list, and `build` always keeps the raiser. A fan-out that fails would otherwise lead
	 * with a group that answered, because a remote node lands while the router awaits.
	 */
	addRaiser(node: FokosRouteNode): void {
		this.#raiserId = node.ref.partitionId;
		const held = this.#nodes.get(node.ref.partitionId);
		const rest = [...this.#nodes];
		this.#nodes.clear();
		this.#nodes.set(node.ref.partitionId, held && ROLE_RANK[held.role] > ROLE_RANK[node.role] ? held : node);
		for (const [id, other] of rest) {
			if (id !== node.ref.partitionId) {
				this.#nodes.set(id, other);
			}
		}
	}

	/**
	 * Removes the node of a partition that refused a forward before any handler ran, when the sender
	 * resolves the request again. The forward still counts, because the RPC was sent.
	 */
	forget(partitionId: string): void {
		this.#nodes.delete(partitionId);
		if (this.#raiserId === partitionId) {
			this.#raiserId = undefined;
		}
	}

	/**
	 * Counts one outbound RPC that carries the request. The sender calls it before it awaits the RPC, so
	 * an RPC that fails without routing still counts.
	 */
	countForward(): void {
		this.#forwardCount += 1;
	}

	/**
	 * Merges the routing of one outbound RPC: its nodes and its forward count. `countForward` counts the
	 * RPC itself. `stamp` rewrites a node before the merge; a hash partition that enters a range tree
	 * uses it to write its own hash depth on the range nodes.
	 */
	mergeForwarded(routing: FokosRouting, stamp?: (node: FokosRouteNode) => FokosRouteNode): void {
		this.#forwardCount += routing.forwardCount;
		this.#truncated ||= routing.servedByTruncated;
		for (const node of routing.servedBy) {
			this.add(stamp ? stamp(node) : node);
		}
	}

	/**
	 * The routing of this partition's response, with the byte cap applied.
	 *
	 * One node is always kept, even when it alone crosses the cap: the raiser on an error, else the
	 * first node with the strongest role. So a response always names at least one partition. The cap
	 * then drops the other nodes in insertion order, and the output keeps the insertion order. A future
	 * change can rank the list before it cuts, so that the deepest owner and the executor survive a wide
	 * fan-out and the shallower routers go first: the deep nodes are the ones a caller caches. Dropping a
	 * node costs one more hop on a later request, and never a wrong count, because a caller aggregates
	 * its own per-partition metrics.
	 */
	build(): FokosRouting {
		const kept = this.#keptNode();
		const servedBy: FokosRouteNode[] = [];
		let bytes = kept ? routeNodeBytes(kept) : 0;
		let full = false;
		for (const node of this.#nodes.values()) {
			if (node === kept) {
				servedBy.push(node);
			} else if (!full) {
				bytes += routeNodeBytes(node);
				full = bytes > ROUTE_EVIDENCE_MAX_BYTES;
				if (!full) {
					servedBy.push(node);
				}
			}
		}
		return { servedBy, forwardCount: this.#forwardCount, servedByTruncated: this.#truncated || full };
	}

	#keptNode(): FokosRouteNode | undefined {
		const raiser = this.#raiserId === undefined ? undefined : this.#nodes.get(this.#raiserId);
		if (raiser) {
			return raiser;
		}
		let kept: FokosRouteNode | undefined;
		for (const node of this.#nodes.values()) {
			if (!kept || ROLE_RANK[node.role] > ROLE_RANK[kept.role]) {
				kept = node;
			}
		}
		return kept;
	}
}

/**
 * An error that carries the routing of the partition that raised it, as the own data property
 * `routing`, so the list crosses an RPC hop as it does on a result. Only `attachRouting` writes it.
 */
export type FokosRoutedError = FokosError & { routing: FokosRouting };

export function attachRouting(err: FokosError, routing: FokosRouting): FokosRoutedError {
	return Object.assign(err, { routing });
}

/** `e` as a `FokosRoutedError`, or undefined when it carries no routing. */
export function routedError(e: unknown): FokosRoutedError | undefined {
	return FokosError.is(e) && "routing" in e && e.routing !== undefined ? (e as FokosRoutedError) : undefined;
}

export function envelope<T>(value: T, routing: FokosRouting): FokosEnvelope<T> {
	return { value, routing };
}
