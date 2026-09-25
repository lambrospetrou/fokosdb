import type { Context } from "hono";
import { isDestroyAbortError, type FokosPartitionRef, type FokosPublicRouting, type FokosRouter, type FokosWalkStub } from "fokosdb/sharding";

/** The types and helpers that the three demos share. The UI reads the types as JSON. */

/** One partition that served a request. */
export type RouteHop = { doName: string; role: string };

/** The partitions that served one request, and the number of forwards between them. */
export type ActionTrace = { servedBy: RouteHop[]; forwardCount: number };

/** One partition box in the topology view. */
export type TopologyItem = {
	id: string;
	doName: string;
	role: "router" | "leaf";
	kind: "hash" | "range";
	/** The repartition state of this partition, or "active". */
	status: string;
	importState: string | null;
	/** A short text that the box shows, for example the tenants of a partition. */
	label: string | null;
	itemCount: number;
	requestCount?: number;
	children: TopologyItem[];
};

export type TileTopology = {
	tile: string;
	roots: TopologyItem[];
	summary: { totalPartitions: number; routerCount: number; leafCount: number };
	reconciliation?: { acknowledgedWrites: number; presentWrites: number; reconciled: boolean };
};

/** The JSON body of an action request. An action without parameters sends no body. */
export async function jsonBody<T>(c: Context): Promise<Partial<T>> {
	return (await c.req.json().catch(() => ({}))) as Partial<T>;
}

export type TreeNode<S> = { ref: FokosPartitionRef; stats: S };

/** Reads every partition of one tree. A parent comes before its children in the list. */
export async function collectTree<S extends { children: FokosPartitionRef[] }>(
	root: FokosPartitionRef,
	read: (doName: string) => Promise<S>,
): Promise<TreeNode<S>[]> {
	const nodes: TreeNode<S>[] = [];
	const walk = async (ref: FokosPartitionRef): Promise<void> => {
		const stats = await read(ref.doName);
		nodes.push({ ref, stats });
		for (const child of stats.children) await walk(child);
	};
	await walk(root);
	return nodes;
}

/** Counts the partitions of the trees that the UI draws. */
export function summaryOf(roots: TopologyItem[]): TileTopology["summary"] {
	const all: TopologyItem[] = [];
	const add = (item: TopologyItem) => {
		all.push(item);
		item.children.forEach(add);
	};
	roots.forEach(add);
	const routerCount = all.filter((i) => i.role === "router").length;
	return { totalPartitions: all.length, routerCount, leafCount: all.length - routerCount };
}

/** Makes the topology that the UI draws from the list of `collectTree`. The first node is the root. */
export function topologyOf<S extends { children: FokosPartitionRef[] }>(
	tile: string,
	nodes: TreeNode<S>[],
	item: (node: TreeNode<S>) => Omit<TopologyItem, "id" | "doName" | "children">,
): TileTopology {
	const build = (node: TreeNode<S>): TopologyItem => ({
		id: node.ref.partitionId,
		doName: node.ref.doName,
		...item(node),
		children: nodes.filter((n) => node.stats.children.some((ch) => ch.doName === n.ref.doName)).map(build),
	});
	const roots = [build(nodes[0])];
	return { tile, roots, summary: summaryOf(roots) };
}

/**
 * Destroys every partition of the tree with `fokosDestroy`, in the same order as `FokosDB.destroy()`.
 * The walk fences each partition, reads its children from its storage, and destroys the children
 * before the parent. Thus a reset that stops halfway can run again, because each parent that remains
 * still knows its children. Every error other than the abort error stops the reset.
 */
export async function resetTree<TPolicy, S extends FokosWalkStub & { fokosDestroy(): Promise<void> }>(
	router: FokosRouter<TPolicy>,
	stub: (doName: string) => S,
): Promise<void> {
	await router.walk(
		(_ctx, doName) => stub(doName),
		async (_ctx, s) => {
			try {
				await s.fokosDestroy();
			} catch (e) {
				if (!isDestroyAbortError(e)) throw e;
			}
		},
	);
}

/** How long a write waits for an import. It must be longer than the 5-second fallback alarm of the runtime. */
const RETRY_FOR_MS = 15_000;
const RETRY_EVERY_MS = 100;

/**
 * Sends a write again while the target partition answers `partition_migrating`. A partition that
 * imports refuses writes, and the runtime moves the import on by itself. After a kill, the fallback
 * alarm of the runtime starts the partition again within 5 seconds.
 */
export async function retryWhileMigrating<T>(send: () => Promise<T>): Promise<T> {
	const deadline = Date.now() + RETRY_FOR_MS;
	for (;;) {
		try {
			return await send();
		} catch (err) {
			if (Date.now() > deadline || !String(err).includes("partition_migrating")) throw err;
			await scheduler.wait(RETRY_EVERY_MS);
		}
	}
}

/** The hops of one request. A router does not list itself in `servedBy`, so this adds the entry partition. */
export function traceOf(entry: FokosPartitionRef, routing: FokosPublicRouting): ActionTrace {
	const executors = routing.servedBy.map((s) => ({ doName: s.ref.doName, role: s.role }));
	const hops = routing.forwardCount > 0 ? [{ doName: entry.doName, role: "router" }, ...executors] : executors;
	return { servedBy: hops, forwardCount: routing.forwardCount };
}
