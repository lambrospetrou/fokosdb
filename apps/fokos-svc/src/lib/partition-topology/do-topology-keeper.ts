import { DurableObject, env } from "cloudflare:workers";
import { PartitionContext, __encodePartitionIdOpaque } from "./partition-topology.js";
import type { RegisterSplitOptions, TopologyKVItem, TopologyNode } from "./types.js";

/**
 * TopologyKeeperDO is the authoritative store for the partition topology tree.
 * It is a single instance per logical database and tracks how partitions have been split over time.
 *
 * This is NOT the source of truth for the partitions. The partitions themselves are!
 * This is only to propagate partition splits to other DOs and the Workers (e.g. routers)
 * that need to be aware of topology changes for routing decisions.
 *
 * Callers:
 *   - initTopology: called once per Partition.
 *   - registerSplit: called by a PartitionDO after it has successfully initialized its child partitions.
 *   - getTopology: called by routers to get the latest topology for routing decisions.
 */
export class TopologyKeeperDO extends DurableObject<Env> {
	private static readonly KV_KEYS = {
		TOPOLOGY: "__topology",
		BASE_PARTITION_CONTEXT: "__base_partition_context",
	};

	#_topology?: TopologyKVItem;
	#_baseContext?: PartitionContext;

	constructor(ctx: DurableObjectState, e: Env) {
		super(ctx, e);
		void ctx.blockConcurrencyWhile(async () => {
			const stored = ctx.storage.kv.get<TopologyKVItem>(TopologyKeeperDO.KV_KEYS.TOPOLOGY);
			if (stored) {
				this.#_topology = stored;
			}
			const storedCtx = ctx.storage.kv.get<PartitionContext>(TopologyKeeperDO.KV_KEYS.BASE_PARTITION_CONTEXT);
			if (storedCtx) {
				this.#_baseContext = storedCtx;
			}
		});
	}

	/**
	 * Initializes the topology with one root node per rootTreesN. Idempotent if called with the same configuration.
	 *
	 * This should be called before any splits. It can be called asynchronously at any point by any partition,
	 * so that we make it easy for users to just start using the database.
	 *
	 * Each partition will asynchronously call initTopology during its startup at least once,
	 * but only the first one will actually initialize the topology.
	 */
	async initTopology(baseContext: PartitionContext): Promise<TopologyKVItem> {
		if (this.#_topology) {
			if (this.#_baseContext?.signature !== baseContext.signature) {
				throw new Error(
					`fokos/topology-keeper: Topology already initialized with a different configuration: ${this.#_baseContext?.signature} vs ${baseContext.signature}`,
				);
			}
			return this.#_topology;
		}

		const roots: TopologyNode[] = Array.from({ length: baseContext.rootTreesN }, (_, i) => {
			const doName = `${baseContext.nsPrefix}.r.${i}`;
			const doId = env[baseContext.ns].idFromName(doName);
			return {
				partitionId: __encodePartitionIdOpaque({ hashIdxs: [i] }),
				partitionContext: {
					doName: doId.name!,
					primaryDoIdStr: doId.toString(),
				},
				children: [],
			};
		});

		const now = Date.now();
		const topology: TopologyKVItem = {
			schema: 1,
			roots,
			createdAt: now,
			updatedAt: now,
		};

		this.#_baseContext = baseContext;
		this.#_topology = topology;
		this.ctx.storage.kv.put<PartitionContext>(TopologyKeeperDO.KV_KEYS.BASE_PARTITION_CONTEXT, baseContext);
		this.ctx.storage.kv.put<TopologyKVItem>(TopologyKeeperDO.KV_KEYS.TOPOLOGY, topology);
		return topology;
	}

	/**
	 * Returns the current topology. Throws if initTopology has not been called.
	 */
	async getTopology(): Promise<TopologyKVItem> {
		if (!this.#_topology) {
			throw new Error("fokos/topology-keeper: Topology not initialized. Call initTopology first.");
		}
		return this.#_topology;
	}

	/**
	 * Records that a partition has been split, attaching the child nodes under the parent in the tree.
	 * Idempotent: calling again with the same children is a no-op.
	 */
	async registerSplit(opts: RegisterSplitOptions): Promise<TopologyKVItem> {
		if (!this.#_topology) {
			throw new Error("fokos/topology-keeper: Topology not initialized.");
		}

		const { parentPartitionId, childPartitions } = opts;
		const roots = structuredClone(this.#_topology.roots);
		const parentNode = findNode(roots, parentPartitionId);
		if (!parentNode) {
			throw new Error(`fokos/topology-keeper: Parent partition not found in topology: ${parentPartitionId}`);
		}

		if (parentNode.children.length > 0) {
			const existingIds = new Set(parentNode.children.map((c) => c.partitionId));
			const incomingIds = new Set(childPartitions.map((c) => c.partitionId));
			if (existingIds.size === incomingIds.size && [...incomingIds].every((id) => existingIds.has(id))) {
				return this.#_topology;
			}
			throw new Error(
				`fokos/topology-keeper: Parent partition ${parentPartitionId} already has children with different IDs. Conflicting split registration.`,
			);
		}

		parentNode.children = childPartitions.map((child) => ({
			partitionId: child.partitionId,
			partitionContext: {
				doName: child.doName,
				primaryDoIdStr: child.primaryDoIdStr,
			},
			children: [],
		}));

		const updated: TopologyKVItem = {
			...this.#_topology,
			roots,
			updatedAt: Date.now(),
		};

		this.#_topology = updated;
		this.ctx.storage.kv.put<TopologyKVItem>(TopologyKeeperDO.KV_KEYS.TOPOLOGY, updated);
		return updated;
	}

	async status() {
		return {
			topology: this.#_topology,
			basePartitionContext: this.#_baseContext,
		};
	}

	async __internalState() {
		return {
			topology: this.ctx.storage.kv.get<TopologyKVItem>(TopologyKeeperDO.KV_KEYS.TOPOLOGY),
			basePartitionContext: this.ctx.storage.kv.get<PartitionContext>(TopologyKeeperDO.KV_KEYS.BASE_PARTITION_CONTEXT),
		};
	}

	private logParams() {
		return {
			actorId: this.ctx.id.toString(),
			actorName: this.ctx.id.name,
		};
	}
}

// FIXME: This is slow, but we won't need it once we migrate to Succinct Trees for the topology representation.
function findNode(nodes: TopologyNode[], partitionId: string): TopologyNode | undefined {
	for (const node of nodes) {
		if (node.partitionId === partitionId) {
			return node;
		}
		const found = findNode(node.children, partitionId);
		if (found) {
			return found;
		}
	}
	return undefined;
}
