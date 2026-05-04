// Primitive types shared across partition-topology modules.
// This file must not import from partition-topology.ts to avoid circular dependencies.

// PartitionNodeId is an opaque identifier for a partition node in the topology.
// It is only used within the topology logic and should not be interpreted by external code.
export type PartitionNodeId = string;

export type SplitType = "hash" | "range";
export type SplitStatus = "split_queued" | "split_partitions_initialized" | "split_started" | "split_completed";

export type PartitionTopologyEncoded = string;

/**
 * A node in the partition topology tree. Root nodes represent the initial set of partitions;
 * child nodes are created when a partition splits.
 */
export type TopologyNode = {
	partitionId: PartitionNodeId;
	partitionContext: {
		doName: string;
		primaryDoIdStr: string;
	};
	children: TopologyNode[];
};

export type TopologyTree = TopologyNode[];

/**
 * The authoritative topology state stored by TopologyKeeperDO.
 */
export type TopologyKVItem = {
	schema: 1;
	roots: TopologyTree;
	createdAt: number;
	updatedAt: number;
};

/**
 * Input for TopologyKeeperDO.registerSplit — called by a partition DO after it has initialized its children.
 */
export type RegisterSplitOptions = {
	parentPartitionId: PartitionNodeId;
	childPartitions: Array<{
		partitionId: PartitionNodeId;
		doName: string;
		primaryDoIdStr: string;
	}>;
	splitType: SplitType;
};
