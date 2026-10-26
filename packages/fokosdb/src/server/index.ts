/**
 * Server entry point: the Durable Object classes.
 *
 * A Worker that uses them must re-export each from its own entry module, otherwise wrangler cannot
 * find the classes its bindings name.
 */
export { FokosDBPartitionDO } from "./do-partition.js";
export type { PartitionOps, PartitionRpc } from "./do-partition.js";

export { FokosDBTransactionCoordinatorDO } from "./do-transaction-coordinator.js";

export { FokosDBMaterializationDO } from "./materialization/do-materialization.js";
export type {
	ApplyLatestImagesRequest,
	ApplyLatestImagesResponse,
	FokosTargetDefinition,
	GsiTargetDefinition,
	LatestImage,
	MaterializationOps,
	PushSinkTargetDefinition,
	PushTargetBatch,
	PushTargetItem,
	PushTargetResult,
} from "./materialization/do-materialization.js";
/** The admin Durable Object of the topology. Bind it, and name the binding in the `nsAdmin` table option. */
export { FokosDBAutoAdminDO } from "./do-auto-admin.js";
/** The cached topology entrypoint. Re-export it, enable its cache, and name it in the `topologyEntrypoint` table option. */
export { FokosDBTopologyCache } from "./topology-entrypoint.js";
