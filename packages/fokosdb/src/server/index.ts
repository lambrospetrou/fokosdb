/**
 * Server entry point: the Durable Object classes.
 *
 * A Worker that uses them must re-export them from its own entry module, otherwise wrangler cannot
 * find the classes its bindings name.
 */
export { PartitionDO } from "./do-partition.js";
export type { PartitionOps, PartitionRpc } from "./do-partition.js";

export { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";

/** The admin Durable Object of the topology. Bind it, and name the binding in the `nsAdmin` table option. */
export { FokosDBAutoAdminDO } from "./do-auto-admin.js";
/** The cached topology entrypoint. Re-export it, enable its cache, and name it in the `topologyEntrypoint` table option. */
export { FokosTopologyCache } from "./topology-entrypoint.js";
