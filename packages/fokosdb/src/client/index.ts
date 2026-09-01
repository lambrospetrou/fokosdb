/**
 * Client entry point: everything a Worker needs to talk to a FokosDB table.
 *
 * It reaches into `../shared/` per module rather than through a barrel, so an entry pulls in only
 * the modules it names. Nothing here imports a Durable Object class as a value, which keeps the
 * server implementation out of this bundle.
 */
export { FokosDB } from "./db.js";
export type { FokosDBOptions } from "./db.js";

export * from "../shared/types.js";

export { PartitionContextCreator } from "../shared/partition-topology/partition-context.js";
export type { PartitionContext, PartitionContextResolved, SplitConditions } from "../shared/partition-topology/partition-context.js";

export { PartitionTopologyRouterImpl } from "../shared/partition-topology/router.js";
export type { PartitionTopologyRouter } from "../shared/partition-topology/router.js";

export { ExpressionError } from "../shared/expression/errors.js";
export { compileConditionExpression } from "../shared/expression/compiler.js";
