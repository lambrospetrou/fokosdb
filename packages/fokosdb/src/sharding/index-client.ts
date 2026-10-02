/**
 * Sharding client entry point, `fokosdb/sharding/client`: what a caller of a shard group needs. It
 * resolves an entry partition, sends an operation, reads the routing, and handles the errors.
 *
 * A Worker that only calls partitions imports this entry. The `check-client-bundle` plugin in
 * `tsdown.config.ts` fails the build when a module of this entry reaches the runtime, the store, the
 * scheduler, the repartition flow, or a FokosDB module. `fokosdb/sharding/server` re-exports this entry.
 */

export * from "./exports-client.js";
