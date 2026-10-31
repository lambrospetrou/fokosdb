/**
 * The admin Durable Object of FokosDB. One class serves two kinds of instances on the same `nsAdmin`
 * binding, and the name selects the role:
 *
 * - `topologyAdminName(shardGroup, fp)` (`fokos.topo.<shardGroup without "fokos.">~<fp>`) holds the
 *   topology of one shard group of a table, for example its partitions (`fokos.p.<tableName>`) or its
 *   coordinators (`fokos.tc.<tableName>.<fp(ns)>`). `fp` is the fingerprint of the namespace that holds
 *   the shard group.
 * - `fokos.admin.<tableName>~<fp(ns)>` is for the administration of a table. It holds no state yet.
 *
 * The topology role is the `FokosShardingAutoAdminDO` of the sharding library, with no change.
 */
import { FokosShardingAutoAdminDO } from "../sharding/topology-keeper.js";

export class FokosDBAutoAdminDO extends FokosShardingAutoAdminDO<Env> {}
