/**
 * The admin Durable Object of FokosDB. One class serves two kinds of instances on the same `nsAdmin`
 * binding, and the name selects the role:
 *
 * - `topologyAdminName(shardGroup)` (`fokos.topo.<shardGroup>`) holds the topology of one shard group
 *   of a table: its partitions (`fokos.p.<tableName>`) or its coordinators (`fokos.tc.<tableName>`).
 * - `fokos.admin.<tableName>` is for the administration of a table. It holds no state yet.
 *
 * The topology role is the `FokosShardingAutoAdminDO` of the sharding library, with no change.
 */
import { FokosShardingAutoAdminDO } from "../sharding/topology-keeper.js";

export class FokosDBAutoAdminDO extends FokosShardingAutoAdminDO<Env> {}
