# Stored state audit before stable releases

**State:** Draft
**Date:** 2026-11-24
**Author:** Lambros Petrou

After the first stable release, a change of a stored format needs a migration and a compatible reader. This list
gives the problems in the stored state of the Durable Objects to fix before that release.

## A. Stored values that depend on the table name, the namespace, the jurisdiction, or the DO ID

A restore copies the storage of each DO into a DO with a different name and ID
([the backup and restore RFC](2026-11-17-consistent-backup-and-restore.md)). Each value below must be a logical
reference, or the restore must rewrite it. The RFC (§1.2, M2) already lists `__fokos/identity`, the repartition
targets and plans, `__fokos/import`, `pending_tx_info.coordinator_json`, `tc_participants.partition_do_name`,
`ancestor_fences`, `producer_fences`, and the rows of the topology keeper. The items below are not in the RFC.

1. **`tc_participants.partition_context_json`** — It stores the full route context of the participant: `doName`,
   `shardGroup`, `jurisdiction`, the binding names of the policy, and `policyVersion`. Commit, cancel, and recovery
   make the stub from it. After a restore into another jurisdiction, the coordinator sends commits to the DOs of the
   source table. Store only the `partitionId`, and build the context from the route context of the coordinator.
   `tc_items.partition_do_name` also holds the full name.
2. **The admin outbox** — The event id is `<source doName>/r<seq>` (`#recordCompletion` in
   `sharding/runtime.ts`). The event holds `shardGroup`, the names of the source and the targets, and DO IDs. A
   restored DO sends a pending event with the old shard group, and the keeper stores rows under that group. Make
   the id from the `partitionId`, and let the keeper use the shard group of its own name.
3. **`fokos_topo_repartitions.actor_id` and `data`** — They hold DO IDs and full names, and no code reads them.
   Remove both columns.
4. **`__fokos/policy`** — A restore copies the `policyVersion` of the source table, but each table has its own
   counter. When the target table sends a lower version, the stored policy of the source stays: wrong binding names
   and wrong split conditions. The stale-transaction job also reads `nsTx` from it. Reset the key at restore, or
   do not store binding names in it.
5. **`topology.jurisdiction` in `__fokos/identity`** — The RFC rewrites only `doName` and `shardGroup`.
   `topologiesEqual` compares the jurisdiction, so a restore into another jurisdiction refuses every request. The
   namespace fingerprint also uses the jurisdiction, so the names of the coordinators and the materialization DOs
   change with it.
6. **`PushTargetBatch.producerId`** — It is `ctx.id.toString()` of the materialization DO, and a push sink of the
   user stores it as a fence key. A restore changes it. Select a logical producer id before the API is stable, or
   document the DO ID as the contract.

## B. General problems

1. **No reader checks a `schema` field** — `__fokos/identity`, `__fokos/import`, the plan head, and the route
   context have one, but no code compares it. Add a reader for each record that refuses or upgrades an unknown
   version.
2. **`__fokos/cache/hash_arena` has no version** — A wrong layout routes to the wrong children with no error. Add a
   version, and drop a snapshot with another version. `__fokos/policy` also has no version.
3. (NON-ISSUE) **No guard against a rollback** — `SQLSchemaMigrations` of `durable-utils` does nothing when the stored migration
   id is higher than the last migration of the code. Older code then runs on a newer schema. Each of the six
   migration trackers must refuse this case with `partition_init_failed`.
4. **JSON columns with no version** — `tc_items.conditions_json` and `update_json` (expression trees),
   `answer_json`, `error_json`, `results_json`, and `op_list`. Only `coordinator_json` has a version. Freeze each
   format, or add a version.
5. (DONE) **`operations_hash` has no fixed test value** — The test compares the hash only with itself. A change of the
   canonical form makes a retry of an in-flight token fail after a deploy. Add tests with fixed expected values.
6. **The range DO name** — It stores the shared leading bytes of the two boundaries two times (FIXME in
   `rangePartitionDoName`, `sharding/partition-id.ts`). The name encoding must never change after the release, so
   decide now.
7. (DONE) **The `PREPARED` fallback** — The coordinator still moves `PREPARED` rows from older code to `COMMITTING`. No such
   rows exist before the release. Remove the fallback.
8. (NON-ISSUE) **Two KV key prefixes** — The sharding store uses `__fokos/`, and the partition, coordinator, materialization,
   and item-stats stores use `__fokosdb/`. A rename costs nothing now.
9. **A wrong topology hint cancels a transaction** — The coordinator stores the participant address from the hint
   (FIXME in the prepare fan-out of `do-transaction-coordinator.ts`). The fix writes `partition_context_json`, so do
   it with A1.
10. (NON-ISSUE) **A split router keeps its item rows** — The rows stay after a hash split (FIXME in `cleanupSourceStep`,
    `do-partition.ts`). This adds storage cost, and a snapshot restore copies the rows. It is not a format change.
11. **Job names are stored** — `__fokos/jobs` keeps an entry of a removed or renamed job. This does no harm, but a
    job name is a stored contract.

## Order

1. A1, A2, A3: they change SQL columns and keys.
2. B1, B2, B3, B5: they make later releases safe.
3. B6, B7: they freeze formats.
4. A4, A5, A6: they need a decision in the restore RFC.
