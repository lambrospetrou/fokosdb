# Bugfix: Learn a range partition with no stored ancestors

**Status:** Decided. `docs/agent-plans/2026-09-27-range-self-hint-and-route-evidence-floor.md` is the spec.

## Issue

`FokosShardingRuntime.#setIdentity` adds a partition's own boundaries to `_rangeAncestors` only when the partition
has stored ancestors. A depth-1 range child has none, so its response omits its own boundaries. A forwarding
partition reads the hash key from the ID but learns boundaries only from `_rangeAncestors`. It cannot learn a direct
route to this child from the response.

The same issue affects deeper children when the configuration selects no ancestors. Routing stays correct, but later
requests can take extra hops through the range root. The root needs no self hint.

Before the migration to `FokosShardingRuntime`, the load path after a restart added the own slice for each range
partition with a depth above 0. Only the `fokosInit` path had the `ancestors.length > 0` condition. The migration
kept only the `fokosInit` condition.

## Decision

- The learner gets the own slice of each range node from its partition ID. The ID already encodes the hash key and
  both boundaries, and the node carries `rangeDepth`. The learner skips depth 0.
- `_rangeAncestors` holds only the selected ancestors. It does not repeat the own slice.
- The learner decodes the full ID. This replaces the learner rule of the base64url ID spec, which stays unchanged.
- A range router at depth d stores only slices at depth d + 2 or deeper. It never uses a shallower slice.

## Related fix: the byte cap can drop every node

`RouteCollector.build` stops at the first node that crosses `ROUTE_EVIDENCE_MAX_BYTES`, and that can be the first
node. A range node with maximum key sizes counts more than 10 KiB, so the response has an empty `servedBy`.

Decision: `build` always keeps one node. On an error it keeps the raiser. On success it keeps the node with the
highest role. The other nodes follow the current cap rule.

## Later work

The spec lists these in its future extensions:

- The raiser of an error is marked by list position. A `raisedBy` field or a `raised` role can replace the position,
  so that no reader depends on the order of `servedBy`.
- Remove repeated ancestor entries across the nodes of one response.
- Rank the list by role before the cut.
- Skip the decode for a recently learned partition ID.
- Make the eviction in `fokos_range_hierarchy` cheaper. A new row in a full table costs about 1 ms.
