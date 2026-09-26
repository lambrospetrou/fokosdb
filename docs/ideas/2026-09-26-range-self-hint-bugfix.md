# Bugfix: Learn a range partition with no stored ancestors

## Issue

`FokosShardingRuntime.#setIdentity` adds a partition's own boundaries to `_rangeAncestors` only when the partition has stored ancestors. A depth-1 range child has none, so its response omits its own boundaries. A forwarding partition reads the hash key from the ID but learns boundaries only from `_rangeAncestors`. It cannot learn a direct route to this child from the response.

The same issue affects deeper children when the configuration selects no ancestors. Routing stays correct, but later requests can take extra hops through the range root. The root needs no self hint.

## Possible fix

Include each non-root range partition's own boundaries even if it has no stored ancestors. Keep ancestor selection unchanged. Test responses from a depth-1 child and a child with no selected ancestors. Check that a forwarding partition learns each child's range slice.
