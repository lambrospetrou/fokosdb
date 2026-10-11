// A local decision writes, applies, releases, or guards the lock row of a key only on the partition
// that owns the key now, and never on a target that imports. The owner of a key is its root, until a
// split of the root cuts over. Then target i of the split owns the keys of child index i.
spec Authority observes eLocalTxMutation, eOwnerChanged, eImportState {
  var routers: set[machine];
  var sliceOf: map[machine, int];
  var importing: set[machine];

  start state Watching {
    on eOwnerChanged do (o: tOwnerChanged) {
      var i: int;
      routers += (o.source);
      while (i < sizeof(o.children)) {
        sliceOf[o.children[i]] = i;
        i = i + 1;
      }
    }

    on eImportState do (s: tImportStateChanged) {
      if (s.current == AWAITING_DATA || s.current == IMPORTING) {
        importing += (s.partition);
      } else {
        importing -= (s.partition);
      }
    }

    on eLocalTxMutation do (e: tLockEvent) {
      assert !(e.partition in importing),
        format("partition {0} changed the lock of key {1} of transaction {2} while it imports", e.partition, e.key, e.tx);
      assert !(e.partition in routers) && (!(e.partition in sliceOf) || sliceOf[e.partition] == ChildIndexOf(e.key.hk)),
        format("partition {0} changed the lock of key {1} of transaction {2}, but it does not own the key", e.partition, e.key, e.tx);
    }
  }
}
