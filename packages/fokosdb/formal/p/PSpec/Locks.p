// While a transaction holds the lock of a key on a partition, no other writer changes the key there,
// and no other transaction takes the lock.
spec LockExclusion observes eLockWritten, eLockDeleted, eItemWritten {
  var holders: map[(partition: machine, key: tKey), int];

  start state Watching {
    on eLockWritten do (e: tLockEvent) {
      assert !((partition = e.partition, key = e.key) in holders),
        format("transaction {0} locked key {1}, but transaction {2} holds the lock",
          e.tx, e.key, holders[(partition = e.partition, key = e.key)]);
      holders[(partition = e.partition, key = e.key)] = e.tx;
    }

    on eLockDeleted do (e: tLockEvent) {
      holders -= ((partition = e.partition, key = e.key));
    }

    on eItemWritten do (w: tItemWritten) {
      var slot: (partition: machine, key: tKey);
      slot = (partition = w.partition, key = w.key);
      assert !(slot in holders) || holders[slot] == w.tx,
        format("writer {0} changed key {1} while transaction {2} holds its lock (0 is a single-item write)",
          w.tx, w.key, holders[slot]);
    }
  }
}
