// The stale job never quarantines a lock of a transaction that reached COMMITTED. The quarantine waits
// for an operator, and is for a lock whose outcome the partition cannot know. A lock of a committed
// transaction can stay only as a duplicate that a late prepare of a check wrote, and its checks
// already applied.
spec CommittedNeverQuarantined observes eTxCompleted, eLockGuarded {
  var committed: set[int];

  start state Watching {
    on eTxCompleted do (c: tTxCompleted) {
      if (c.committed) {
        committed += (c.tx);
      }
    }

    on eLockGuarded do (g: tTxGuarded) {
      assert !(g.tx in committed), format("a partition quarantined a lock of transaction {0}, which is COMMITTED", g.tx);
    }
  }
}

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
