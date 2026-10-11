// The liveness monitors. A hot state at the end of a run is a violation. Each run ends after its
// faults, so each monitor states what holds when the faults stop.

// Each lock row is eventually deleted or guarded.
spec LocksResolve observes eLockWritten, eLockDeleted, eLockGuarded {
  var locks: map[(partition: machine, key: tKey), int];
  var guarded: set[(partition: machine, tx: int)];

  start cold state Resolved {
    on eLockWritten do (e: tLockEvent) { Add(e); }
    on eLockDeleted do (e: tLockEvent) { Remove(e); }
    on eLockGuarded do (g: tTxGuarded) { Guard(g); }
  }

  hot state Unresolved {
    on eLockWritten do (e: tLockEvent) { Add(e); }
    on eLockDeleted do (e: tLockEvent) { Remove(e); }
    on eLockGuarded do (g: tTxGuarded) { Guard(g); }
  }

  fun Add(e: tLockEvent) {
    locks[(partition = e.partition, key = e.key)] = e.tx;
    Next();
  }

  fun Remove(e: tLockEvent) {
    locks -= ((partition = e.partition, key = e.key));
    Next();
  }

  fun Guard(g: tTxGuarded) {
    guarded += ((partition = g.partition, tx = g.tx));
    Next();
  }

  fun Next() {
    var slot: (partition: machine, key: tKey);
    foreach (slot in keys(locks)) {
      if (!((partition = slot.partition, tx = locks[slot]) in guarded)) {
        goto Unresolved;
      }
    }
    goto Resolved;
  }
}

// Each call of a client eventually gets an answer or an error.
spec ClientAnswered observes eCallStarted, eCallEnded {
  var open: set[tClientCall];

  start cold state Answered {
    on eCallStarted do (c: tClientCall) { open += (c); goto Waiting; }
    on eCallEnded do (c: tClientCall) { open -= (c); }
  }

  hot state Waiting {
    on eCallStarted do (c: tClientCall) { open += (c); }
    on eCallEnded do (c: tClientCall) {
      open -= (c);
      if (sizeof(open) == 0) {
        goto Answered;
      }
    }
  }
}

// Each coordinator transaction eventually reaches COMMITTED or CANCELLED.
spec TransactionsComplete observes eTxCreated, eTxCompleted {
  var open: set[int];

  start cold state Complete {
    on eTxCreated do (tx: int) { open += (tx); goto Incomplete; }
    on eTxCompleted do (c: tTxCompleted) { open -= (c.tx); }
  }

  hot state Incomplete {
    on eTxCreated do (tx: int) { open += (tx); }
    on eTxCompleted do (c: tTxCompleted) {
      open -= (c.tx);
      if (sizeof(open) == 0) {
        goto Complete;
      }
    }
  }
}
