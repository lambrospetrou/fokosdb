// No commit, cancel, quarantine, or repair deletes or changes a transfer copy: the lock row of a key
// that a source gave to a target. Only the cleanup of the source deletes a copy.
spec CopiesUntouched observes eCopyChanged {
  start state Watching {
    on eCopyChanged do (e: tLockEvent) {
      assert false, format("partition {0} changed the transfer copy of key {1} of transaction {2}", e.partition, e.key, e.tx);
    }
  }
}

// A source deletes a transfer copy only after it recorded the acknowledgement of the target that owns
// the key. Until then, the copy can be the only lock row of the key.
spec Retention observes eCopyDeleted, eAckRecorded {
  var acked: set[tAckRecorded];

  start state Watching {
    on eAckRecorded do (a: tAckRecorded) {
      acked += (a);
    }

    on eCopyDeleted do (c: tCopyDeleted) {
      assert (source = c.partition, target = c.target) in acked,
        format("source {0} deleted the copy of key {1} of transaction {2} before target {3} acknowledged its import",
          c.partition, c.key, c.tx, c.target);
    }
  }
}
