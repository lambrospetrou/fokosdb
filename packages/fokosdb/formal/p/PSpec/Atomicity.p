// A write transaction applies on all its items or on none:
// - No key of a transaction applies when the decision is cancel.
// - No lock of a transaction is released by a cancel when the decision is commit, before the key of
//   the lock applied. After the key applied, a lock of the same key is a duplicate that a late
//   prepare wrote, and it holds nothing that a cancel can lose.
// - When a transaction reaches COMMITTED, every key of it has applied.
spec Atomicity observes eDecision, eApplied, eReleased, eTxCompleted {
  var decisions: map[int, tDecision];
  var applied: map[int, set[tKey]];
  var released: map[int, set[tKey]];

  start state Watching {
    on eDecision do (d: tDecision) {
      decisions[d.tx] = d;
      if (d.commit) {
        assert !(d.tx in released), format("transaction {0} decided commit after a cancel released {1}", d.tx, released[d.tx]);
      } else {
        assert !(d.tx in applied), format("transaction {0} decided cancel after {1} applied", d.tx, applied[d.tx]);
      }
    }

    on eApplied do (e: tLockEvent) {
      assert e.tx in decisions && decisions[e.tx].commit,
        format("key {0} of transaction {1} applied with no commit decision", e.key, e.tx);
      if (!(e.tx in applied)) {
        applied[e.tx] = default(set[tKey]);
      }
      applied[e.tx] += (e.key);
    }

    on eReleased do (e: tLockEvent) {
      assert !(e.tx in decisions && decisions[e.tx].commit) || (e.tx in applied && e.key in applied[e.tx]),
        format("a cancel released key {0} of transaction {1}, but the decision is commit and the key has not applied", e.key, e.tx);
      if (!(e.tx in released)) {
        released[e.tx] = default(set[tKey]);
      }
      released[e.tx] += (e.key);
    }

    on eTxCompleted do (c: tTxCompleted) {
      if (c.committed) {
        assert c.tx in applied && applied[c.tx] == decisions[c.tx].itemKeys,
          format("transaction {0} reached COMMITTED, but only some of its keys {1} applied", c.tx, decisions[c.tx].itemKeys);
      }
    }
  }
}

// For each clientRequestToken, each key of its puts and deletes applies at most once, over all
// partitions, inside the idempotency window of the token. A check is not counted: its apply only
// raises a stamp with MAX, so a second apply changes nothing. A late prepare of a committed check can
// lock the key again, and the stale job then applies the check a second time.
spec SingleApply observes eDecision, eApplied, eTokenSwept {
  var tokenOf: map[int, int];
  var writeKeys: map[int, set[tKey]];
  var applied: map[int, set[tKey]];

  start state Watching {
    on eDecision do (d: tDecision) {
      tokenOf[d.tx] = d.token;
      writeKeys[d.tx] = d.writeKeys;
    }

    on eTokenSwept do (token: int) {
      applied -= (token);
    }

    on eApplied do (e: tLockEvent) {
      var token: int;
      // Atomicity reports an apply with no decision.
      if (!(e.tx in tokenOf) || !(e.key in writeKeys[e.tx])) {
        return;
      }
      token = tokenOf[e.tx];
      if (!(token in applied)) {
        applied[token] = default(set[tKey]);
      }
      assert !(e.key in applied[token]),
        format("key {0} of token {1} applied a second time, by transaction {2}", e.key, token, e.tx);
      applied[token] += (e.key);
    }
  }
}
