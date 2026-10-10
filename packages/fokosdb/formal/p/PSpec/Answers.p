// The client gets committed only for a transaction in COMMITTED, and cancelled only for a
// transaction with the decision cancel.
spec AnswerMatchesDecision observes eDecision, eTxCompleted, eClientAnswer {
  var decisions: map[int, bool];
  var committed: set[int];

  start state Watching {
    on eDecision do (d: tDecision) {
      decisions[d.tx] = d.commit;
    }

    on eTxCompleted do (c: tTxCompleted) {
      if (c.committed) {
        committed += (c.tx);
      }
    }

    on eClientAnswer do (a: tClientAnswer) {
      if (a.answer == ANSWER_COMMITTED) {
        assert a.tx in committed, format("the client got committed for transaction {0}, which is not in COMMITTED", a.tx);
      } else if (a.answer == ANSWER_CANCELLED) {
        assert a.tx in decisions && !decisions[a.tx],
          format("the client got cancelled for transaction {0}, which has no cancel decision", a.tx);
      }
    }
  }
}

// One write of a key, in the order that the partitions applied the writes.
type tKeyWrite = (tx: int, deleted: bool, value: tValue);

// A read that starts after the client got committed for a transaction returns, for each key of that
// transaction, the value of that transaction or of a later writer.
spec ReadAfterCommit observes eDecision, eItemWritten, eClientAnswer, eReadStart, eReadResult {
  var txKeys: map[int, set[tKey]];
  var history: map[tKey, seq[tKeyWrite]];
  var committedAnswers: set[int];
  // The committed transactions that each read must see, by the client and the request of the read.
  var mustSee: map[(client: machine, reqId: int), set[int]];

  start state Watching {
    on eDecision do (d: tDecision) {
      txKeys[d.tx] = d.itemKeys;
    }

    on eItemWritten do (w: tItemWritten) {
      if (!(w.key in history)) {
        history[w.key] = default(seq[tKeyWrite]);
      }
      history[w.key] += (sizeof(history[w.key]), (tx = w.tx, deleted = w.deleted, value = w.value));
    }

    on eClientAnswer do (a: tClientAnswer) {
      if (a.answer == ANSWER_COMMITTED) {
        committedAnswers += (a.tx);
      }
    }

    on eReadStart do (r: tReadStart) {
      var tx: int;
      var txs: set[int];
      foreach (tx in committedAnswers) {
        if (r.key in txKeys[tx]) {
          txs += (tx);
        }
      }
      mustSee[(client = r.client, reqId = r.reqId)] = txs;
    }

    on eReadResult do (r: tReadResult) {
      var tx: int;
      var writes: seq[tKeyWrite];
      var i: int;
      var at: int;
      var seen: bool;
      if (r.key in history) {
        writes = history[r.key];
      }
      foreach (tx in mustSee[(client = r.client, reqId = r.reqId)]) {
        at = -1;
        i = 0;
        while (i < sizeof(writes)) {
          if (writes[i].tx == tx) {
            at = i;
          }
          i = i + 1;
        }
        assert at >= 0, format("a read of key {0} started after transaction {1} answered committed, but the transaction has not applied the key", r.key, tx);
        seen = false;
        i = at;
        while (i < sizeof(writes)) {
          if ((r.found && !writes[i].deleted && writes[i].value == r.value) || (!r.found && writes[i].deleted)) {
            seen = true;
          }
          i = i + 1;
        }
        assert seen, format("a read of key {0} returned (found {1}, value {2}), which is older than the write of committed transaction {3}", r.key, r.found, r.value, tx);
      }
    }
  }
}
