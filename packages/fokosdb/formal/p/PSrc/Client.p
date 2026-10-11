enum tClientOpKind { PUT_ITEM, DELETE_ITEM, GET_ITEM, TRANSACT_WRITE, TRANSACT_GET }

// One call of the caller. `key` and `value` belong to a single-item call. `token` and `items` belong
// to transactWriteItems, and token 0 is a call with no token. After an error,
// transaction_commit_pending, or transaction_undecided, the caller sends the same token again, up to
// `retries` times. With `replay`, it sends the same token one more time after the final answer.
// `readKeys` belong to transactGetItems. `generatedToken` is the token that the client generates
// when a call with no token goes to the coordinator.
type tClientOp = (kind: tClientOpKind, key: tKey, value: tValue, token: int, items: seq[tTxOp], retries: int, replay: bool,
                  readKeys: seq[tKey], generatedToken: int);

type tClientConfig = (bugs: tBugs, topology: tTopology, coordinator: machine, env: machine, ops: seq[tClientOp]);

// FokosDB in the Worker, and the caller that uses it. It sends each single-item call to the root
// partition of the key. A transactWriteItems with no token whose keys are all on one root goes to
// that partition (#writeSingleShotFastPath), and every other one to the coordinator. A
// transactGetItems whose keys are all on one root goes to that partition (#readSnapshotFastPath),
// and every other one runs the two-phase path (#readTransaction). The outcome not_applicable of
// either single-partition path sends the call to the other path. It sends the next call after the
// answer.
machine Client {
  var bugs: tBugs;
  var topology: tTopology;
  var coordinator: machine;
  var env: machine;
  var ops: seq[tClientOp];
  var next: int;
  var retried: int;
  var replayed: bool;
  // The token of the transactWriteItems in progress, or 0 for a call with no token.
  var token: int;
  // The read in progress: the keys of each root partition, the path, the phase, the attempts of the
  // call to each root, the answers of the phase, the calls of the phase that used up their attempts,
  // and the result of phase 1.
  var groups: map[machine, seq[tKey]];
  var snapshot: bool;
  var phase: int;
  var attempts: map[machine, int];
  var replies: map[machine, seq[tReadItem]];
  var failedCalls: int;
  var phase1: map[tKey, tReadItem];

  start state Init {
    entry (cfg: tClientConfig) {
      bugs = cfg.bugs;
      topology = cfg.topology;
      coordinator = cfg.coordinator;
      env = cfg.env;
      ops = cfg.ops;
      goto Calling;
    }
  }

  state Calling {
    entry {
      if (next == sizeof(ops)) {
        goto Done;
      }
      retried = 0;
      replayed = false;
      token = ops[next].token;
      Send();
    }

    on ePutItemResp do (resp: tPutItemResp) { Answered(resp.reqId); }
    on eDeleteItemResp do (resp: tDeleteItemResp) { Answered(resp.reqId); }
    on eGetItemResp do (resp: tGetItemResp) {
      var readValues: seq[tReadValue];
      readValues += (0, (key = ops[resp.reqId].key, found = resp.found, value = resp.value));
      announce eReadResult, (client = this, reqId = resp.reqId, transaction = false, results = readValues);
      Answered(resp.reqId);
    }
    // not_applicable: the client generates a token and uses the coordinator path.
    on eExecuteSingleShotResp do (resp: tSingleShotResp) {
      if (resp.notApplicable) {
        token = ops[next].generatedToken;
        Send();
        return;
      }
      Answered(resp.reqId);
    }
    on eInitiateWriteResp do (resp: tInitiateWriteResp) {
      announce eClientAnswer, (client = this, tx = resp.tx, answer = resp.answer);
      announce eCallEnded, (client = this, reqId = resp.reqId);
      if (resp.answer == ANSWER_COMMIT_PENDING || resp.answer == ANSWER_UNDECIDED) {
        RetryOrNext();
      } else if (ops[next].replay && !replayed) {
        replayed = true;
        Send();
      } else {
        Next();
      }
    }

    // The snapshot path: the outcome aborted is the pending-write error, and the outcome
    // not_applicable starts the two-phase path.
    on eReadSnapshotResp do (resp: tReadSnapshotResp) {
      assert resp.reqId == next, format("client got the answer of read {0} while it waits for call {1}", resp.reqId, next);
      if (resp.notApplicable) {
        snapshot = false;
        StartPhase();
        return;
      }
      if (resp.aborted) {
        ReadFailed();
        return;
      }
      ReadDone(resp.items);
    }
    on eReadForTransactionResp do (resp: tReadForTransactionResp) {
      assert resp.reqId == next, format("client got the answer of read {0} while it waits for call {1}", resp.reqId, next);
      replies[resp.partition] = resp.items;
      PhaseCallSettled();
    }

    // The single-partition path does not retry: the outcome of a failed call is unknown. A read
    // applies nothing, so each read call sends again.
    on eRpcFailed do (f: tRpcFailed) {
      if (ops[next].kind == TRANSACT_GET) {
        ReadCallFailed(f.target);
        return;
      }
      announce eCallEnded, (client = this, reqId = f.reqId);
      if (ops[next].kind == TRANSACT_WRITE && token != 0) {
        RetryOrNext();
      } else {
        Next();
      }
    }
  }

  state Done {}

  fun Send() {
    var op: tClientOp;
    var roots: set[machine];
    var txOp: tTxOp;
    op = ops[next];
    announce eCallStarted, (client = this, reqId = next);
    if (op.kind == PUT_ITEM) {
      send NewRpc(this, RootOf(topology, op.key), env), ePutItem, (caller = this, reqId = next, key = op.key, value = op.value);
    } else if (op.kind == DELETE_ITEM) {
      send NewRpc(this, RootOf(topology, op.key), env), eDeleteItem, (caller = this, reqId = next, key = op.key);
    } else if (op.kind == GET_ITEM) {
      announce eReadStart, (client = this, reqId = next, transaction = false);
      send NewRpc(this, RootOf(topology, op.key), env), eGetItem, (caller = this, reqId = next, key = op.key);
    } else if (op.kind == TRANSACT_GET) {
      StartRead(op);
    } else {
      foreach (txOp in op.items) {
        roots += (RootOf(topology, txOp.key));
      }
      if (token == 0 && sizeof(roots) == 1) {
        send NewRpc(this, RootOf(topology, op.items[0].key), env), eExecuteSingleShot, (caller = this, reqId = next, items = op.items);
        return;
      }
      assert token != 0, "the test driver gives each transaction of the coordinator path a token";
      send NewRpc(this, coordinator, env), eInitiateWrite, (caller = this, reqId = next, token = token, items = op.items);
    }
  }

  // #transactGetItems: groups the keys by root partition. One group takes the snapshot path, and
  // more groups take the two-phase path.
  fun StartRead(op: tClientOp) {
    var key: tKey;
    var root: machine;
    groups = default(map[machine, seq[tKey]]);
    foreach (key in op.readKeys) {
      root = RootOf(topology, key);
      if (!(root in groups)) {
        groups[root] = default(seq[tKey]);
      }
      groups[root] += (sizeof(groups[root]), key);
    }
    announce eReadStart, (client = this, reqId = next, transaction = true);
    snapshot = sizeof(groups) == 1;
    phase = 1;
    StartPhase();
  }

  // One read phase: one call to each group at the same time (Promise.allSettled).
  fun StartPhase() {
    var root: machine;
    replies = default(map[machine, seq[tReadItem]]);
    failedCalls = 0;
    foreach (root in keys(groups)) {
      attempts[root] = 1;
      SendRead(root);
    }
  }

  fun SendRead(root: machine) {
    if (snapshot) {
      send NewRpc(this, root, env), eReadSnapshot, (caller = this, reqId = next, readKeys = groups[root]);
    } else {
      send NewRpc(this, root, env), eReadForTransaction, (caller = this, reqId = next, readKeys = groups[root]);
    }
  }

  // The retry policy of a read call: up to READ_MAX_ATTEMPTS attempts. A lost call and a restart are
  // transient faults of the runtime, so the snapshot path sends again too. When the last attempt
  // fails, the read throws that error after the other calls of the phase settled.
  fun ReadCallFailed(target: machine) {
    if (attempts[target] < READ_MAX_ATTEMPTS()) {
      attempts[target] = attempts[target] + 1;
      SendRead(target);
      return;
    }
    if (snapshot) {
      ReadFailed();
      return;
    }
    failedCalls = failedCalls + 1;
    PhaseCallSettled();
  }

  // The end of a phase of the two-phase path, when every call settled. A pending write in either
  // phase gives the pending-write error. After phase 2, each key compares the two phases: found
  // must match, a found item compares version, and an absent item compares maxDeletedV. A
  // difference gives read_conflict. The result is the result of phase 1.
  fun PhaseCallSettled() {
    var root: machine;
    var item: tReadItem;
    var items: seq[tReadItem];
    if (sizeof(replies) + failedCalls < sizeof(groups)) {
      return;
    }
    if (failedCalls > 0) {
      ReadFailed();
      return;
    }
    foreach (root in keys(groups)) {
      foreach (item in replies[root]) {
        if (item.hasPendingWrite && !bugs.readIgnoresPendingWrite) {
          ReadFailed();
          return;
        }
        items += (sizeof(items), item);
      }
    }
    if (phase == 1) {
      phase1 = default(map[tKey, tReadItem]);
      foreach (item in items) {
        phase1[item.key] = item;
      }
      if (bugs.readSkipsPhaseTwo) {
        ReadDone(items);
        return;
      }
      phase = 2;
      StartPhase();
      return;
    }
    foreach (item in items) {
      if (!SameCommittedState(phase1[item.key], item)) {
        ReadFailed();
        return;
      }
    }
    ReadDone(values(phase1));
  }

  fun SameCommittedState(a: tReadItem, b: tReadItem): bool {
    if (a.found != b.found) {
      return false;
    }
    if (a.found) {
      return a.version == b.version;
    }
    return a.maxDeletedV == b.maxDeletedV || bugs.readIgnoresMaxDeletedV;
  }

  fun ReadDone(items: seq[tReadItem]) {
    var item: tReadItem;
    var readValues: seq[tReadValue];
    foreach (item in items) {
      readValues += (sizeof(readValues), (key = item.key, found = item.found, value = item.value));
    }
    announce eReadResult, (client = this, reqId = next, transaction = true, results = readValues);
    announce eCallEnded, (client = this, reqId = next);
    Next();
  }

  // The read ends with an error: the pending-write error, read_conflict, or the error of a call
  // that used up its attempts.
  fun ReadFailed() {
    announce eCallEnded, (client = this, reqId = next);
    Next();
  }

  fun RetryOrNext() {
    if (retried < ops[next].retries) {
      retried = retried + 1;
      Send();
    } else {
      Next();
    }
  }

  fun Answered(reqId: int) {
    assert reqId == next, format("client got the answer of call {0} while it waits for call {1}", reqId, next);
    announce eCallEnded, (client = this, reqId = reqId);
    Next();
  }

  fun Next() {
    next = next + 1;
    goto Calling;
  }
}
