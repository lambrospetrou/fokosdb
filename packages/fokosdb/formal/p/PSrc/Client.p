enum tClientOpKind { PUT_ITEM, DELETE_ITEM, GET_ITEM, TRANSACT_WRITE }

// One call of the caller. `key` and `value` belong to a single-item call. `token` and `items` belong
// to transactWriteItems. After an error, transaction_commit_pending, or transaction_undecided, the
// caller sends the same token again, up to `retries` times. With `replay`, it sends the same token
// one more time after the final answer.
type tClientOp = (kind: tClientOpKind, key: tKey, value: tValue, token: int, items: seq[tTxOp], retries: int, replay: bool);

type tClientConfig = (topology: tTopology, coordinator: machine, env: machine, ops: seq[tClientOp]);

// FokosDB in the Worker, and the caller that uses it. It sends each single-item call to the root
// partition of the key, and each transactWriteItems to the coordinator. It sends the next call
// after the answer.
machine Client {
  var topology: tTopology;
  var coordinator: machine;
  var env: machine;
  var ops: seq[tClientOp];
  var next: int;
  var retried: int;
  var replayed: bool;

  start state Init {
    entry (cfg: tClientConfig) {
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
      Send();
    }

    on ePutItemResp do (resp: tPutItemResp) { Answered(resp.reqId); }
    on eDeleteItemResp do (resp: tDeleteItemResp) { Answered(resp.reqId); }
    on eGetItemResp do (resp: tGetItemResp) {
      announce eReadResult, (client = this, reqId = resp.reqId, key = ops[resp.reqId].key, found = resp.found, value = resp.value);
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
    on eRpcFailed do (f: tRpcFailed) {
      announce eCallEnded, (client = this, reqId = f.reqId);
      if (ops[next].kind == TRANSACT_WRITE) {
        RetryOrNext();
      } else {
        Next();
      }
    }
  }

  state Done {}

  fun Send() {
    var op: tClientOp;
    op = ops[next];
    announce eCallStarted, (client = this, reqId = next);
    if (op.kind == PUT_ITEM) {
      send NewRpc(this, RootOf(topology, op.key), env), ePutItem, (caller = this, reqId = next, key = op.key, value = op.value);
    } else if (op.kind == DELETE_ITEM) {
      send NewRpc(this, RootOf(topology, op.key), env), eDeleteItem, (caller = this, reqId = next, key = op.key);
    } else if (op.kind == GET_ITEM) {
      announce eReadStart, (client = this, reqId = next, key = op.key);
      send NewRpc(this, RootOf(topology, op.key), env), eGetItem, (caller = this, reqId = next, key = op.key);
    } else {
      send NewRpc(this, coordinator, env), eInitiateWrite, (caller = this, reqId = next, token = op.token, items = op.items);
    }
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
