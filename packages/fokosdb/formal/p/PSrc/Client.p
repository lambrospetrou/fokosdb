enum tClientOpKind { PUT_ITEM, DELETE_ITEM, GET_ITEM, TRANSACT_WRITE }

// One call of the caller. `key` and `value` belong to a single-item call. `token` and `items` belong
// to transactWriteItems.
type tClientOp = (kind: tClientOpKind, key: tKey, value: tValue, token: int, items: seq[tTxOp]);

type tClientConfig = (topology: tTopology, coordinator: machine, ops: seq[tClientOp]);

// FokosDB in the Worker. It sends each single-item call to the root partition of the key, and each
// transactWriteItems to the coordinator. It sends the next call after the answer.
machine Client {
  var topology: tTopology;
  var coordinator: machine;
  var ops: seq[tClientOp];
  var next: int;

  start state Init {
    entry (cfg: tClientConfig) {
      topology = cfg.topology;
      coordinator = cfg.coordinator;
      ops = cfg.ops;
      goto Calling;
    }
  }

  state Calling {
    entry {
      var op: tClientOp;
      if (next == sizeof(ops)) {
        goto Done;
      }
      op = ops[next];
      if (op.kind == PUT_ITEM) {
        send NewRpc(this, RootOf(topology, op.key)), ePutItem, (caller = this, reqId = next, key = op.key, value = op.value);
      } else if (op.kind == DELETE_ITEM) {
        send NewRpc(this, RootOf(topology, op.key)), eDeleteItem, (caller = this, reqId = next, key = op.key);
      } else if (op.kind == GET_ITEM) {
        announce eReadStart, (client = this, reqId = next, key = op.key);
        send NewRpc(this, RootOf(topology, op.key)), eGetItem, (caller = this, reqId = next, key = op.key);
      } else {
        send NewRpc(this, coordinator), eInitiateWrite, (caller = this, reqId = next, token = op.token, items = op.items);
      }
    }

    on ePutItemResp do (resp: tPutItemResp) { Answered(resp.reqId); }
    on eDeleteItemResp do (resp: tDeleteItemResp) { Answered(resp.reqId); }
    on eGetItemResp do (resp: tGetItemResp) {
      announce eReadResult, (client = this, reqId = resp.reqId, key = ops[resp.reqId].key, found = resp.found, value = resp.value);
      Answered(resp.reqId);
    }
    on eInitiateWriteResp do (resp: tInitiateWriteResp) {
      announce eClientAnswer, (client = this, tx = resp.tx, answer = resp.answer);
      Answered(resp.reqId);
    }
  }

  state Done {}

  fun Answered(reqId: int) {
    assert reqId == next, format("client got the answer of call {0} while it waits for call {1}", reqId, next);
    next = next + 1;
    goto Calling;
  }
}
