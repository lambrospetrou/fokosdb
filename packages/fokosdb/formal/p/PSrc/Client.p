enum tItemOpKind { PUT_ITEM, DELETE_ITEM, GET_ITEM }

type tItemOp = (kind: tItemOpKind, key: tKey, value: tValue);

type tClientConfig = (partition: machine, ops: seq[tItemOp]);

// FokosDB in the Worker: putItem, deleteItem, and getItem. The client sends each call to the root
// partition of the key, and sends the next call after the answer.
machine Client {
  var partition: machine;
  var ops: seq[tItemOp];
  var next: int;

  start state Init {
    entry (cfg: tClientConfig) {
      partition = cfg.partition;
      ops = cfg.ops;
      goto Calling;
    }
  }

  state Calling {
    entry {
      var op: tItemOp;
      if (next == sizeof(ops)) {
        goto Done;
      }
      op = ops[next];
      if (op.kind == PUT_ITEM) {
        send partition, ePutItem, (caller = this, reqId = next, key = op.key, value = op.value);
      } else if (op.kind == DELETE_ITEM) {
        send partition, eDeleteItem, (caller = this, reqId = next, key = op.key);
      } else {
        send partition, eGetItem, (caller = this, reqId = next, key = op.key);
      }
    }

    on ePutItemResp do (resp: tPutItemResp) { Answered(resp.reqId); }
    on eDeleteItemResp do (resp: tDeleteItemResp) { Answered(resp.reqId); }
    on eGetItemResp do (resp: tGetItemResp) { Answered(resp.reqId); }
  }

  state Done {}

  fun Answered(reqId: int) {
    assert reqId == next, format("client got the answer of call {0} while it waits for call {1}", reqId, next);
    next = next + 1;
    goto Calling;
  }
}
