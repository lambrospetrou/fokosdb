type tRpcConfig = (caller: machine, target: machine);

// Starts one call. The caller sends the request event to the machine that this returns.
fun NewRpc(caller: machine, target: machine): machine {
  return new RpcCall((caller = caller, target = target));
}

// One Workers RPC call: the request to the target, and the answer back to the caller. Workers RPC
// keeps no order between calls, so each call has its own machine, and the checker can deliver the
// request and the answer of each call at any point relative to the other calls. The machine stops
// after it delivers the answer.
machine RpcCall {
  var caller: machine;
  var target: machine;

  start state Calling {
    entry (c: tRpcConfig) {
      caller = c.caller;
      target = c.target;
    }

    on ePutItem do (req: tPutItemReq) {
      var r: tPutItemReq;
      r = req;
      r.caller = this;
      send target, ePutItem, r;
    }
    on eDeleteItem do (req: tDeleteItemReq) {
      var r: tDeleteItemReq;
      r = req;
      r.caller = this;
      send target, eDeleteItem, r;
    }
    on eGetItem do (req: tGetItemReq) {
      var r: tGetItemReq;
      r = req;
      r.caller = this;
      send target, eGetItem, r;
    }
    on eInitiateWrite do (req: tInitiateWriteReq) {
      var r: tInitiateWriteReq;
      r = req;
      r.caller = this;
      send target, eInitiateWrite, r;
    }
    on eTxPrepare do (req: tPrepareReq) {
      var r: tPrepareReq;
      r = req;
      r.caller = this;
      send target, eTxPrepare, r;
    }
    on eTxCommit do (req: tCommitReq) {
      var r: tCommitReq;
      r = req;
      r.caller = this;
      send target, eTxCommit, r;
    }
    on eTxCancel do (req: tCancelReq) {
      var r: tCancelReq;
      r = req;
      r.caller = this;
      send target, eTxCancel, r;
    }

    on ePutItemResp do (resp: tPutItemResp) { send caller, ePutItemResp, resp; raise halt; }
    on eDeleteItemResp do (resp: tDeleteItemResp) { send caller, eDeleteItemResp, resp; raise halt; }
    on eGetItemResp do (resp: tGetItemResp) { send caller, eGetItemResp, resp; raise halt; }
    on eInitiateWriteResp do (resp: tInitiateWriteResp) { send caller, eInitiateWriteResp, resp; raise halt; }
    on eTxPrepareResp do (resp: tPrepareResp) { send caller, eTxPrepareResp, resp; raise halt; }
    on eTxCommitResp do (resp: tCommitResp) { send caller, eTxCommitResp, resp; raise halt; }
    on eTxCancelResp do (resp: tCancelResp) { send caller, eTxCancelResp, resp; raise halt; }
  }
}
