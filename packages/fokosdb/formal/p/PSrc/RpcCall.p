type tRpcConfig = (caller: machine, target: machine, env: machine);

// The request of a call whose answer got lost reaches the target in a later handler of the call.
event eDeliverLate;

// Starts one call. The caller sends the request event to the machine that this returns.
fun NewRpc(caller: machine, target: machine, env: machine): machine {
  send env, eRpcOpened;
  return new RpcCall((caller = caller, target = target, env = env));
}

// One Workers RPC call: the request to the target, and the answer back to the caller. Workers RPC
// keeps no order between calls, so each call has its own machine, and the checker can deliver the
// request and the answer of each call at any point relative to the other calls. The Environment
// counts the open calls.
//
// The Environment can lose the request or the answer. The caller then gets eRpcFailed. When the
// answer gets lost, the request still reaches the target, before or after the caller sees the error.
machine RpcCall {
  var caller: machine;
  var target: machine;
  var env: machine;
  var reqId: int;
  var lateEvent: event;
  var latePayload: any;

  start state Calling {
    entry (c: tRpcConfig) {
      caller = c.caller;
      target = c.target;
      env = c.env;
    }

    on ePutItem do (req: tPutItemReq) {
      var r: tPutItemReq;
      r = req;
      r.caller = this;
      Deliver(r.reqId, ePutItem, r);
    }
    on eDeleteItem do (req: tDeleteItemReq) {
      var r: tDeleteItemReq;
      r = req;
      r.caller = this;
      Deliver(r.reqId, eDeleteItem, r);
    }
    on eGetItem do (req: tGetItemReq) {
      var r: tGetItemReq;
      r = req;
      r.caller = this;
      Deliver(r.reqId, eGetItem, r);
    }
    on eInitiateWrite do (req: tInitiateWriteReq) {
      var r: tInitiateWriteReq;
      r = req;
      r.caller = this;
      Deliver(r.reqId, eInitiateWrite, r);
    }
    on eTxPrepare do (req: tPrepareReq) {
      var r: tPrepareReq;
      r = req;
      r.caller = this;
      Deliver(r.reqId, eTxPrepare, r);
    }
    on eTxCommit do (req: tCommitReq) {
      var r: tCommitReq;
      r = req;
      r.caller = this;
      Deliver(r.reqId, eTxCommit, r);
    }
    on eTxCancel do (req: tCancelReq) {
      var r: tCancelReq;
      r = req;
      r.caller = this;
      Deliver(r.reqId, eTxCancel, r);
    }
    on eRecover do (req: tRecoverReq) {
      var r: tRecoverReq;
      r = req;
      r.caller = this;
      Deliver(r.reqId, eRecover, r);
    }
    on eForceResolve do (req: tForceResolveReq) {
      var r: tForceResolveReq;
      r = req;
      r.caller = this;
      Deliver(r.reqId, eForceResolve, r);
    }

    on ePutItemResp do (resp: tPutItemResp) { send caller, ePutItemResp, resp; Close(); }
    on eDeleteItemResp do (resp: tDeleteItemResp) { send caller, eDeleteItemResp, resp; Close(); }
    on eGetItemResp do (resp: tGetItemResp) { send caller, eGetItemResp, resp; Close(); }
    on eInitiateWriteResp do (resp: tInitiateWriteResp) { send caller, eInitiateWriteResp, resp; Close(); }
    on eTxPrepareResp do (resp: tPrepareResp) { send caller, eTxPrepareResp, resp; Close(); }
    on eTxCommitResp do (resp: tCommitResp) { send caller, eTxCommitResp, resp; Close(); }
    on eTxCancelResp do (resp: tCancelResp) { send caller, eTxCancelResp, resp; Close(); }
    on eRecoverResp do (resp: tRecoverResp) { send caller, eRecoverResp, resp; Close(); }
    on eForceResolveResp do (resp: tForceResolveResp) { send caller, eForceResolveResp, resp; Close(); }

    on eRpcBroken do {
      send caller, eRpcFailed, (reqId = reqId, target = target);
      Close();
    }
  }

  // The answer got lost: the caller already has the error. The request is still in flight, so later
  // calls of the caller can reach the target before it.
  state AnswerLost {
    on eDeliverLate do {
      send target, lateEvent, latePayload;
    }
    on ePutItemResp, eDeleteItemResp, eGetItemResp, eInitiateWriteResp, eTxPrepareResp, eTxCommitResp,
      eTxCancelResp, eRecoverResp, eForceResolveResp, eRpcBroken goto Done;
  }

  state Done {
    entry {
      Close();
    }
  }

  fun Close() {
    send env, eRpcClosed;
    raise halt;
  }

  fun Deliver(id: int, ev: event, payload: any) {
    var loss: tLoss;
    reqId = id;
    send env, eMayLose, (rpc = this, target = target);
    receive {
      case eLossDecision: (d: tLoss) { loss = d; }
    }
    if (loss == LOSE_REQUEST) {
      send caller, eRpcFailed, (reqId = reqId, target = target);
      Close();
    }
    if (loss == LOSE_ANSWER) {
      send caller, eRpcFailed, (reqId = reqId, target = target);
      lateEvent = ev;
      latePayload = payload;
      send this, eDeliverLate;
      goto AnswerLost;
    }
    send target, ev, payload;
  }
}
