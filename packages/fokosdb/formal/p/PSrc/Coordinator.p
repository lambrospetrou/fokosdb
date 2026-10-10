enum tTcState { CREATED, PREPARING, COMMITTING, COMMITTED, CANCELLING, CANCELLED }

// One row of `tc_participants`, with the operations of its `tc_items` rows.
type tTcParticipant = (items: seq[tTxOp], prepare: tPrepareOutcome, committed: bool, cancelled: bool);

// One row of `tc_state`, with the rows of its participants.
type tTcTransaction = (token: int, tcState: tTcState, participants: map[machine, tTcParticipant]);

// The SQLite state of the coordinator. `byToken` is the index of `tc_state` on the token.
type tCoordinatorDurable = (txs: map[int, tTcTransaction], byToken: map[int, int], nextTx: int);

// A drive in progress: the request that waits for the answer, and the calls that the drive waits
// for. `allAccepted` is the result of the prepares in memory.
type tDrive = (caller: machine, callerReqId: int, tx: int, waiting: set[machine], allAccepted: bool,
               confirmed: set[machine], answered: bool);

// TransactionCoordinatorDO. Each handler is one block of the code. A storage call does not end a
// block: an input gate holds every other event while the storage call runs.
machine Coordinator {
  var bugs: tBugs;
  var topology: tTopology;
  var durable: tCoordinatorDurable;
  var drives: map[int, tDrive];
  var nextDrive: int;

  start state Init {
    entry (cfg: (bugs: tBugs, topology: tTopology)) {
      bugs = cfg.bugs;
      topology = cfg.topology;
      durable.nextTx = 1;
      goto Serving;
    }
  }

  state Serving {
    // initiateWriteLocal: groups the operations by the root partition of each key, inserts the
    // transaction in CREATED, and calls drivePrepare.
    on eInitiateWrite do (req: tInitiateWriteReq) {
      var tx: int;
      var t: tTcTransaction;
      var p: tTcParticipant;
      var root: machine;
      var op: tTxOp;
      assert !(req.token in durable.byToken), format("resumeTransaction is not modeled: token {0} is used again", req.token);
      tx = durable.nextTx;
      durable.nextTx = tx + 1;
      t.token = req.token;
      t.tcState = CREATED;
      foreach (op in req.items) {
        root = RootOf(topology, op.key);
        if (root in t.participants) {
          p = t.participants[root];
        } else {
          p = default(tTcParticipant);
        }
        p.items += (sizeof(p.items), op);
        t.participants[root] = p;
      }
      durable.txs[tx] = t;
      durable.byToken[req.token] = tx;
      DrivePrepare(req.caller, req.reqId, tx);
    }

    // The answer of one txPrepare: storePrepareAnswer. When the last answer arrives, the drive
    // decides.
    on eTxPrepareResp do (resp: tPrepareResp) {
      var d: tDrive;
      var t: tTcTransaction;
      var p: tTcParticipant;
      d = drives[resp.reqId];
      t = durable.txs[d.tx];
      if (t.tcState == PREPARING && t.participants[resp.partition].prepare == PREPARE_NONE) {
        p = t.participants[resp.partition];
        p.prepare = resp.outcome;
        t.participants[resp.partition] = p;
        durable.txs[d.tx] = t;
      }
      d.waiting -= (resp.partition);
      if (bugs.W1) {
        d.allAccepted = d.allAccepted || resp.outcome == PREPARE_ACCEPTED;
      } else {
        d.allAccepted = d.allAccepted && resp.outcome == PREPARE_ACCEPTED;
      }
      drives[resp.reqId] = d;
      if (sizeof(d.waiting) > 0) {
        return;
      }
      if (d.allAccepted) {
        MarkCommitting(d.tx);
        if (bugs.W3 && durable.txs[d.tx].tcState == COMMITTING) {
          Answer(resp.reqId);
        }
        RunCommit(resp.reqId);
      } else {
        CancelTransactionInStore(d.tx);
        RunCancel(resp.reqId);
      }
    }

    // The answer of one txCommit. When the last answer arrives, runCommit completes the
    // transaction if every participant confirmed.
    on eTxCommitResp do (resp: tCommitResp) {
      var d: tDrive;
      var t: tTcTransaction;
      var p: tTcParticipant;
      var part: machine;
      var uncommitted: int;
      d = drives[resp.reqId];
      d.waiting -= (resp.partition);
      if (resp.ok) {
        d.confirmed += (resp.partition);
      }
      drives[resp.reqId] = d;
      if (sizeof(d.waiting) > 0) {
        return;
      }
      t = durable.txs[d.tx];
      if (t.tcState == COMMITTING) {
        foreach (part in d.confirmed) {
          p = t.participants[part];
          p.committed = true;
          t.participants[part] = p;
        }
        durable.txs[d.tx] = t;
      }
      foreach (part in keys(t.participants)) {
        if (!t.participants[part].committed) {
          uncommitted = uncommitted + 1;
        }
      }
      if (uncommitted == 0) {
        CompleteTransaction(d.tx, COMMITTED);
      }
      Answer(resp.reqId);
    }

    // The answer of one txCancel: runCancel stores the cancel outcome. When the last answer
    // arrives, it completes the transaction if every participant has an outcome.
    on eTxCancelResp do (resp: tCancelResp) {
      var d: tDrive;
      var t: tTcTransaction;
      var p: tTcParticipant;
      var part: machine;
      var pending: int;
      d = drives[resp.reqId];
      d.waiting -= (resp.partition);
      drives[resp.reqId] = d;
      t = durable.txs[d.tx];
      p = t.participants[resp.partition];
      p.cancelled = true;
      t.participants[resp.partition] = p;
      durable.txs[d.tx] = t;
      if (sizeof(d.waiting) > 0) {
        return;
      }
      foreach (part in keys(t.participants)) {
        if (!t.participants[part].committed && !t.participants[part].cancelled) {
          pending = pending + 1;
        }
      }
      if (pending == 0) {
        CompleteTransaction(d.tx, CANCELLED);
      }
      Answer(resp.reqId);
    }
  }

  // drivePrepare: writes PREPARING, reads the state again, and sends one txPrepare to each
  // participant. When another drive has already decided, it answers from the stored state.
  fun DrivePrepare(caller: machine, callerReqId: int, tx: int) {
    var t: tTcTransaction;
    var part: machine;
    var id: int;
    t = durable.txs[tx];
    if (t.tcState == CREATED) {
      t.tcState = PREPARING;
      durable.txs[tx] = t;
    }
    nextDrive = nextDrive + 1;
    id = nextDrive;
    drives[id] = (caller = caller, callerReqId = callerReqId, tx = tx, waiting = default(set[machine]),
                  allAccepted = !bugs.W1, confirmed = default(set[machine]), answered = false);
    if (t.tcState != PREPARING) {
      Answer(id);
      return;
    }
    foreach (part in keys(t.participants)) {
      drives[id].waiting += (part);
      send NewRpc(this, part), eTxPrepare, (caller = this, reqId = id, tx = tx, items = t.participants[part].items);
    }
  }

  // markCommitting: writes COMMITTING only from PREPARING, and only when every stored prepare
  // answer is accepted.
  fun MarkCommitting(tx: int) {
    var t: tTcTransaction;
    var part: machine;
    var accepted: int;
    t = durable.txs[tx];
    if (t.tcState != PREPARING) {
      return;
    }
    foreach (part in keys(t.participants)) {
      if (t.participants[part].prepare == PREPARE_ACCEPTED) {
        accepted = accepted + 1;
      }
    }
    if ((bugs.W1 && accepted > 0) || accepted == sizeof(t.participants)) {
      t.tcState = COMMITTING;
      durable.txs[tx] = t;
      announce eDecision, (tx = tx, commit = true, itemKeys = KeysOf(t));
    }
  }

  // cancelTransactionInStore: writes CANCELLING only from PREPARING.
  fun CancelTransactionInStore(tx: int) {
    var t: tTcTransaction;
    t = durable.txs[tx];
    if (t.tcState != PREPARING) {
      return;
    }
    t.tcState = CANCELLING;
    durable.txs[tx] = t;
    announce eDecision, (tx = tx, commit = false, itemKeys = KeysOf(t));
  }

  // runCommit: sends txCommit, with the keys only, to each participant with no stored commit
  // outcome, and only when the stored state is COMMITTING.
  fun RunCommit(id: int) {
    var d: tDrive;
    var t: tTcTransaction;
    var part: machine;
    d = drives[id];
    t = durable.txs[d.tx];
    if (t.tcState != COMMITTING) {
      Answer(id);
      return;
    }
    foreach (part in keys(t.participants)) {
      if (!t.participants[part].committed) {
        d.waiting += (part);
        send NewRpc(this, part), eTxCommit, (caller = this, reqId = id, tx = d.tx, itemKeys = KeysOfParticipant(t.participants[part]));
      }
    }
    drives[id] = d;
    if (sizeof(d.waiting) == 0) {
      CompleteTransaction(d.tx, COMMITTED);
      Answer(id);
    }
  }

  // runCancel: sends txCancel to each participant with no stored cancel outcome, and only when the
  // stored state is CANCELLING.
  fun RunCancel(id: int) {
    var d: tDrive;
    var t: tTcTransaction;
    var part: machine;
    d = drives[id];
    t = durable.txs[d.tx];
    if (t.tcState != CANCELLING) {
      Answer(id);
      return;
    }
    foreach (part in keys(t.participants)) {
      if (!t.participants[part].cancelled) {
        d.waiting += (part);
        send NewRpc(this, part), eTxCancel, (caller = this, reqId = id, tx = d.tx, itemKeys = KeysOfParticipant(t.participants[part]));
      }
    }
    drives[id] = d;
    if (sizeof(d.waiting) == 0) {
      CompleteTransaction(d.tx, CANCELLED);
      Answer(id);
    }
  }

  // completeTransaction: writes the terminal state only from COMMITTING or CANCELLING.
  fun CompleteTransaction(tx: int, terminal: tTcState) {
    var t: tTcTransaction;
    t = durable.txs[tx];
    if ((terminal == COMMITTED && t.tcState != COMMITTING) || (terminal == CANCELLED && t.tcState != CANCELLING)) {
      return;
    }
    t.tcState = terminal;
    durable.txs[tx] = t;
    announce eTxCompleted, (tx = tx, committed = terminal == COMMITTED);
  }

  // loadFinalResponse: answers the request of the drive from the stored state, one time.
  fun Answer(id: int) {
    var d: tDrive;
    var st: tTcState;
    var answer: tWriteAnswer;
    d = drives[id];
    if (d.answered) {
      return;
    }
    st = durable.txs[d.tx].tcState;
    if (st == COMMITTED || (bugs.W3 && st == COMMITTING)) {
      answer = ANSWER_COMMITTED;
    } else if (st == COMMITTING) {
      answer = ANSWER_COMMIT_PENDING;
    } else if (st == CANCELLING || st == CANCELLED) {
      answer = ANSWER_CANCELLED;
    } else {
      answer = ANSWER_UNDECIDED;
    }
    d.answered = true;
    drives[id] = d;
    send d.caller, eInitiateWriteResp, (reqId = d.callerReqId, tx = d.tx, answer = answer);
  }
}

fun KeysOfParticipant(p: tTcParticipant): seq[tKey] {
  var ks: seq[tKey];
  var op: tTxOp;
  foreach (op in p.items) {
    ks += (sizeof(ks), op.key);
  }
  return ks;
}

fun KeysOf(t: tTcTransaction): set[tKey] {
  var ks: set[tKey];
  var p: tTcParticipant;
  var op: tTxOp;
  foreach (p in values(t.participants)) {
    foreach (op in p.items) {
      ks += (op.key);
    }
  }
  return ks;
}
