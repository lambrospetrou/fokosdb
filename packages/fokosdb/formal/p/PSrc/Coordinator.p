enum tTcState { CREATED, PREPARING, COMMITTING, COMMITTED, CANCELLING, CANCELLED }

// One row of `tc_participants`, with the operations of its `tc_items` rows.
type tTcParticipant = (items: seq[tTxOp], prepare: tPrepareOutcome, committed: bool, cancelled: bool);

// One row of `tc_state`, with the rows of its participants. `ts` is transaction_ts. completedAt is -1
// until the completion.
type tTcTransaction = (token: int, tcState: tTcState, ts: int, createdAt: int, completedAt: int, nextRecoveryAt: int,
                       participants: map[machine, tTcParticipant]);

// The SQLite state of the coordinator. `byToken` is the index of `tc_state` on the token. The alarm
// is durable too.
type tCoordinatorDurable = (txs: map[int, tTcTransaction], byToken: map[int, int], nextTx: int, alarmArmed: bool);

// The functions of the code that a drive runs.
enum tDriveKind { DRIVE_PREPARE, DRIVE_PREPARE_RECOVERY, DRIVE_COMMIT, DRIVE_CANCEL }

// A drive in progress: drivePrepare, runPrepareRecovery, runCommit, or runCancel for one
// transaction. `caller` is null for a drive of the tx_recovery job. `waiting` holds the attempt of
// each call that the drive waits for. `allAccepted` is the result of the prepares in memory.
type tDrive = (caller: machine, callerReqId: int, tx: int, kind: tDriveKind, waiting: map[machine, int],
               allAccepted: bool, confirmed: set[machine], answered: bool, inStep: bool);

// `recoverBlackout`: the coordinator drops each recoverTransactionForParticipant for a transaction
// that its ledger still holds.
type tCoordinatorConfig = (bugs: tBugs, topology: tTopology, env: machine, clock: machine, recoverBlackout: bool);

// TransactionCoordinatorDO. Each handler is one block of the code. A storage call does not end a
// block: an input gate holds every other event while the storage call runs.
machine Coordinator {
  var bugs: tBugs;
  var topology: tTopology;
  var env: machine;
  var clock: machine;
  var recoverBlackout: bool;
  var durable: tCoordinatorDurable;
  var now: int;
  var drives: map[int, tDrive];
  // The drives of the tx_recovery step in progress.
  var stepDrives: set[int];
  var stepRunning: bool;
  // An eAlarm waits in the inbox.
  var alarmQueued: bool;
  // The ids of the drives. A model value, not a value of the code: a restart keeps it, so that an
  // answer to a call from before the restart does not match a new drive.
  var nextDrive: int;
  // The outcome of each completed transaction, as the log line of the completion. The sweep and a
  // restart keep it.
  var completionLog: map[int, bool];

  start state Init {
    entry (cfg: tCoordinatorConfig) {
      bugs = cfg.bugs;
      topology = cfg.topology;
      env = cfg.env;
      clock = cfg.clock;
      recoverBlackout = cfg.recoverBlackout;
      durable.nextTx = 1;
      goto Serving;
    }
  }

  state Serving {
    // An eviction or a crash between two handlers: the memory state goes, SQLite and the alarm stay.
    // Each request that waits for its answer gets an error.
    on eRestart do {
      var d: tDrive;
      foreach (d in values(drives)) {
        if (d.caller != null && !d.answered) {
          send d.caller, eRpcBroken;
        }
      }
      drives = default(map[int, tDrive]);
      stepDrives = default(set[int]);
      stepRunning = false;
      CheckAlarm();
    }

    on eTick do (t: int) {
      now = t;
      CheckAlarm();
    }

    // initiateWriteLocal: a token with a stored row resumes that transaction (resumeTransaction).
    // Otherwise it groups the operations by the root partition of each key, inserts the transaction
    // in CREATED, and calls drivePrepare.
    on eInitiateWrite do (req: tInitiateWriteReq) {
      var tx: int;
      var t: tTcTransaction;
      var p: tTcParticipant;
      var root: machine;
      var op: tTxOp;
      var id: int;
      if (req.token in durable.byToken && !bugs.tokenRowIgnored) {
        tx = durable.byToken[req.token];
        id = NewDrive(req.caller, req.reqId, tx, DRIVE_PREPARE, false);
        ResumeTransaction(id);
        return;
      }
      tx = durable.nextTx;
      durable.nextTx = tx + 1;
      t.token = req.token;
      t.tcState = CREATED;
      t.ts = tx;
      t.createdAt = now;
      t.completedAt = -1;
      t.nextRecoveryAt = now + STALE_TRANSACTION();
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
      announce eTxCreated, tx;
      UpdateAlarm();
      id = NewDrive(req.caller, req.reqId, tx, DRIVE_PREPARE, false);
      DrivePrepare(id);
    }

    // The answer of one txPrepare: storePrepareAnswer keeps the first answer of each participant,
    // and only in PREPARING.
    on eTxPrepareResp do (resp: tPrepareResp) {
      var d: tDrive;
      var t: tTcTransaction;
      var p: tTcParticipant;
      if (!(resp.reqId in drives) || !(resp.partition in drives[resp.reqId].waiting)) {
        return;
      }
      d = drives[resp.reqId];
      if (!(d.tx in durable.txs)) {
        Abandon(resp.reqId);
        return;
      }
      t = durable.txs[d.tx];
      if (t.tcState == PREPARING && t.participants[resp.partition].prepare == PREPARE_NONE) {
        p = t.participants[resp.partition];
        p.prepare = resp.outcome;
        t.participants[resp.partition] = p;
        durable.txs[d.tx] = t;
      }
      d.waiting -= (resp.partition);
      if (bugs.commitOnOneAccept) {
        d.allAccepted = d.allAccepted || resp.outcome == PREPARE_ACCEPTED;
      } else {
        d.allAccepted = d.allAccepted && resp.outcome == PREPARE_ACCEPTED;
      }
      drives[resp.reqId] = d;
      if (sizeof(d.waiting) == 0) {
        Settle(resp.reqId);
      }
    }

    // The answer of one txCommit.
    on eTxCommitResp do (resp: tCommitResp) {
      var d: tDrive;
      if (!(resp.reqId in drives) || !(resp.partition in drives[resp.reqId].waiting)) {
        return;
      }
      d = drives[resp.reqId];
      if (!(d.tx in durable.txs)) {
        Abandon(resp.reqId);
        return;
      }
      d.waiting -= (resp.partition);
      if (resp.ok) {
        d.confirmed += (resp.partition);
      }
      drives[resp.reqId] = d;
      if (sizeof(d.waiting) == 0) {
        Settle(resp.reqId);
      }
    }

    // The answer of one txCancel: runCancel stores the cancel outcome of the participant.
    on eTxCancelResp do (resp: tCancelResp) {
      var d: tDrive;
      var t: tTcTransaction;
      var p: tTcParticipant;
      if (!(resp.reqId in drives) || !(resp.partition in drives[resp.reqId].waiting)) {
        return;
      }
      d = drives[resp.reqId];
      if (!(d.tx in durable.txs)) {
        Abandon(resp.reqId);
        return;
      }
      d.waiting -= (resp.partition);
      drives[resp.reqId] = d;
      t = durable.txs[d.tx];
      p = t.participants[resp.partition];
      p.cancelled = true;
      t.participants[resp.partition] = p;
      durable.txs[d.tx] = t;
      if (sizeof(d.waiting) == 0) {
        Settle(resp.reqId);
      }
    }

    // A call of a drive failed. The retry policy of the call sends it again, or the drive counts it as
    // failed: a prepare that is not accepted, a commit or a cancel that is not confirmed.
    on eRpcFailed do (f: tRpcFailed) {
      var d: tDrive;
      var attempt: int;
      if (!(f.reqId in drives) || !(f.target in drives[f.reqId].waiting)) {
        return;
      }
      d = drives[f.reqId];
      if (!(d.tx in durable.txs)) {
        Abandon(f.reqId);
        return;
      }
      attempt = d.waiting[f.target];
      if (attempt < MaxAttempts(d.kind)) {
        d.waiting[f.target] = attempt + 1;
        drives[f.reqId] = d;
        SendCall(f.reqId, f.target);
        return;
      }
      d.waiting -= (f.target);
      if (d.kind == DRIVE_PREPARE) {
        d.allAccepted = bugs.commitOnOneAccept && d.allAccepted;
      }
      drives[f.reqId] = d;
      if (sizeof(d.waiting) == 0) {
        Settle(f.reqId);
      }
    }

    // recoverTransactionLocal: answers from the ledger and does not drive the transaction. For a
    // transaction that is not complete, it makes the transaction and the tx_recovery job due now.
    on eRecover do (req: tRecoverReq) {
      var t: tTcTransaction;
      var ledgerState: tRecoverState;
      if (recoverBlackout && req.tx in durable.txs) {
        send req.caller, eRpcBroken;
        return;
      }
      if (!(req.tx in durable.txs)) {
        ledgerState = RECOVER_NOT_FOUND;
      } else {
        t = durable.txs[req.tx];
        if (t.tcState == COMMITTED) {
          ledgerState = RECOVER_COMMITTED;
        } else if (t.tcState == CANCELLED) {
          ledgerState = RECOVER_CANCELLED;
        } else {
          if (now < t.nextRecoveryAt) {
            t.nextRecoveryAt = now;
            durable.txs[req.tx] = t;
          }
          UpdateAlarm();
          ledgerState = RECOVER_DRIVING;
        }
      }
      send req.caller, eRecoverResp, (reqId = req.reqId, ledgerState = ledgerState);
    }

    // The alarm runs the idempotency_sweep job, then one step of the tx_recovery job.
    on eAlarm do {
      alarmQueued = false;
      if (stepRunning) {
        return;
      }
      SweepExpiredTransactions();
      RecoverStaleTransactions();
      UpdateAlarm();
    }

    // The log of the completions, which an operator reads.
    on eLogLookup do (q: tLogLookup) {
      if (q.tx in completionLog) {
        send q.operator, eLogEntry, (tx = q.tx, known = true, committed = completionLog[q.tx]);
      } else {
        send q.operator, eLogEntry, (tx = q.tx, known = false, committed = false);
      }
    }
  }

  fun NewDrive(caller: machine, callerReqId: int, tx: int, kind: tDriveKind, inStep: bool): int {
    nextDrive = nextDrive + 1;
    drives[nextDrive] = (caller = caller, callerReqId = callerReqId, tx = tx, kind = kind,
                         waiting = default(map[machine, int]), allAccepted = !bugs.commitOnOneAccept,
                         confirmed = default(set[machine]), answered = false, inStep = inStep);
    return nextDrive;
  }

  fun MaxAttempts(kind: tDriveKind): int {
    if (kind == DRIVE_PREPARE) {
      return PREPARE_MAX_ATTEMPTS();
    }
    if (kind == DRIVE_PREPARE_RECOVERY) {
      return PREPARE_RECOVERY_MAX_ATTEMPTS();
    }
    return FANOUT_MAX_ATTEMPTS();
  }

  // One attempt of the call of a drive to one participant, in a new RpcCall.
  fun SendCall(id: int, part: machine) {
    var d: tDrive;
    var t: tTcTransaction;
    d = drives[id];
    t = durable.txs[d.tx];
    if (d.kind == DRIVE_PREPARE || d.kind == DRIVE_PREPARE_RECOVERY) {
      send NewRpc(this, part, env), eTxPrepare,
        (caller = this, reqId = id, coordinator = this, tx = d.tx, ts = t.ts, items = t.participants[part].items);
    } else if (d.kind == DRIVE_COMMIT) {
      send NewRpc(this, part, env), eTxCommit,
        (caller = this, reqId = id, tx = d.tx, ts = t.ts, itemKeys = KeysOfParticipant(t.participants[part]));
    } else {
      send NewRpc(this, part, env), eTxCancel,
        (caller = this, reqId = id, tx = d.tx, itemKeys = KeysOfParticipant(t.participants[part]));
    }
  }

  fun Start(id: int, kind: tDriveKind, parts: set[machine]) {
    var d: tDrive;
    var part: machine;
    d = drives[id];
    d.kind = kind;
    d.waiting = default(map[machine, int]);
    d.confirmed = default(set[machine]);
    foreach (part in parts) {
      d.waiting[part] = 1;
    }
    drives[id] = d;
    foreach (part in parts) {
      SendCall(id, part);
    }
    if (sizeof(parts) == 0) {
      Settle(id);
    }
  }

  // resumeTransaction: continues the stored transaction of the token from its stored state.
  fun ResumeTransaction(id: int) {
    var st: tTcState;
    st = durable.txs[drives[id].tx].tcState;
    if (st == CREATED) {
      DrivePrepare(id);
    } else if (st == PREPARING) {
      RunPrepareRecovery(id);
    } else if (st == COMMITTING) {
      RunCommit(id);
    } else if (st == CANCELLING) {
      RunCancel(id);
    } else {
      Finish(id);
    }
  }

  // drivePrepare: writes PREPARING, reads the state again, and sends one txPrepare to each
  // participant. When another drive has already decided, it answers from the stored state.
  fun DrivePrepare(id: int) {
    var t: tTcTransaction;
    var tx: int;
    tx = drives[id].tx;
    t = durable.txs[tx];
    if (t.tcState == CREATED) {
      t.tcState = PREPARING;
      durable.txs[tx] = t;
    }
    if (t.tcState != PREPARING) {
      Finish(id);
      return;
    }
    Start(id, DRIVE_PREPARE, PartsOf(t, false));
  }

  // The participants of a transaction, or only those with no stored prepare answer.
  fun PartsOf(t: tTcTransaction, unansweredOnly: bool): set[machine] {
    var part: machine;
    var parts: set[machine];
    foreach (part in keys(t.participants)) {
      if (!unansweredOnly || t.participants[part].prepare == PREPARE_NONE) {
        parts += (part);
      }
    }
    return parts;
  }

  // runPrepareRecovery: re-prepares each participant with no stored answer.
  fun RunPrepareRecovery(id: int) {
    var t: tTcTransaction;
    t = durable.txs[drives[id].tx];
    if (t.tcState != PREPARING) {
      Finish(id);
      return;
    }
    Start(id, DRIVE_PREPARE_RECOVERY, PartsOf(t, true));
  }

  // runCommit: sends txCommit, with the keys only, to each participant with no stored commit
  // outcome, and only when the stored state is COMMITTING.
  fun RunCommit(id: int) {
    var t: tTcTransaction;
    var part: machine;
    var parts: set[machine];
    t = durable.txs[drives[id].tx];
    if (t.tcState != COMMITTING) {
      Finish(id);
      return;
    }
    foreach (part in keys(t.participants)) {
      if (!t.participants[part].committed) {
        parts += (part);
      }
    }
    Start(id, DRIVE_COMMIT, parts);
  }

  // runCancel: sends txCancel to each participant with no stored cancel outcome, and only when the
  // stored state is CANCELLING.
  fun RunCancel(id: int) {
    var t: tTcTransaction;
    var part: machine;
    var parts: set[machine];
    t = durable.txs[drives[id].tx];
    if (t.tcState != CANCELLING && !bugs.cancelInAnyState) {
      Finish(id);
      return;
    }
    foreach (part in keys(t.participants)) {
      if (!t.participants[part].cancelled) {
        parts += (part);
      }
    }
    Start(id, DRIVE_CANCEL, parts);
  }

  // The block after the last call of a drive.
  fun Settle(id: int) {
    var d: tDrive;
    var t: tTcTransaction;
    var p: tTcParticipant;
    var part: machine;
    var open: int;
    var rejected: bool;
    var accepted: bool;
    d = drives[id];
    t = durable.txs[d.tx];
    if (d.kind == DRIVE_PREPARE) {
      // drivePrepare: every answer in memory is accepted, so markCommitting checks the stored
      // answers again. Otherwise cancelTransactionInStore.
      if (d.allAccepted) {
        MarkCommitting(d.tx);
        if (bugs.committedBeforeApply && durable.txs[d.tx].tcState == COMMITTING) {
          Answer(id);
        }
        RunCommit(id);
      } else {
        CancelTransactionInStore(d.tx);
        RunCancel(id);
      }
    } else if (d.kind == DRIVE_PREPARE_RECOVERY) {
      // runPrepareRecovery reads the stored answers. It cancels on a rejection, or when the
      // transaction stayed in PREPARING longer than maxPreparingHoldMs. Otherwise it leaves the
      // transaction in PREPARING.
      accepted = true;
      foreach (p in values(t.participants)) {
        accepted = accepted && p.prepare == PREPARE_ACCEPTED;
        rejected = rejected || p.prepare == PREPARE_REJECTED;
      }
      if (accepted) {
        MarkCommitting(d.tx);
        RunCommit(id);
      } else if (rejected || (!bugs.noPreparingHold && now - t.createdAt > MAX_PREPARING_HOLD())) {
        CancelTransactionInStore(d.tx);
        RunCancel(id);
      } else {
        Finish(id);
      }
    } else if (d.kind == DRIVE_COMMIT) {
      // runCommit stores the confirmed outcomes, and completes when no participant is left.
      if (t.tcState == COMMITTING) {
        foreach (part in d.confirmed) {
          p = t.participants[part];
          p.committed = true;
          t.participants[part] = p;
        }
        durable.txs[d.tx] = t;
      }
      foreach (p in values(t.participants)) {
        if (!p.committed) {
          open = open + 1;
        }
      }
      if (open == 0) {
        CompleteTransaction(d.tx, COMMITTED);
      }
      Finish(id);
    } else {
      // runCancel completes when every participant has an outcome.
      foreach (p in values(t.participants)) {
        if (!p.committed && !p.cancelled) {
          open = open + 1;
        }
      }
      if (open == 0) {
        CompleteTransaction(d.tx, CANCELLED);
      }
      Finish(id);
    }
  }

  // markCommitting: writes COMMITTING only from PREPARING, and only when every stored prepare
  // answer is accepted.
  fun MarkCommitting(tx: int) {
    var t: tTcTransaction;
    var p: tTcParticipant;
    var accepted: int;
    t = durable.txs[tx];
    if (t.tcState != PREPARING) {
      return;
    }
    foreach (p in values(t.participants)) {
      if (p.prepare == PREPARE_ACCEPTED) {
        accepted = accepted + 1;
      }
    }
    if ((bugs.commitOnOneAccept && accepted > 0) || accepted == sizeof(t.participants)) {
      t.tcState = COMMITTING;
      durable.txs[tx] = t;
      announce eDecision, (tx = tx, token = t.token, commit = true, itemKeys = KeysOf(t));
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
    announce eDecision, (tx = tx, token = t.token, commit = false, itemKeys = KeysOf(t));
  }

  // completeTransaction: writes the terminal state and completed_at only from COMMITTING or
  // CANCELLING, and logs the outcome.
  fun CompleteTransaction(tx: int, terminal: tTcState) {
    var t: tTcTransaction;
    t = durable.txs[tx];
    if ((terminal == COMMITTED && t.tcState != COMMITTING) || (terminal == CANCELLED && t.tcState != CANCELLING)) {
      return;
    }
    t.tcState = terminal;
    t.completedAt = now;
    durable.txs[tx] = t;
    completionLog[tx] = terminal == COMMITTED;
    announce eTxCompleted, (tx = tx, committed = terminal == COMMITTED);
    UpdateAlarm();
  }

  // A drive whose transaction the idempotency_sweep job deleted: loadFinalResponse finds no row and
  // throws, so the request gets an error.
  fun Abandon(id: int) {
    var d: tDrive;
    d = drives[id];
    if (d.caller != null && !d.answered) {
      send d.caller, eRpcBroken;
    }
    d.answered = true;
    drives[id] = d;
    Finish(id);
  }

  // The end of a drive: the request gets its answer, and the tx_recovery step counts the drive.
  fun Finish(id: int) {
    var d: tDrive;
    d = drives[id];
    if (d.caller != null) {
      Answer(id);
    }
    drives -= (id);
    if (d.inStep) {
      stepDrives -= (id);
      if (sizeof(stepDrives) == 0) {
        stepRunning = false;
        UpdateAlarm();
      }
    }
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
    if (st == COMMITTED || (bugs.committedBeforeApply && st == COMMITTING)) {
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

  // sweepExpiredTransactions: deletes each transaction whose idempotency window has passed since
  // its completion.
  fun SweepExpiredTransactions() {
    var tx: int;
    var t: tTcTransaction;
    foreach (tx in keys(durable.txs)) {
      t = durable.txs[tx];
      if (t.completedAt >= 0 && now - t.completedAt > IDEMPOTENCY_WINDOW()) {
        durable.txs -= (tx);
        if (durable.byToken[t.token] == tx) {
          durable.byToken -= (t.token);
          announce eTokenSwept, t.token;
        }
      }
    }
  }

  // recoverStaleTransactions: claims each transaction that is not complete and is due, moves its
  // next_recovery_at forward, and drives it from its stored state (driveTransaction). The step ends
  // when its last drive ends.
  fun RecoverStaleTransactions() {
    var tx: int;
    var t: tTcTransaction;
    var ids: seq[int];
    var id: int;
    var noCaller: machine;
    foreach (tx in keys(durable.txs)) {
      t = durable.txs[tx];
      if (t.completedAt < 0 && t.nextRecoveryAt <= now) {
        t.nextRecoveryAt = NextRecoveryAt(now, t.createdAt);
        durable.txs[tx] = t;
        id = NewDrive(noCaller, 0, tx, DRIVE_PREPARE, true);
        stepDrives += (id);
        ids += (sizeof(ids), id);
      }
    }
    if (sizeof(ids) == 0) {
      return;
    }
    stepRunning = true;
    foreach (id in ids) {
      ResumeTransaction(id);
    }
  }

  // The deadline of the jobs: the earliest next_recovery_at of a transaction that is not complete,
  // and the end of the idempotency window of a completed one.
  fun HasDeadline(): bool {
    return sizeof(durable.txs) > 0;
  }

  fun IsDue(): bool {
    var t: tTcTransaction;
    foreach (t in values(durable.txs)) {
      if ((t.completedAt < 0 && t.nextRecoveryAt <= now) || (t.completedAt >= 0 && now - t.completedAt > IDEMPOTENCY_WINDOW())) {
        return true;
      }
    }
    return false;
  }

  fun UpdateAlarm() {
    var armed: bool;
    armed = HasDeadline();
    if (armed != durable.alarmArmed) {
      durable.alarmArmed = armed;
      send clock, eArmed, (who = this, armed = armed);
    }
    CheckAlarm();
  }

  fun CheckAlarm() {
    if (!alarmQueued && !stepRunning && IsDue()) {
      alarmQueued = true;
      send this, eAlarm;
    }
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
