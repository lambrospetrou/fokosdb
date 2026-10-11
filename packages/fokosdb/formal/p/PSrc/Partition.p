// One row of `pending_transactions`: the lock of a key, its transaction, and its operation.
type tLockRow = (tx: int, op: tTxOp);

// One row of `pending_tx_info`: the timestamp and the coordinator of the transaction, and the times
// of the stale job.
type tPendingTxInfo = (ts: int, coordinator: machine, createdAt: int, nextRecoveryAt: int, guarded: bool);

// The SQLite state of a partition: the `items` table, the `deletion_metadata` row, the lock rows,
// and the `pending_tx_info` rows. The alarm is durable too.
type tPartitionDurable = (items: map[tKey, tItemRow], maxDeletedV: int, maxDeleteTxOrderTs: int,
                          locks: map[tKey, tLockRow], pendingTx: map[int, tPendingTxInfo], alarmArmed: bool);

// The stale job in progress: the start time of the step, and the transaction that waits for the
// answer of its coordinator.
type tStaleStep = (running: bool, startedAt: int, tx: int, reqId: int);

type tPartitionConfig = (bugs: tBugs, env: machine, clock: machine, operator: machine);

// PartitionDO with its PartitionStore and TransactionParticipant. Each handler is one block of the
// code. The topology is fixed, so this partition owns every key that it receives.
machine Partition {
  var bugs: tBugs;
  var env: machine;
  var clock: machine;
  var operator: machine;
  var durable: tPartitionDurable;
  var now: int;
  // The clock of this Durable Object as the Environment sets it. A restart keeps it.
  var tsClock: tClock;
  var step: tStaleStep;
  // An eAlarm waits in the inbox.
  var alarmQueued: bool;
  // The ids of the calls of this machine. A model value, not a value of the code: a restart keeps it,
  // so that an answer to a call from before the restart does not match a new call.
  var nextReqId: int;

  start state Init {
    entry (cfg: tPartitionConfig) {
      bugs = cfg.bugs;
      env = cfg.env;
      clock = cfg.clock;
      operator = cfg.operator;
      goto Serving;
    }
  }

  state Serving {
    // An eviction or a crash between two handlers: the memory state goes, SQLite and the alarm stay.
    on eRestart do {
      step = default(tStaleStep);
      CheckAlarm();
    }

    on eTick do (t: int) {
      now = t;
      CheckAlarm();
    }

    on eClock do (c: tClock) {
      tsClock = c;
    }

    // apiPutItem: putItemLocal refuses a key with a lock row (item_locked_by_transaction), then
    // PartitionStore.upsertItem writes the row at the clock of this partition (txOrderTimestampNow).
    on ePutItem do (req: tPutItemReq) {
      var v: int;
      var ts: int;
      if (req.key in durable.locks && !bugs.putIgnoresLock) {
        send req.caller, ePutItemResp, (reqId = req.reqId, locked = true, version = 0);
        return;
      }
      ts = TsNow();
      v = WriteItem(req.key, req.value, 0, ts);
      announce eStamped, (partition = this, key = req.key, operation = OP_PUT, ts = ts, twoPhase = false);
      send req.caller, ePutItemResp, (reqId = req.reqId, locked = false, version = v);
    }

    // apiDeleteItem: deleteItemLocal refuses a key with a lock row, then PartitionStore.deleteItem
    // removes the row at the clock of this partition. A delete of an absent row changes nothing.
    on eDeleteItem do (req: tDeleteItemReq) {
      var ts: int;
      if (req.key in durable.locks) {
        send req.caller, eDeleteItemResp, (reqId = req.reqId, locked = true, deleted = false);
        return;
      }
      if (!(req.key in durable.items)) {
        send req.caller, eDeleteItemResp, (reqId = req.reqId, locked = false, deleted = false);
        return;
      }
      ts = TsNow();
      DeleteItem(req.key, 0, ts);
      announce eStamped, (partition = this, key = req.key, operation = OP_DELETE, ts = ts, twoPhase = false);
      send req.caller, eDeleteItemResp, (reqId = req.reqId, locked = false, deleted = true);
    }

    // apiGetItem: readItemLocally reads the committed row. A lock does not block it.
    on eGetItem do (req: tGetItemReq) {
      var row: tItemRow;
      if (!(req.key in durable.items)) {
        send req.caller, eGetItemResp, (reqId = req.reqId, found = false, value = 0, version = 0);
        return;
      }
      row = durable.items[req.key];
      send req.caller, eGetItemResp, (reqId = req.reqId, found = true, value = row.value, version = row.v);
    }

    // txPrepare: prepareLocal. A timestamp more than maxClockSkewMs ahead of the clock of this
    // partition gives clock_skew for the whole request. Then, for each key, a lock of another
    // transaction gives pending_conflict, a failed condition gives condition_failed, and a timestamp
    // that is not above the watermark of the item gives timestamp_conflict. The watermark of a check
    // is last_write_ts, of a put or a delete last_read_ts, and of an absent item
    // max_delete_tx_order_ts. Each rejection writes no lock. Otherwise the lock block writes one
    // pending_tx_info row and one lock row for each key. A repeated prepare finds its own locks and
    // passes.
    on eTxPrepare do (req: tPrepareReq) {
      var op: tTxOp;
      var watermark: int;
      if (req.ts > TsNow() + MAX_CLOCK_SKEW()) {
        Reject(req);
        return;
      }
      foreach (op in req.items) {
        if (op.key in durable.locks) {
          if (durable.locks[op.key].tx != req.tx) {
            Reject(req);
            return;
          }
          continue;
        }
        if (!ConditionHolds(op)) {
          Reject(req);
          return;
        }
        if (!(op.key in durable.items)) {
          watermark = durable.maxDeleteTxOrderTs;
        } else if (op.operation == OP_CHECK) {
          watermark = durable.items[op.key].lastWriteTs;
        } else {
          watermark = durable.items[op.key].lastReadTs;
        }
        if (req.ts <= watermark && !bugs.prepareSkipsWatermark) {
          Reject(req);
          return;
        }
      }
      if (!(req.tx in durable.pendingTx)) {
        durable.pendingTx[req.tx] = (ts = req.ts, coordinator = req.coordinator, createdAt = now,
                                     nextRecoveryAt = now + STALE_TRANSACTION(), guarded = false);
      }
      foreach (op in req.items) {
        if (!(op.key in durable.locks)) {
          durable.locks[op.key] = (tx = req.tx, op = op);
          announce eLockWritten, (partition = this, key = op.key, tx = req.tx);
        }
      }
      UpdateAlarm();
      send req.caller, eTxPrepareResp, (reqId = req.reqId, partition = this, outcome = PREPARE_ACCEPTED);
    }

    // txCommit: commitLocal.
    on eTxCommit do (req: tCommitReq) {
      var ok: bool;
      ok = CommitLocal(req.tx, req.itemKeys);
      send req.caller, eTxCommitResp, (reqId = req.reqId, partition = this, ok = ok);
    }

    // txCancel: cancelLocal.
    on eTxCancel do (req: tCancelReq) {
      CancelLocal(req.tx, req.itemKeys);
      send req.caller, eTxCancelResp, (reqId = req.reqId, partition = this);
    }

    // txExecuteSingleShot: executeSingleShot applies the whole transaction in one storage
    // transaction at the clock of this partition. It takes no lock and checks no timestamp. A lock
    // of a two-phase transaction gives pending_conflict, and a failed condition gives
    // condition_failed. A rejection writes nothing.
    on eExecuteSingleShot do (req: tSingleShotReq) {
      var op: tTxOp;
      var ts: int;
      ts = TsNow();
      foreach (op in req.items) {
        if ((op.key in durable.locks && !bugs.singleShotIgnoresLock) || !ConditionHolds(op)) {
          send req.caller, eExecuteSingleShotResp, (reqId = req.reqId, committed = false);
          return;
        }
      }
      foreach (op in req.items) {
        ApplyOp(op, 0, ts, false);
      }
      send req.caller, eExecuteSingleShotResp, (reqId = req.reqId, committed = true);
    }

    // txReadForTransaction: readForTransactionLocal reads every key in one block and writes nothing.
    on eReadForTransaction do (req: tReadReq) {
      send req.caller, eReadForTransactionResp, (reqId = req.reqId, partition = this, items = ReadLocal(req.readKeys));
    }

    // txReadSnapshot: the same read in one block. A pending write on any key gives the outcome
    // aborted.
    on eReadSnapshot do (req: tReadReq) {
      var items: seq[tReadItem];
      var item: tReadItem;
      items = ReadLocal(req.readKeys);
      foreach (item in items) {
        if (item.hasPendingWrite) {
          send req.caller, eReadSnapshotResp, (reqId = req.reqId, aborted = true, items = default(seq[tReadItem]));
          return;
        }
      }
      send req.caller, eReadSnapshotResp, (reqId = req.reqId, aborted = false, items = items);
    }

    // debugForceResolveTransaction: sends every row of the transaction through dispatch. On a fixed
    // topology this partition owns each row, so the commit or the cancel applies here.
    on eForceResolve do (req: tForceResolveReq) {
      var rows: seq[tKey];
      rows = RowsOf(req.tx);
      if (req.commit) {
        CommitLocal(req.tx, rows);
      } else {
        CancelLocal(req.tx, rows);
      }
      send req.caller, eForceResolveResp, (reqId = req.reqId, partition = this);
    }

    // The alarm runs the stale_tx_recovery job: one step at a time.
    on eAlarm do {
      alarmQueued = false;
      if (step.running || !IsDue()) {
        return;
      }
      step = (running = true, startedAt = now, tx = 0, reqId = 0);
      ClaimNext();
    }

    // The answer of recoverTransactionForParticipant. The step reads the rows of the transaction
    // again, because they can change during the call, and applies the answer through dispatch.
    on eRecoverResp do (resp: tRecoverResp) {
      var rows: seq[tKey];
      var tx: int;
      if (!step.running || resp.reqId != step.reqId) {
        return;
      }
      tx = step.tx;
      rows = RowsOf(tx);
      if (sizeof(rows) > 0) {
        if (resp.ledgerState == RECOVER_COMMITTED) {
          CommitLocal(tx, rows);
        } else if (resp.ledgerState == RECOVER_CANCELLED || (resp.ledgerState == RECOVER_DRIVING && bugs.staleCancelsOnDriving)) {
          CancelLocal(tx, rows);
        } else if (resp.ledgerState == RECOVER_NOT_FOUND) {
          // A lock older than the idempotency window gets the quarantine, except a lock of checks
          // whose stamps already cover the transaction. That lock is what a late prepare of a
          // committed check writes again, and a cancel is correct for either outcome.
          if (now - durable.pendingTx[tx].createdAt > IDEMPOTENCY_WINDOW()
              && (bugs.quarantineStampedChecks || !ChecksAlreadyStamped(tx, rows))) {
            Guard(tx);
          } else {
            CancelLocal(tx, rows);
          }
        }
      }
      ClaimNext();
    }

    on eRpcFailed do (f: tRpcFailed) {
      if (!step.running || f.reqId != step.reqId) {
        return;
      }
      ClaimNext();
    }
  }

  fun Reject(req: tPrepareReq) {
    send req.caller, eTxPrepareResp, (reqId = req.reqId, partition = this, outcome = PREPARE_REJECTED);
  }

  // The transaction order timestamp of this partition now (txOrderTimestampNow).
  fun TsNow(): int {
    return TsOf(now, tsClock);
  }

  fun ConditionHolds(op: tTxOp): bool {
    return !((op.condition == EXISTS && !(op.key in durable.items)) || (op.condition == NOT_EXISTS && op.key in durable.items));
  }

  // readForTransactionLocal: the committed row of each key, and the max_deleted_v of the partition. A
  // lock row of a check does not set hasPendingWrite (READ_ONLY_PENDING_OPERATIONS).
  fun ReadLocal(readKeys: seq[tKey]): seq[tReadItem] {
    var items: seq[tReadItem];
    var key: tKey;
    var item: tReadItem;
    foreach (key in readKeys) {
      item = (key = key, found = false, value = 0, version = 0, maxDeletedV = durable.maxDeletedV, hasPendingWrite = false);
      if (key in durable.locks) {
        item.hasPendingWrite = durable.locks[key].op.operation != OP_CHECK;
      }
      if (key in durable.items) {
        item.found = true;
        item.value = durable.items[key].value;
        item.version = durable.items[key].v;
      }
      items += (sizeof(items), item);
    }
    return items;
  }

  // The keys of the lock rows of a transaction.
  fun RowsOf(tx: int): seq[tKey] {
    var rows: seq[tKey];
    var key: tKey;
    foreach (key in keys(durable.locks)) {
      if (durable.locks[key].tx == tx) {
        rows += (sizeof(rows), key);
      }
    }
    return rows;
  }

  // commitLocal: with no lock row of the transaction left, it answers the idempotent success.
  // Otherwise the keys of the request must be the keys of the lock rows, or it throws
  // commit_keyset_mismatch. It applies the operation of each lock row at the timestamp of the
  // transaction (apply), and deletes one lock row for each key.
  fun CommitLocal(tx: int, requestKeys: seq[tKey]): bool {
    var owned: set[tKey];
    var requested: set[tKey];
    var key: tKey;
    var ts: int;
    foreach (key in RowsOf(tx)) {
      owned += (key);
    }
    if (sizeof(owned) == 0) {
      return true;
    }
    foreach (key in requestKeys) {
      requested += (key);
    }
    if (owned != requested) {
      return false;
    }
    ts = durable.pendingTx[tx].ts;
    foreach (key in owned) {
      ApplyOp(durable.locks[key].op, tx, ts, true);
      announce eApplied, (partition = this, key = key, tx = tx);
      DeleteLock(key, tx);
    }
    return true;
  }

  // TransactionParticipant.#write: one operation at `ts`. A transactional delete also raises
  // max_delete_tx_order_ts for an absent row (bumpTxOrderTsAlways). A check raises last_read_ts of a
  // present row (bumpItemReadTs). An absent item has no row to hold the stamp, so a check of an
  // absent item raises max_delete_tx_order_ts (bumpMaxDeleteTxOrderTs). `tx` is 0 for a
  // single-partition transaction.
  fun ApplyOp(op: tTxOp, tx: int, ts: int, twoPhase: bool) {
    var row: tItemRow;
    if (op.operation == OP_PUT) {
      WriteItem(op.key, op.value, tx, ts);
    } else if (op.operation == OP_DELETE) {
      if (op.key in durable.items) {
        DeleteItem(op.key, tx, ts);
      } else {
        durable.maxDeleteTxOrderTs = Max(durable.maxDeleteTxOrderTs, ts);
        announce eItemWritten, (partition = this, key = op.key, deleted = true, value = 0, v = 0, tx = tx);
      }
    } else if (op.key in durable.items) {
      row = durable.items[op.key];
      row.lastReadTs = Max(row.lastReadTs, ts);
      durable.items[op.key] = row;
    } else if (!bugs.absentCheckKeepsNoStamp) {
      durable.maxDeleteTxOrderTs = Max(durable.maxDeleteTxOrderTs, ts);
    }
    announce eStamped, (partition = this, key = op.key, operation = op.operation, ts = ts, twoPhase = twoPhase);
  }

  // PartitionStore.deleteItem of a present row: removes the row, raises max_deleted_v to its v, and
  // raises max_delete_tx_order_ts to the larger of `ts` and its last_read_ts.
  fun DeleteItem(key: tKey, tx: int, ts: int) {
    var removed: tItemRow;
    removed = durable.items[key];
    durable.items -= (key);
    durable.maxDeletedV = Max(durable.maxDeletedV, removed.v);
    durable.maxDeleteTxOrderTs = Max(durable.maxDeleteTxOrderTs, Max(ts, removed.lastReadTs));
    announce eItemWritten, (partition = this, key = key, deleted = true, value = 0, v = removed.v, tx = tx);
  }

  // cancelLocal: deletes the lock rows of the transaction under the given keys.
  fun CancelLocal(tx: int, requestKeys: seq[tKey]) {
    var key: tKey;
    foreach (key in requestKeys) {
      if (key in durable.locks && durable.locks[key].tx == tx) {
        announce eReleased, (partition = this, key = key, tx = tx);
        DeleteLock(key, tx);
      }
    }
  }

  // PartitionStore.deletePendingTxKeys: deletes one lock row, and the pending_tx_info row with the
  // last lock row of its transaction.
  fun DeleteLock(key: tKey, tx: int) {
    durable.locks -= (key);
    announce eLockDeleted, (partition = this, key = key, tx = tx);
    if (sizeof(RowsOf(tx)) == 0) {
      durable.pendingTx -= (tx);
      UpdateAlarm();
    }
  }

  // PartitionDO.#checksAlreadyStamped: each lock row of the transaction is a check, and the stamp that
  // a check of its item records (last_read_ts of a present item, max_delete_tx_order_ts of an absent
  // one) is at or above the timestamp of the transaction. While the lock exists, no other request
  // changes the item, so a commit and a cancel then leave the same state.
  fun ChecksAlreadyStamped(tx: int, rows: seq[tKey]): bool {
    var key: tKey;
    var stamp: int;
    foreach (key in rows) {
      if (durable.locks[key].op.operation != OP_CHECK) {
        return false;
      }
      if (key in durable.items) {
        stamp = durable.items[key].lastReadTs;
      } else {
        stamp = durable.maxDeleteTxOrderTs;
      }
      if (stamp < durable.pendingTx[tx].ts) {
        return false;
      }
    }
    return true;
  }

  // PartitionStore.guardPendingTx: sets guarded_at one time, and logs the lock-age guard error.
  fun Guard(tx: int) {
    var info: tPendingTxInfo;
    info = durable.pendingTx[tx];
    if (info.guarded) {
      return;
    }
    info.guarded = true;
    durable.pendingTx[tx] = info;
    announce eLockGuarded, (partition = this, tx = tx);
    if (operator != null) {
      send operator, eGuardLogged, (partition = this, tx = tx);
    }
    UpdateAlarm();
  }

  // PartitionStore.upsertItem: a new row starts at max_deleted_v + 1 with its timestamps at least
  // max_delete_tx_order_ts, and an existing row runs v = v + 1. Both timestamps rise with MAX.
  // Returns the new v.
  fun WriteItem(key: tKey, value: tValue, tx: int, ts: int): int {
    var row: tItemRow;
    if (key in durable.items) {
      row = durable.items[key];
      row.v = row.v + 1;
      row.lastReadTs = Max(row.lastReadTs, ts);
      row.lastWriteTs = Max(row.lastWriteTs, ts);
    } else {
      if (bugs.newRowVersionFromOne) {
        row.v = 1;
      } else {
        row.v = durable.maxDeletedV + 1;
      }
      if (bugs.newRowTsFromOwnStamp) {
        row.lastReadTs = ts;
      } else {
        row.lastReadTs = Max(ts, durable.maxDeleteTxOrderTs);
      }
      row.lastWriteTs = row.lastReadTs;
    }
    row.value = value;
    durable.items[key] = row;
    announce eItemWritten, (partition = this, key = key, deleted = false, value = value, v = row.v, tx = tx);
    return row.v;
  }

  // claimStaleTransactions: claims the unguarded transaction with the earliest next_recovery_at that
  // was due when the step started, and moves its next_recovery_at forward. Then the step calls its
  // coordinator. The step ends when no transaction is due.
  fun ClaimNext() {
    var tx: int;
    var best: int;
    var info: tPendingTxInfo;
    best = 0;
    foreach (tx in keys(durable.pendingTx)) {
      info = durable.pendingTx[tx];
      if (!info.guarded && info.nextRecoveryAt <= step.startedAt
          && (best == 0 || info.nextRecoveryAt < durable.pendingTx[best].nextRecoveryAt)) {
        best = tx;
      }
    }
    if (best == 0) {
      step = default(tStaleStep);
      UpdateAlarm();
      return;
    }
    info = durable.pendingTx[best];
    info.nextRecoveryAt = NextRecoveryAt(now, info.createdAt);
    durable.pendingTx[best] = info;
    nextReqId = nextReqId + 1;
    step.tx = best;
    step.reqId = nextReqId;
    send NewRpc(this, info.coordinator, env), eRecover, (caller = this, reqId = nextReqId, tx = best);
  }

  // The deadline of the job: the earliest next_recovery_at of an unguarded transaction.
  fun HasDeadline(): bool {
    var info: tPendingTxInfo;
    foreach (info in values(durable.pendingTx)) {
      if (!info.guarded) {
        return true;
      }
    }
    return false;
  }

  fun IsDue(): bool {
    var info: tPendingTxInfo;
    foreach (info in values(durable.pendingTx)) {
      if (!info.guarded && info.nextRecoveryAt <= now) {
        return true;
      }
    }
    return false;
  }

  // The runtime arms the alarm at the deadline of the job, and clears it when no deadline is left.
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
    if (!alarmQueued && !step.running && IsDue()) {
      alarmQueued = true;
      send this, eAlarm;
    }
  }
}

fun Max(a: int, b: int): int {
  if (a > b) {
    return a;
  }
  return b;
}
