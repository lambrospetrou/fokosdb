// One row of `pending_transactions`: the lock of a key, its transaction, and its operation.
type tLockRow = (tx: int, op: tTxOp);

// One row of `pending_tx_info`: the timestamp and the coordinator of the transaction, and the times
// of the stale job.
type tPendingTxInfo = (ts: int, coordinator: machine, createdAt: int, nextRecoveryAt: int, guarded: bool);

// One target row of a split: the child, whether its fokosInit succeeded, and whether it acknowledged
// its import.
type tSplitTarget = (child: machine, initialized: bool, acked: bool);

// The SQLite state of a partition: the `items` table, the `deletion_metadata` row, the lock rows,
// and the `pending_tx_info` rows. The alarm is durable too. A split source also has the state of its
// split and its target rows. A target also has its source, its child index, and its import record
// with the cursor.
type tPartitionDurable = (items: map[tKey, tItemRow], maxDeletedV: int, maxDeleteTxOrderTs: int,
                          locks: map[tKey, tLockRow], pendingTx: map[int, tPendingTxInfo], alarmArmed: bool,
                          split: tSplitState, children: seq[tSplitTarget],
                          source: machine, slice: int, importState: tImportState, cursor: tCursor);

// The stale job in progress: the start time of the step, and the transaction that waits for the
// answer of its coordinator.
type tStaleStep = (running: bool, startedAt: int, tx: int, reqId: int);

// A `group` request that a router divided by owner: the caller and its request id, the children that
// did not answer yet, and the parts of the answer. `failed` is a part that threw, and `rejected` a
// prepare that a child rejected.
enum tFanoutKind { FAN_PREPARE, FAN_COMMIT, FAN_CANCEL, FAN_READ }
type tFanout = (caller: machine, reqId: int, kind: tFanoutKind, waiting: set[machine], failed: bool, rejected: bool,
                items: seq[tReadItem]);

// `now` and `tsClock` are the clock of the source that creates a child.
type tPartitionConfig = (bugs: tBugs, env: machine, clock: machine, operator: machine, now: int, tsClock: tClock);

// PartitionDO with its PartitionStore, its TransactionParticipant, and its FokosShardingRuntime. Each
// handler is one block of the code.
//
// A partition owns every key that it receives, until its hash split cuts over. From the cutover it
// is a router: it owns no key, and sends each request to the child that owns the key. A child answers
// partition_migrating until its import ends, and reads apiGetItem through its source.
//
// A partition keeps a continuation for a `point`, a `single_owner`, or a read-through forward.
// Its outbound RpcCall returns to that continuation before the partition answers the caller.
// It also keeps one for a `group` request, which can have a part on each child.
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
  // so that an answer to a call from before the restart does not match a new call. The calls of the
  // repartition flow have the id 0: they keep no continuation.
  var nextReqId: int;
  var fanouts: map[int, tFanout];
  var forwards: map[int, machine];

  start state Init {
    entry (cfg: tPartitionConfig) {
      bugs = cfg.bugs;
      env = cfg.env;
      clock = cfg.clock;
      operator = cfg.operator;
      now = cfg.now;
      tsClock = cfg.tsClock;
      goto Serving;
    }
  }

  state Serving {
    // An eviction or a crash between two handlers: the memory state goes, SQLite and the alarm stay.
    // Each request that waits for a forward gets an error.
    on eRestart do {
      var f: tFanout;
      var forwarded: machine;
      foreach (forwarded in values(forwards)) {
        send forwarded, eRpcBroken;
      }
      forwards = default(map[int, machine]);
      foreach (f in values(fanouts)) {
        send f.caller, eRpcBroken;
      }
      fanouts = default(map[int, tFanout]);
      step = default(tStaleStep);
      CheckAlarm();
      // The alarm runs the import job again. A target in awaiting_data waits for fokosStartImport.
      if (durable.importState != AWAITING_DATA) {
        ImportStep();
      }
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
      if (Routed(req.caller, req.key, ePutItem, req)) {
        return;
      }
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
      if (Routed(req.caller, req.key, eDeleteItem, req)) {
        return;
      }
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

    // apiGetItem: readItemLocally reads the committed row. A lock does not block it. A target that
    // imports reads through its source (fokosExecuteLocal), and a router forwards to the owner.
    on eGetItem do (req: tGetItemReq) {
      if (Importing()) {
        Forward(req.caller, durable.source, eExecuteLocal, req);
        return;
      }
      if (IsRouter() && !bugs.routerServesGetItem) {
        Forward(req.caller, OwnerOf(req.key), eGetItem, req);
        return;
      }
      ReadItemLocally(req);
    }

    // fokosExecuteLocal: the source runs the local handler of apiGetItem for a target that imports,
    // with no owner resolution. A split source keeps its item rows for life.
    on eExecuteLocal do (req: tGetItemReq) {
      ReadItemLocally(req);
    }

    // txPrepare: prepareLocal. A timestamp more than maxClockSkewMs ahead of the clock of this
    // partition gives clock_skew for the whole request. Then, for each key, a lock of another
    // transaction gives pending_conflict, a failed condition gives condition_failed, and a timestamp
    // that is not above the watermark of the item gives timestamp_conflict. The watermark of a check
    // is last_write_ts, of a put or a delete last_read_ts, and of an absent item
    // max_delete_tx_order_ts. Each rejection writes no lock. Otherwise the lock block writes one
    // pending_tx_info row and one lock row for each key. A repeated prepare finds its own locks and
    // passes. A router sends the operations of each child in one call (fail_fast).
    on eTxPrepare do (req: tPrepareReq) {
      var op: tTxOp;
      var watermark: int;
      var parts: map[machine, seq[tTxOp]];
      var child: machine;
      var id: int;
      if (!bugs.importingChildServesPrepare && Migrating(req.caller)) {
        return;
      }
      if (IsRouter()) {
        foreach (op in req.items) {
          child = OwnerOf(op.key);
          if (!(child in parts)) {
            parts[child] = default(seq[tTxOp]);
          }
          parts[child] += (sizeof(parts[child]), op);
        }
        id = NewFanout(req.caller, req.reqId, FAN_PREPARE, keys(parts));
        foreach (child in keys(parts)) {
          send NewRpc(this, child, env), eTxPrepare,
            (caller = this, reqId = id, coordinator = req.coordinator, tx = req.tx, ts = req.ts, items = parts[child]);
        }
        return;
      }
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
          Mutated(op.key, req.tx);
        }
      }
      UpdateAlarm();
      send req.caller, eTxPrepareResp, (reqId = req.reqId, partition = this, outcome = PREPARE_ACCEPTED);
    }

    // txCommit: commitLocal. A router sends the keys of each child in one call (attempt_all).
    on eTxCommit do (req: tCommitReq) {
      var ok: bool;
      var parts: map[machine, seq[tKey]];
      var child: machine;
      var id: int;
      if (Migrating(req.caller)) {
        return;
      }
      if (IsRouter()) {
        parts = KeysByOwner(req.itemKeys);
        id = NewFanout(req.caller, req.reqId, FAN_COMMIT, keys(parts));
        foreach (child in keys(parts)) {
          send NewRpc(this, child, env), eTxCommit, (caller = this, reqId = id, tx = req.tx, ts = req.ts, itemKeys = parts[child]);
        }
        return;
      }
      ok = CommitLocal(req.tx, req.itemKeys);
      send req.caller, eTxCommitResp, (reqId = req.reqId, partition = this, ok = ok);
    }

    // txCancel: cancelLocal. A router sends the keys of each child in one call (attempt_all).
    on eTxCancel do (req: tCancelReq) {
      var parts: map[machine, seq[tKey]];
      var child: machine;
      var id: int;
      if (Migrating(req.caller)) {
        return;
      }
      if (IsRouter()) {
        parts = KeysByOwner(req.itemKeys);
        id = NewFanout(req.caller, req.reqId, FAN_CANCEL, keys(parts));
        foreach (child in keys(parts)) {
          send NewRpc(this, child, env), eTxCancel, (caller = this, reqId = id, tx = req.tx, itemKeys = parts[child]);
        }
        return;
      }
      CancelLocal(req.tx, req.itemKeys);
      send req.caller, eTxCancelResp, (reqId = req.reqId, partition = this);
    }

    // txExecuteSingleShot: executeSingleShot applies the whole transaction in one storage
    // transaction at the clock of this partition. It takes no lock and checks no timestamp. A lock
    // of a two-phase transaction gives pending_conflict, and a failed condition gives
    // condition_failed. A rejection writes nothing. A router forwards the request when one child owns
    // every key, and answers not_applicable for every other request.
    on eExecuteSingleShot do (req: tSingleShotReq) {
      var op: tTxOp;
      var ts: int;
      var owners: set[machine];
      if (Migrating(req.caller)) {
        return;
      }
      if (IsRouter()) {
        foreach (op in req.items) {
          owners += (OwnerOf(op.key));
        }
        if (sizeof(owners) == 1) {
          Forward(req.caller, owners[0], eExecuteSingleShot, req);
        } else {
          send req.caller, eExecuteSingleShotResp, (reqId = req.reqId, notApplicable = true, committed = false);
        }
        return;
      }
      ts = TsNow();
      foreach (op in req.items) {
        if ((op.key in durable.locks && !bugs.singleShotIgnoresLock) || !ConditionHolds(op)) {
          send req.caller, eExecuteSingleShotResp, (reqId = req.reqId, notApplicable = false, committed = false);
          return;
        }
      }
      foreach (op in req.items) {
        ApplyOp(op, 0, ts, false);
      }
      send req.caller, eExecuteSingleShotResp, (reqId = req.reqId, notApplicable = false, committed = true);
    }

    // txReadForTransaction: readForTransactionLocal reads every key in one block and writes nothing.
    // A router sends the keys of each child in one call (fail_fast).
    on eReadForTransaction do (req: tReadReq) {
      var parts: map[machine, seq[tKey]];
      var child: machine;
      var id: int;
      if (Migrating(req.caller)) {
        return;
      }
      if (IsRouter()) {
        parts = KeysByOwner(req.readKeys);
        id = NewFanout(req.caller, req.reqId, FAN_READ, keys(parts));
        foreach (child in keys(parts)) {
          send NewRpc(this, child, env), eReadForTransaction, (caller = this, reqId = id, readKeys = parts[child]);
        }
        return;
      }
      send req.caller, eReadForTransactionResp, (reqId = req.reqId, partition = this, items = ReadLocal(req.readKeys));
    }

    // txReadSnapshot: the same read in one block. A pending write on any key gives the outcome
    // aborted. A router forwards the request when one child owns every key, and answers
    // not_applicable for every other request.
    on eReadSnapshot do (req: tReadReq) {
      var items: seq[tReadItem];
      var item: tReadItem;
      var parts: map[machine, seq[tKey]];
      if (Migrating(req.caller)) {
        return;
      }
      if (IsRouter()) {
        parts = KeysByOwner(req.readKeys);
        if (sizeof(parts) == 1) {
          Forward(req.caller, keys(parts)[0], eReadSnapshot, req);
        } else {
          send req.caller, eReadSnapshotResp, (reqId = req.reqId, notApplicable = true, aborted = false, items = items);
        }
        return;
      }
      items = ReadLocal(req.readKeys);
      foreach (item in items) {
        if (item.hasPendingWrite) {
          send req.caller, eReadSnapshotResp,
            (reqId = req.reqId, notApplicable = false, aborted = true, items = default(seq[tReadItem]));
          return;
        }
      }
      send req.caller, eReadSnapshotResp, (reqId = req.reqId, notApplicable = false, aborted = false, items = items);
    }

    on eForwardResp do (resp: tForwardResp) {
      if (resp.forwardId in forwards) {
        send forwards[resp.forwardId], resp.response, resp.payload;
        forwards -= (resp.forwardId);
      }
    }

    // The answers of the children to the parts of a group request.
    on eTxPrepareResp do (resp: tPrepareResp) {
      if (resp.reqId in fanouts && resp.outcome == PREPARE_REJECTED) {
        fanouts[resp.reqId].rejected = true;
      }
      PartSettled(resp.reqId, resp.partition, false);
    }
    on eTxCommitResp do (resp: tCommitResp) {
      PartSettled(resp.reqId, resp.partition, !resp.ok);
    }
    on eTxCancelResp do (resp: tCancelResp) {
      PartSettled(resp.reqId, resp.partition, false);
    }
    on eReadForTransactionResp do (resp: tReadForTransactionResp) {
      var item: tReadItem;
      if (resp.reqId in fanouts) {
        foreach (item in resp.items) {
          fanouts[resp.reqId].items += (sizeof(fanouts[resp.reqId].items), item);
        }
      }
      PartSettled(resp.reqId, resp.partition, false);
    }

    // debugForceResolveTransaction: sends every row of the transaction through dispatch. This
    // partition owns each row, so the commit or the cancel applies here. No test case repairs a
    // lock after a split, so the model has no repair on a router.
    on eForceResolve do (req: tForceResolveReq) {
      var rows: seq[tKey];
      if (Migrating(req.caller)) {
        return;
      }
      assert !IsRouter(), "the model has no debugForceResolveTransaction on a router";
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
    // again, because they can change during the call, and applies the answer through dispatch. A
    // partition that cut over during the call owns no row, so the answer changes nothing.
    on eRecoverResp do (resp: tRecoverResp) {
      var rows: seq[tKey];
      var tx: int;
      if (!step.running || resp.reqId != step.reqId) {
        return;
      }
      tx = step.tx;
      rows = RowsOf(tx);
      if (sizeof(rows) > 0 && !IsRouter()) {
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

    // A call failed: a forward, a part of a group request, the call of the stale job, or a call of the
    // repartition flow, whose job then runs its step again.
    on eRpcFailed do (f: tRpcFailed) {
      if (f.reqId in forwards) {
        send forwards[f.reqId], eRpcBroken;
        forwards -= (f.reqId);
      } else if (f.reqId in fanouts) {
        PartSettled(f.reqId, f.target, true);
      } else if (step.running && f.reqId == step.reqId) {
        ClaimNext();
      } else if (f.reqId == 0 && f.target == durable.source) {
        ImportStep();
      } else if (f.reqId == 0) {
        send this, eSourceStep;
      }
    }

    // RepartitionSource.queue: writes the split in `queued`, and schedules the source_repartition job.
    on eRequestSplit do {
      SetSplit(SPLIT_QUEUED);
      send this, eSourceStep;
    }

    // sourceStep and sourceCleanupStep: one step of the source_repartition job.
    on eSourceStep do {
      var i: int;
      var child: machine;
      var pending: bool;
      if (durable.split == SPLIT_QUEUED) {
        // #plan: writes one target row for each of the hashSplitN children. The machine of a child
        // stands for its Durable Object name.
        while (i < HASH_SPLIT_N()) {
          child = new Partition((bugs = bugs, env = env, clock = clock, operator = operator, now = now, tsClock = tsClock));
          send env, eDoCreated, child;
          durable.children += (i, (child = child, initialized = false, acked = false));
          i = i + 1;
        }
        SetSplit(SPLIT_PLANNED);
        send this, eSourceStep;
      } else if (durable.split == SPLIT_PLANNED) {
        // #initializeTargets: calls fokosInit on each target that is not initialized. #cutover: when
        // every target is initialized, the source becomes a router.
        while (i < sizeof(durable.children)) {
          if (!durable.children[i].initialized) {
            pending = true;
            send NewRpc(this, durable.children[i].child, env), eFokosInit, (caller = this, reqId = 0, source = this, slice = i);
          }
          i = i + 1;
        }
        if (!pending) {
          Cutover();
        }
      } else if (durable.split == SPLIT_COMPLETED) {
        // cleanupSourceStep: deletes the lock copies of one transaction in each step, then writes
        // `cleaned`. A split router keeps its item rows.
        if (sizeof(durable.pendingTx) > 0) {
          DeleteCopies(keys(durable.pendingTx)[0]);
          send this, eSourceStep;
        } else {
          SetSplit(SPLIT_CLEANED);
        }
      }
    }

    // The answer of fokosInit: the source marks the target `initialized`.
    on eFokosInitResp do (slice: int) {
      var target: tSplitTarget;
      if (durable.split != SPLIT_PLANNED) {
        return;
      }
      durable.children[slice].initialized = true;
      foreach (target in durable.children) {
        if (!target.initialized) {
          return;
        }
      }
      send this, eSourceStep;
    }

    // fokosInit: initAsTarget writes the identity of the target and its import record in
    // `awaiting_data`. A repeated call changes nothing.
    on eFokosInit do (req: tFokosInitReq) {
      if (durable.importState == NOT_TARGET) {
        durable.source = req.source;
        durable.slice = req.slice;
        SetImport(AWAITING_DATA);
      }
      send req.caller, eFokosInitResp, durable.slice;
    }

    // fokosStartImport: the target starts its import. In the code a target in awaiting_data also
    // starts from its own alarm, and the source refuses a pull before the cutover. The model sends
    // this event one time at the cutover and does not lose it, so that alarm is not necessary.
    on eStartImport do {
      ImportStep();
    }

    // fokosMigrationPull: servePage refuses a pull before the cutover (repartition_not_cut_over) and
    // after the completion (partition_migrating).
    on eMigrationPull do (req: tMigrationPullReq) {
      if (durable.split != SPLIT_CUTOVER) {
        send req.caller, eRpcBroken;
        return;
      }
      send req.caller, eMigrationPage, BuildPage(req.slice, req.cursor);
    }

    // The page of a pull: importOnePage commits the page with its cursor. It drops a page whose cursor
    // is not the durable cursor. applyPage inserts the item rows with their v and their timestamps,
    // and the lock rows with their pending_tx_info rows, guarded_at included, and merges the deletion
    // metadata with MAX. The last page writes `imported`. The step keeps no continuation, so the page
    // of a pull from before a restart can apply, and the cursor check then drops the page of the pull
    // that the restart started.
    on eMigrationPage do (page: tMigrationPage) {
      var key: tKey;
      var tx: int;
      if (!Importing() || page.cursor != durable.cursor) {
        return;
      }
      foreach (key in keys(page.items)) {
        if (!(key in durable.items)) {
          durable.items[key] = page.items[key];
        }
      }
      if (!bugs.importSkipsPendingTx) {
        foreach (tx in keys(page.pendingTx)) {
          if (!(tx in durable.pendingTx)) {
            durable.pendingTx[tx] = page.pendingTx[tx];
            if (page.pendingTx[tx].guarded) {
              announce eLockGuarded, (partition = this, tx = tx);
            }
          }
        }
        foreach (key in keys(page.locks)) {
          if (!(key in durable.locks)) {
            durable.locks[key] = page.locks[key];
            announce eLockWritten, (partition = this, key = key, tx = page.locks[key].tx);
          }
        }
      }
      if (!bugs.childSkipsDeletionMetadata) {
        durable.maxDeletedV = Max(durable.maxDeletedV, page.maxDeletedV);
        durable.maxDeleteTxOrderTs = Max(durable.maxDeleteTxOrderTs, page.maxDeleteTxOrderTs);
      }
      durable.cursor = page.next;
      if (page.done) {
        SetImport(IMPORTED);
      } else if (durable.importState == AWAITING_DATA) {
        SetImport(IMPORTING);
      }
      ImportStep();
    }

    // fokosMigrationAck: acceptAck records the acknowledgement of a target. When every target
    // acknowledged, the same transaction writes `completed`. A repeated acknowledgement after that is
    // a success.
    on eMigrationAck do (req: tMigrationAckReq) {
      var target: tSplitTarget;
      var open: bool;
      if (durable.split == SPLIT_CUTOVER) {
        durable.children[req.slice].acked = true;
        announce eAckRecorded, (source = this, target = durable.children[req.slice].child);
        foreach (target in durable.children) {
          open = open || !target.acked;
        }
        if (!open) {
          SetSplit(SPLIT_COMPLETED);
          send this, eSourceStep;
        }
      }
      send req.caller, eMigrationAckResp;
    }

    // The answer of fokosMigrationAck: sendAck writes `active`.
    on eMigrationAckResp do {
      if (durable.importState == IMPORTED) {
        SetImport(ACTIVE);
      }
    }
  }

  // A split source in cutover, completed, or cleaned owns no key.
  fun IsRouter(): bool {
    return durable.split == SPLIT_CUTOVER || durable.split == SPLIT_COMPLETED || durable.split == SPLIT_CLEANED;
  }

  // The child that owns a key of this router.
  fun OwnerOf(key: tKey): machine {
    return durable.children[ChildIndexOf(key.hk)].child;
  }

  // A target in awaiting_data or importing holds only some of the rows of its slice.
  fun Importing(): bool {
    return durable.importState == AWAITING_DATA || durable.importState == IMPORTING;
  }

  // The import gate: a target that imports answers partition_migrating. Returns true when it did.
  fun Migrating(caller: machine): bool {
    if (Importing()) {
      send caller, eRpcBroken;
    }
    return Importing();
  }

  // The import gate and the forward of a `point` request. Returns true when this partition does not
  // run the request.
  fun Routed(caller: machine, key: tKey, ev: event, req: any): bool {
    if (Migrating(caller)) {
      return true;
    }
    if (IsRouter()) {
      Forward(caller, OwnerOf(key), ev, req);
    }
    return IsRouter();
  }

  fun Forward(caller: machine, target: machine, ev: event, req: any) {
    nextReqId = nextReqId + 1;
    forwards[nextReqId] = caller;
    send NewForwardRpc(this, target, env, nextReqId), ev, req;
  }

  // The keys of a `group` or a `single_owner` request on a router, by the child that owns each one.
  fun KeysByOwner(ks: seq[tKey]): map[machine, seq[tKey]] {
    var parts: map[machine, seq[tKey]];
    var key: tKey;
    var child: machine;
    foreach (key in ks) {
      child = OwnerOf(key);
      if (!(child in parts)) {
        parts[child] = default(seq[tKey]);
      }
      parts[child] += (sizeof(parts[child]), key);
    }
    return parts;
  }

  fun NewFanout(caller: machine, reqId: int, kind: tFanoutKind, children: seq[machine]): int {
    var f: tFanout;
    var child: machine;
    f.caller = caller;
    f.reqId = reqId;
    f.kind = kind;
    foreach (child in children) {
      f.waiting += (child);
    }
    nextReqId = nextReqId + 1;
    fanouts[nextReqId] = f;
    return nextReqId;
  }

  // One part of a group request settled. txPrepare and txReadForTransaction are fail_fast: the
  // request throws at the first part that failed. txCommit and txCancel are attempt_all: the
  // request waits for every part, and then throws partition_fanout_failed when a part failed.
  // Otherwise the answer merges the parts: one rejected prepare rejects the request.
  fun PartSettled(id: int, child: machine, failed: bool) {
    var f: tFanout;
    var outcome: tPrepareOutcome;
    if (!(id in fanouts)) {
      return;
    }
    f = fanouts[id];
    f.waiting -= (child);
    f.failed = f.failed || failed;
    fanouts[id] = f;
    if (f.failed && (f.kind == FAN_PREPARE || f.kind == FAN_READ || sizeof(f.waiting) == 0)) {
      send f.caller, eRpcBroken;
    } else if (sizeof(f.waiting) > 0) {
      return;
    } else if (f.kind == FAN_PREPARE) {
      outcome = PREPARE_ACCEPTED;
      if (f.rejected) {
        outcome = PREPARE_REJECTED;
      }
      send f.caller, eTxPrepareResp, (reqId = f.reqId, partition = this, outcome = outcome);
    } else if (f.kind == FAN_COMMIT) {
      send f.caller, eTxCommitResp, (reqId = f.reqId, partition = this, ok = true);
    } else if (f.kind == FAN_CANCEL) {
      send f.caller, eTxCancelResp, (reqId = f.reqId, partition = this);
    } else {
      send f.caller, eReadForTransactionResp, (reqId = f.reqId, partition = this, items = f.items);
    }
    fanouts -= (id);
  }

  fun SetSplit(current: tSplitState) {
    durable.split = current;
    announce eSplitState, (partition = this, current = current);
    UpdateAlarm();
  }

  fun SetImport(current: tImportState) {
    durable.importState = current;
    announce eImportState, (partition = this, current = current);
    UpdateAlarm();
  }

  // #cutover: from here the source is a router, and its lock rows are transfer copies. Then
  // #notifyStart tells each target to start its import.
  fun Cutover() {
    var target: tSplitTarget;
    var owners: seq[machine];
    var tx: int;
    SetSplit(SPLIT_CUTOVER);
    foreach (target in durable.children) {
      owners += (sizeof(owners), target.child);
    }
    announce eOwnerChanged, (source = this, children = owners);
    if (bugs.routerDeletesCopiesAtCutover) {
      foreach (tx in keys(durable.pendingTx)) {
        DeleteCopies(tx);
      }
    }
    foreach (target in durable.children) {
      send target.child, eStartImport;
    }
  }

  // PartitionStore.deletePendingTxCopiesBatch: deletes the lock copies of one whole transaction, and
  // its pending_tx_info row.
  fun DeleteCopies(tx: int) {
    var key: tKey;
    foreach (key in RowsOf(tx)) {
      durable.locks -= (key);
      announce eCopyDeleted, (partition = this, key = key, tx = tx, target = OwnerOf(key));
      announce eLockDeleted, (partition = this, key = key, tx = tx);
    }
    durable.pendingTx -= (tx);
  }

  // FokosMigrationHost.buildPage: one row of the slice at the cursor. The `items` stream hands over
  // to the `pending_tx` stream. Each page of the `pending_tx` stream carries the deletion metadata, so
  // a slice with no lock receives it in one empty page.
  fun BuildPage(slice: int, cursor: tCursor): tMigrationPage {
    var page: tMigrationPage;
    var rows: seq[tKey];
    var key: tKey;
    var tx: int;
    page.cursor = cursor;
    if (cursor.stream == STREAM_ITEMS) {
      foreach (key in keys(durable.items)) {
        if (ChildIndexOf(key.hk) == slice) {
          rows += (sizeof(rows), key);
        }
      }
      if (cursor.index < sizeof(rows)) {
        page.items[rows[cursor.index]] = durable.items[rows[cursor.index]];
      }
      if (cursor.index + 1 < sizeof(rows)) {
        page.next = (stream = STREAM_ITEMS, index = cursor.index + 1);
      } else {
        page.next = (stream = STREAM_PENDING_TX, index = 0);
      }
      return page;
    }
    foreach (key in keys(durable.locks)) {
      if (ChildIndexOf(key.hk) == slice) {
        rows += (sizeof(rows), key);
      }
    }
    if (cursor.index < sizeof(rows)) {
      key = rows[cursor.index];
      tx = durable.locks[key].tx;
      page.locks[key] = durable.locks[key];
      page.pendingTx[tx] = durable.pendingTx[tx];
    }
    page.maxDeletedV = durable.maxDeletedV;
    page.maxDeleteTxOrderTs = durable.maxDeleteTxOrderTs;
    page.next = (stream = STREAM_PENDING_TX, index = cursor.index + 1);
    page.done = cursor.index + 1 >= sizeof(rows);
    return page;
  }

  // One step of the import job: importOnePage pulls the page of the durable cursor, and sendAck
  // acknowledges an import that ended.
  fun ImportStep() {
    if (Importing()) {
      send NewRpc(this, durable.source, env), eMigrationPull,
        (caller = this, reqId = 0, slice = durable.slice, cursor = durable.cursor);
    } else if (durable.importState == IMPORTED) {
      send NewRpc(this, durable.source, env), eMigrationAck, (caller = this, reqId = 0, slice = durable.slice);
    }
  }

  // A local decision changed the lock row of a key. On a router the row is a transfer copy.
  fun Mutated(key: tKey, tx: int) {
    if (IsRouter()) {
      announce eCopyChanged, (partition = this, key = key, tx = tx);
    } else {
      announce eLocalTxMutation, (partition = this, key = key, tx = tx);
    }
  }

  // readItemLocally: the committed row of the key.
  fun ReadItemLocally(req: tGetItemReq) {
    var row: tItemRow;
    if (!(req.key in durable.items)) {
      send req.caller, eGetItemResp, (reqId = req.reqId, found = false, value = 0, version = 0);
      return;
    }
    row = durable.items[req.key];
    send req.caller, eGetItemResp, (reqId = req.reqId, found = true, value = row.value, version = row.v);
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

  // PartitionStore.deleteItem of a present row: removes the row, raises max_deleted_v to its delete
  // revision v + 1, and raises max_delete_tx_order_ts to the larger of `ts` and its last_read_ts. The
  // delete announces its delete revision as `v`.
  fun DeleteItem(key: tKey, tx: int, ts: int) {
    var removed: tItemRow;
    removed = durable.items[key];
    durable.items -= (key);
    durable.maxDeletedV = Max(durable.maxDeletedV, removed.v + 1);
    durable.maxDeleteTxOrderTs = Max(durable.maxDeleteTxOrderTs, Max(ts, removed.lastReadTs));
    announce eItemWritten, (partition = this, key = key, deleted = true, value = 0, v = removed.v + 1, tx = tx);
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
    Mutated(key, tx);
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
    var key: tKey;
    info = durable.pendingTx[tx];
    if (info.guarded) {
      return;
    }
    info.guarded = true;
    durable.pendingTx[tx] = info;
    announce eLockGuarded, (partition = this, tx = tx);
    foreach (key in RowsOf(tx)) {
      Mutated(key, tx);
    }
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
  // coordinator. The step ends when no transaction is due, or when the job cannot run.
  fun ClaimNext() {
    var tx: int;
    var best: int;
    var info: tPendingTxInfo;
    best = 0;
    foreach (tx in keys(durable.pendingTx)) {
      info = durable.pendingTx[tx];
      if (CanSweepLocally() && !info.guarded && info.nextRecoveryAt <= step.startedAt
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

  // canSweepLocally: a router and a target that imports do not own complete lock state, so the stale
  // job does not run there.
  fun CanSweepLocally(): bool {
    return !IsRouter() && !Importing();
  }

  // The deadline of the job: the earliest next_recovery_at of an unguarded transaction.
  fun HasDeadline(): bool {
    var info: tPendingTxInfo;
    foreach (info in values(durable.pendingTx)) {
      if (CanSweepLocally() && !info.guarded) {
        return true;
      }
    }
    return false;
  }

  fun IsDue(): bool {
    var info: tPendingTxInfo;
    foreach (info in values(durable.pendingTx)) {
      if (CanSweepLocally() && !info.guarded && info.nextRecoveryAt <= now) {
        return true;
      }
    }
    return false;
  }

  // The runtime arms the alarm at the earliest deadline of its jobs, and clears it when no deadline is
  // left. The source_repartition job has a deadline from `queued` until `cleaned`, and the import job
  // until `active`.
  fun UpdateAlarm() {
    var armed: bool;
    armed = HasDeadline() || (durable.split != NOT_SOURCE && durable.split != SPLIT_CLEANED)
      || (durable.importState != NOT_TARGET && durable.importState != ACTIVE);
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
