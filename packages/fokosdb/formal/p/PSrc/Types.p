// A key is a pair of a hash key and a sort key: key a1 is (hk = "a", sk = "1").
type tKey = (hk: string, sk: string);

// The id of the writer of a value. Each write writes a unique id, so a monitor can tell which
// writer produced a value that a read returns.
type tValue = int;

// One row of the `items` table.
type tItemRow = (v: int, value: tValue, lastReadTs: int, lastWriteTs: int);

// The seeded defects. Each field turns on one deliberate defect, and the test case of the same name
// must then report a violation: commitOnOneAccept has the test case tcBugCommitOnOneAccept.
//   newRowVersionFromOne: a new row starts at v = 1, not at max_deleted_v + 1.
//   commitOnOneAccept: drivePrepare and markCommitting decide commit when one participant accepted.
//   putIgnoresLock: apiPutItem ignores the lock of the key.
//   committedBeforeApply: drivePrepare answers committed after markCommitting, before runCommit ends.
//   tokenRowIgnored: initiateWriteLocal ignores the stored row of the token.
//   staleCancelsOnDriving: the stale job of a partition cancels on the answer driving.
//   cancelInAnyState: runCancel sends cancels in every state, not only in CANCELLING.
//   noPreparingHold: runPrepareRecovery has no maxPreparingHoldMs bound.
//   prepareSkipsWatermark: prepareLocal skips the timestamp watermark check.
//   singleShotIgnoresLock: executeSingleShot ignores the lock of a key.
//   newRowTsFromOwnStamp: a new row starts its timestamps at its own stamp, not at least
//     max_delete_tx_order_ts.
//   absentCheckKeepsNoStamp: the commit of a check of an absent item does not raise
//     max_delete_tx_order_ts.
//   quarantineStampedChecks: the stale job quarantines an over-age not_found lock also when it holds
//     only checks whose stamps already cover the transaction.
//   readSkipsPhaseTwo: the two-phase path of transactGetItems returns the result of phase 1.
//   readIgnoresPendingWrite: the two-phase path ignores hasPendingWrite.
//   readIgnoresMaxDeletedV: the two-phase path does not compare maxDeletedV for an absent item.
//   childSkipsDeletionMetadata: a child of a split does not merge the deletion metadata of its source.
//   importingChildServesPrepare: a child serves txPrepare while it imports.
//   routerServesGetItem: a split router serves apiGetItem from its own item rows.
//   importSkipsPendingTx: the import inserts no lock row of the pending_tx stream.
//   routerDeletesCopiesAtCutover: a split router deletes its lock copies at the cutover.
type tBugs = (newRowVersionFromOne: bool, commitOnOneAccept: bool, putIgnoresLock: bool, committedBeforeApply: bool,
              tokenRowIgnored: bool, staleCancelsOnDriving: bool, cancelInAnyState: bool, noPreparingHold: bool,
              prepareSkipsWatermark: bool, singleShotIgnoresLock: bool, newRowTsFromOwnStamp: bool,
              absentCheckKeepsNoStamp: bool, quarantineStampedChecks: bool, readSkipsPhaseTwo: bool,
              readIgnoresPendingWrite: bool, readIgnoresMaxDeletedV: bool, childSkipsDeletionMetadata: bool,
              importingChildServesPrepare: bool, routerServesGetItem: bool, importSkipsPendingTx: bool,
              routerDeletesCopiesAtCutover: bool);

fun NoBugs(): tBugs {
  return (newRowVersionFromOne = false, commitOnOneAccept = false, putIgnoresLock = false, committedBeforeApply = false,
          tokenRowIgnored = false, staleCancelsOnDriving = false, cancelInAnyState = false, noPreparingHold = false,
          prepareSkipsWatermark = false, singleShotIgnoresLock = false, newRowTsFromOwnStamp = false,
          absentCheckKeepsNoStamp = false, quarantineStampedChecks = false, readSkipsPhaseTwo = false,
          readIgnoresPendingWrite = false, readIgnoresMaxDeletedV = false, childSkipsDeletionMetadata = false,
          importingChildServesPrepare = false, routerServesGetItem = false, importSkipsPendingTx = false,
          routerDeletesCopiesAtCutover = false);
}

// The clock of a Durable Object, as transaction order timestamps read it (txOrderTimestampNow). A
// timestamp is (tick + 1) * TS_PER_TICK + fine + offset. The + 1 keeps it above 0 as in the code,
// and a tick moves it above every value of the tick before. The ages of the recovery jobs use whole
// ticks with no fine step and no offset, so the parts below do not change the recovery timing.
//
// The three parts model three different facts:
//
// - tick: the wall clock of the Environment. A tick is about staleTransactionMs, and it moves only
//   while no call is open, because it moves the ages of the recovery jobs. Thus all the calls of a
//   burst of requests read the same tick.
//
// - fine: real time that passes inside a tick. In the code, time passes between two requests, so two
//   transactions of one coordinator can get the same timestamp (the same millisecond) or different
//   ones. With the tick alone, every transaction of one tick gets the same timestamp. Then a second
//   transaction on a shared key always gets timestamp_conflict, and a check with EXISTS after the
//   put that created the item never commits. A fine step moves the clock of every Durable Object
//   together, so it adds no skew.
//
// - offset: the difference between the clocks of the Durable Objects, which run on different
//   machines. Without it, all clocks are equal. Then a prepare is never ahead of the partition, so
//   clock_skew cannot occur. A single-item write or a single-partition transaction also never stamps
//   below an earlier stamp of the coordinator, so MAX never absorbs a clock that lags, and the
//   defect newRowTsFromOwnStamp cannot show. An offset that goes down also lets a later transaction
//   get a lower timestamp.
//
// Each part needs its own mechanism. An offset move in every test case adds real skew, and
// clock_skew then cancels transactions of a test case that has no skew. A fine step for one Durable
// Object alone also adds skew. Thus the Environment makes fine steps in every test case, and offset
// moves only in a test case with clock skew.
fun TS_PER_TICK(): int { return 100; }

// maxClockSkewMs in the unit of a timestamp. The Environment keeps each offset between 0 and
// MAX_CLOCK_SKEW() + 1, so the real skew can pass the configured bound and clock_skew can occur.
fun MAX_CLOCK_SKEW(): int { return 1; }

type tClock = (fine: int, offset: int);
event eClock: tClock;

fun TsOf(tick: int, clock: tClock): int {
  return (tick + 1) * TS_PER_TICK() + clock.fine + clock.offset;
}

// Time is in ticks of the clock of the Environment. These values keep the order of the defaults of the code:
// staleTransactionMs < maxPreparingHoldMs <= IDEMPOTENCY_WINDOW_MS, and STALE_RECOVERY_MAX_DELAY_MS
// below the window.
fun STALE_TRANSACTION(): int { return 1; }
fun MAX_PREPARING_HOLD(): int { return 2; }
fun IDEMPOTENCY_WINDOW(): int { return 3; }
fun STALE_RECOVERY_MAX_DELAY(): int { return 2; }

// nextRecoveryAt: the wait is half the age of the transaction, at least the stale time and at most
// the longest delay.
fun NextRecoveryAt(now: int, createdAt: int): int {
  var wait: int;
  wait = (now - createdAt + 1) / 2;
  if (wait > STALE_RECOVERY_MAX_DELAY()) {
    wait = STALE_RECOVERY_MAX_DELAY();
  }
  if (wait < STALE_TRANSACTION()) {
    wait = STALE_TRANSACTION();
  }
  return now + wait;
}

// The attempts of one call of the coordinator: prepareMaxAttempts, prepareRecoveryMaxAttempts, and
// the retries of a commit or a cancel inside fanoutRequestBudgetMs.
fun PREPARE_MAX_ATTEMPTS(): int { return 3; }
fun PREPARE_RECOVERY_MAX_ATTEMPTS(): int { return 5; }
fun FANOUT_MAX_ATTEMPTS(): int { return 3; }

// The attempts of one read call of transactGetItems: maxAttempts of the default retry options.
fun READ_MAX_ATTEMPTS(): int { return 5; }

// The single-item RPCs of PartitionDO. `caller` is the machine that gets the answer. `locked` is
// the error item_locked_by_transaction.
type tPutItemReq = (caller: machine, reqId: int, key: tKey, value: tValue);
type tPutItemResp = (reqId: int, locked: bool, version: int);
type tDeleteItemReq = (caller: machine, reqId: int, key: tKey);
type tDeleteItemResp = (reqId: int, locked: bool, deleted: bool);
type tGetItemReq = (caller: machine, reqId: int, key: tKey);
type tGetItemResp = (reqId: int, found: bool, value: tValue, version: int);

event ePutItem: tPutItemReq;
event ePutItemResp: tPutItemResp;
event eDeleteItem: tDeleteItemReq;
event eDeleteItemResp: tDeleteItemResp;
event eGetItem: tGetItemReq;
event eGetItemResp: tGetItemResp;

// A write transaction. Each operation is a put, a delete, or a check of one item, with an optional
// condition on the item. A check always has a condition. `value` belongs to a put.
enum tOperation { OP_PUT, OP_DELETE, OP_CHECK }
enum tCondition { NO_CONDITION, EXISTS, NOT_EXISTS }
type tTxOp = (key: tKey, operation: tOperation, condition: tCondition, value: tValue);

// The answer of initiateWrite that the client gives the caller: the outcome committed or cancelled,
// or the error transaction_commit_pending or transaction_undecided.
enum tWriteAnswer { ANSWER_COMMITTED, ANSWER_CANCELLED, ANSWER_COMMIT_PENDING, ANSWER_UNDECIDED }

type tInitiateWriteReq = (caller: machine, reqId: int, token: int, items: seq[tTxOp]);
type tInitiateWriteResp = (reqId: int, tx: int, answer: tWriteAnswer);

event eInitiateWrite: tInitiateWriteReq;
event eInitiateWriteResp: tInitiateWriteResp;

// The participant RPCs. `reqId` is the drive of the coordinator that sent the request, and `ts` is
// the transaction timestamp. `ok` false on a commit is the error commit_keyset_mismatch.
enum tPrepareOutcome { PREPARE_NONE, PREPARE_ACCEPTED, PREPARE_REJECTED }

type tPrepareReq = (caller: machine, reqId: int, coordinator: machine, tx: int, ts: int, items: seq[tTxOp]);
type tPrepareResp = (reqId: int, partition: machine, outcome: tPrepareOutcome);
type tCommitReq = (caller: machine, reqId: int, tx: int, ts: int, itemKeys: seq[tKey]);
type tCommitResp = (reqId: int, partition: machine, ok: bool);
type tCancelReq = (caller: machine, reqId: int, tx: int, itemKeys: seq[tKey]);
type tCancelResp = (reqId: int, partition: machine);

event eTxPrepare: tPrepareReq;
event eTxPrepareResp: tPrepareResp;
event eTxCommit: tCommitReq;
event eTxCommitResp: tCommitResp;
event eTxCancel: tCancelReq;
event eTxCancelResp: tCancelResp;

// txExecuteSingleShot and its answer: committed, or rejected with nothing written. `notApplicable`
// is the outcome not_applicable: one owner does not hold every key of the request.
type tSingleShotReq = (caller: machine, reqId: int, items: seq[tTxOp]);
type tSingleShotResp = (reqId: int, notApplicable: bool, committed: bool);

event eExecuteSingleShot: tSingleShotReq;
event eExecuteSingleShotResp: tSingleShotResp;

// txReadForTransaction and txReadSnapshot, and their answers: one item for each requested key.
// `maxDeletedV` is the max_deleted_v of the partition. `aborted` is the snapshot outcome for a
// pending write, and its answer then holds no item. `notApplicable` is the snapshot outcome
// not_applicable: one owner does not hold every key of the request.
type tReadReq = (caller: machine, reqId: int, readKeys: seq[tKey]);
type tReadItem = (key: tKey, found: bool, value: tValue, version: int, maxDeletedV: int, hasPendingWrite: bool);
type tReadForTransactionResp = (reqId: int, partition: machine, items: seq[tReadItem]);
type tReadSnapshotResp = (reqId: int, notApplicable: bool, aborted: bool, items: seq[tReadItem]);

event eReadForTransaction: tReadReq;
event eReadForTransactionResp: tReadForTransactionResp;
event eReadSnapshot: tReadReq;
event eReadSnapshotResp: tReadSnapshotResp;

// recoverTransactionForParticipant and its answer.
enum tRecoverState { RECOVER_COMMITTED, RECOVER_CANCELLED, RECOVER_NOT_FOUND, RECOVER_DRIVING }
type tRecoverReq = (caller: machine, reqId: int, tx: int);
type tRecoverResp = (reqId: int, ledgerState: tRecoverState);

event eRecover: tRecoverReq;
event eRecoverResp: tRecoverResp;

// debugForceResolveTransaction and its answer.
type tForceResolveReq = (caller: machine, reqId: int, tx: int, commit: bool);
type tForceResolveResp = (reqId: int, partition: machine);

event eForceResolve: tForceResolveReq;
event eForceResolveResp: tForceResolveResp;

// The repartition flow of a hash split: the states of the source (RepartitionSource) and of a target
// (RepartitionTarget). NOT_SOURCE is a partition that queued no split, and NOT_TARGET is a root.
enum tSplitState { NOT_SOURCE, SPLIT_QUEUED, SPLIT_PLANNED, SPLIT_CUTOVER, SPLIT_COMPLETED, SPLIT_CLEANED }
enum tImportState { NOT_TARGET, AWAITING_DATA, IMPORTING, IMPORTED, ACTIVE }

// RepartitionSource.queue: the Environment requests the hash split of a partition.
event eRequestSplit;

// One step of the source_repartition job, from the alarm of the source.
event eSourceStep;

// fokosInit and its answer. `slice` is the child index of the target, and the answer repeats it.
type tFokosInitReq = (caller: machine, reqId: int, source: machine, slice: int);
event eFokosInit: tFokosInitReq;
event eFokosInitResp: int;

// fokosStartImport: the source cut over, and the target starts its import.
event eStartImport;

// fokosMigrationPull and its page. A cursor is a stream and the index of the next row of the slice
// in it. A page holds at most one row. Each page of the pending_tx stream carries the deletion
// metadata of the source. `cursor` is the cursor of the request, and `done` marks the last page.
enum tStream { STREAM_ITEMS, STREAM_PENDING_TX }
type tCursor = (stream: tStream, index: int);
type tMigrationPullReq = (caller: machine, reqId: int, slice: int, cursor: tCursor);
type tMigrationPage = (cursor: tCursor, items: map[tKey, tItemRow], locks: map[tKey, tLockRow],
                       pendingTx: map[int, tPendingTxInfo], maxDeletedV: int, maxDeleteTxOrderTs: int, next: tCursor,
                       done: bool);
event eMigrationPull: tMigrationPullReq;
event eMigrationPage: tMigrationPage;

// fokosMigrationAck and its answer.
type tMigrationAckReq = (caller: machine, reqId: int, slice: int);
event eMigrationAck: tMigrationAckReq;
event eMigrationAckResp;

// fokosExecuteLocal for apiGetItem: the read-through of a target that imports. The answer is the
// answer of apiGetItem.
event eExecuteLocal: tGetItemReq;

// A call failed: the request or the answer got lost, or the target restarted or refused the call
// (partition_migrating). The caller does not know whether the target ran the request.
type tRpcFailed = (reqId: int, target: machine);
event eRpcFailed: tRpcFailed;

// A target that restarts tells each call that waits for its answer.
event eRpcBroken;

// The Environment: a restart of a Durable Object, and the loss decision of one call.
event eRestart;
enum tLoss { LOSE_NONE, LOSE_REQUEST, LOSE_ANSWER }
type tMayLose = (rpc: machine, target: machine);
event eMayLose: tMayLose;
event eLossDecision: tLoss;

// The clock of the Environment: the wall clock in ticks, and whether a Durable Object has an alarm
// deadline.
event eTick: int;
type tArmed = (who: machine, armed: bool);
event eArmed: tArmed;

// A Durable Object that a split created. The Environment sends it the clock, and can restart it.
event eDoCreated: machine;

// The alarm of a Durable Object fires.
event eAlarm;

// Records outside SQLite that an operator reads: the lock-age guard error of a partition, and the
// outcome that a coordinator logged at the completion of a transaction.
type tGuardLogged = (partition: machine, tx: int);
event eGuardLogged: tGuardLogged;
type tLogLookup = (operator: machine, tx: int);
event eLogLookup: tLogLookup;
type tLogEntry = (tx: int, known: bool, committed: bool);
event eLogEntry: tLogEntry;

// Monitor events.

// A partition changed an item row. A put gives the new value and its v. A delete gives the v of the
// row that it removed. `tx` is the transaction that wrote, or 0 for a single-item write and a
// single-partition transaction. A transactional delete of an absent row gives v 0. A single-item
// delete of an absent row changes nothing and is not announced.
type tItemWritten = (partition: machine, key: tKey, deleted: bool, value: tValue, v: int, tx: int);
event eItemWritten: tItemWritten;

// An operation applied on a key at timestamp `ts`. `twoPhase` is false for a single-item write and
// a single-partition transaction.
type tStamped = (partition: machine, key: tKey, operation: tOperation, ts: int, twoPhase: bool);
event eStamped: tStamped;

// A partition wrote or deleted the lock row of a key.
type tLockEvent = (partition: machine, key: tKey, tx: int);
event eLockWritten: tLockEvent;
event eLockDeleted: tLockEvent;

// commitLocal applied a key of a transaction, and cancelLocal released the lock of a key.
event eApplied: tLockEvent;
event eReleased: tLockEvent;

// A partition set guarded_at on the pending_tx_info row of a transaction.
type tTxGuarded = (partition: machine, tx: int);
event eLockGuarded: tTxGuarded;

// The coordinator inserted a transaction.
event eTxCreated: int;

// The idempotency_sweep job deleted the transaction of a token. A later request with the token
// starts a new transaction.
event eTokenSwept: int;

// The coordinator wrote COMMITTING (commit) or CANCELLING (cancel) for a transaction of a token,
// with these keys. `writeKeys` are the keys of its puts and deletes.
type tDecision = (tx: int, token: int, commit: bool, itemKeys: set[tKey], writeKeys: set[tKey]);
event eDecision: tDecision;

// The coordinator wrote COMMITTED or CANCELLED.
type tTxCompleted = (tx: int, committed: bool);
event eTxCompleted: tTxCompleted;

// A client got the answer of a write transaction.
type tClientAnswer = (client: machine, tx: int, answer: tWriteAnswer);
event eClientAnswer: tClientAnswer;

// A client sent a getItem or a transactGetItems, and got its result. `transaction` is true for a
// transactGetItems. A read that ends with an error has no result.
type tReadStart = (client: machine, reqId: int, transaction: bool);
type tReadValue = (key: tKey, found: bool, value: tValue);
type tReadResult = (client: machine, reqId: int, transaction: bool, results: seq[tReadValue]);
event eReadStart: tReadStart;
event eReadResult: tReadResult;

// A client sent a call, and got its answer or an error.
type tClientCall = (client: machine, reqId: int);
event eCallStarted: tClientCall;
event eCallEnded: tClientCall;

// A local decision (a prepare, a commit, a cancel, or a quarantine) wrote, applied, released, or
// guarded the lock row of a key that the partition owns by the topology, or changed a transfer copy:
// the lock row of a key that the partition gave to a target.
event eLocalTxMutation: tLockEvent;
event eCopyChanged: tLockEvent;

// A source deleted a transfer copy. `target` is the target that owns the key.
type tCopyDeleted = (partition: machine, key: tKey, tx: int, target: machine);
event eCopyDeleted: tCopyDeleted;

// A source recorded the acknowledgement of a target.
type tAckRecorded = (source: machine, target: machine);
event eAckRecorded: tAckRecorded;

// A source wrote a state of its split, and a target wrote a state of its import.
type tSplitStateChanged = (partition: machine, current: tSplitState);
event eSplitState: tSplitStateChanged;
type tImportStateChanged = (partition: machine, current: tImportState);
event eImportState: tImportStateChanged;

// A source cut over: it owns no key, and child i owns the keys of child index i.
type tOwnerChanged = (source: machine, children: seq[machine]);
event eOwnerChanged: tOwnerChanged;
