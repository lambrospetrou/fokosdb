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
type tBugs = (newRowVersionFromOne: bool, commitOnOneAccept: bool, putIgnoresLock: bool, committedBeforeApply: bool,
              tokenRowIgnored: bool, staleCancelsOnDriving: bool, cancelInAnyState: bool, noPreparingHold: bool);

fun NoBugs(): tBugs {
  return (newRowVersionFromOne = false, commitOnOneAccept = false, putIgnoresLock = false, committedBeforeApply = false,
          tokenRowIgnored = false, staleCancelsOnDriving = false, cancelInAnyState = false, noPreparingHold = false);
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

// A write transaction. Each operation is a put of one item, with an optional condition on the item.
enum tCondition { NO_CONDITION, EXISTS, NOT_EXISTS }
type tTxOp = (key: tKey, condition: tCondition, value: tValue);

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

// A call failed: the request or the answer got lost, or the target restarted. The caller does not
// know whether the target ran the request.
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
// row that it removed. `tx` is the transaction that wrote, or 0 for a single-item write. A delete of
// an absent row changes nothing and is not announced.
type tItemWritten = (partition: machine, key: tKey, deleted: bool, value: tValue, v: int, tx: int);
event eItemWritten: tItemWritten;

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
// with these keys.
type tDecision = (tx: int, token: int, commit: bool, itemKeys: set[tKey]);
event eDecision: tDecision;

// The coordinator wrote COMMITTED or CANCELLED.
type tTxCompleted = (tx: int, committed: bool);
event eTxCompleted: tTxCompleted;

// A client got the answer of a write transaction.
type tClientAnswer = (client: machine, tx: int, answer: tWriteAnswer);
event eClientAnswer: tClientAnswer;

// A client sent a getItem, and got its result.
type tReadStart = (client: machine, reqId: int, key: tKey);
type tReadResult = (client: machine, reqId: int, key: tKey, found: bool, value: tValue);
event eReadStart: tReadStart;
event eReadResult: tReadResult;

// A client sent a call, and got its answer or an error.
type tClientCall = (client: machine, reqId: int);
event eCallStarted: tClientCall;
event eCallEnded: tClientCall;
