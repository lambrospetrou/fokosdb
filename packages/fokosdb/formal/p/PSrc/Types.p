// A key is a pair of a hash key and a sort key: key a1 is (hk = "a", sk = "1").
type tKey = (hk: string, sk: string);

// The id of the writer of a value. Each write writes a unique id, so a monitor can tell which
// writer produced a value that a read returns.
type tValue = int;

// One row of the `items` table.
type tItemRow = (v: int, value: tValue);

// The seeded defects. Each field turns on one deliberate defect, and the checker must then
// report a violation.
//   V1: a new row starts at v = 1, not at max_deleted_v + 1.
//   W1: drivePrepare and markCommitting decide commit when one participant accepted.
//   W2: apiPutItem ignores the lock of the key.
//   W3: drivePrepare answers committed after markCommitting, before runCommit ends.
type tBugs = (V1: bool, W1: bool, W2: bool, W3: bool);

fun NoBugs(): tBugs {
  return (V1 = false, W1 = false, W2 = false, W3 = false);
}

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

// The participant RPCs. `reqId` is the drive of the coordinator that sent the request. `ok` false on
// a commit is the error commit_keyset_mismatch.
enum tPrepareOutcome { PREPARE_NONE, PREPARE_ACCEPTED, PREPARE_REJECTED }

type tPrepareReq = (caller: machine, reqId: int, tx: int, items: seq[tTxOp]);
type tPrepareResp = (reqId: int, partition: machine, outcome: tPrepareOutcome);
type tCommitReq = (caller: machine, reqId: int, tx: int, itemKeys: seq[tKey]);
type tCommitResp = (reqId: int, partition: machine, ok: bool);
type tCancelReq = (caller: machine, reqId: int, tx: int, itemKeys: seq[tKey]);
type tCancelResp = (reqId: int, partition: machine);

event eTxPrepare: tPrepareReq;
event eTxPrepareResp: tPrepareResp;
event eTxCommit: tCommitReq;
event eTxCommitResp: tCommitResp;
event eTxCancel: tCancelReq;
event eTxCancelResp: tCancelResp;

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

// The coordinator wrote COMMITTING (commit) or CANCELLING (cancel) for a transaction with these keys.
type tDecision = (tx: int, commit: bool, itemKeys: set[tKey]);
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
