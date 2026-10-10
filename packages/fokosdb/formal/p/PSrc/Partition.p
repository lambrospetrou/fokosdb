// One row of `pending_transactions`: the lock of a key, its transaction, and its operation.
type tLockRow = (tx: int, op: tTxOp);

// The SQLite state of a partition: the `items` table, `max_deleted_v` of the `deletion_metadata`
// row, and the lock rows.
type tPartitionDurable = (items: map[tKey, tItemRow], maxDeletedV: int, locks: map[tKey, tLockRow]);

// PartitionDO with its PartitionStore and TransactionParticipant. Each handler is one block of the
// code. The topology is fixed, so this partition owns every key that it receives.
machine Partition {
  var bugs: tBugs;
  var durable: tPartitionDurable;

  start state Init {
    entry (b: tBugs) {
      bugs = b;
      goto Serving;
    }
  }

  state Serving {
    // apiPutItem: putItemLocal refuses a key with a lock row (item_locked_by_transaction), then
    // PartitionStore.upsertItem writes the row.
    on ePutItem do (req: tPutItemReq) {
      var v: int;
      if (req.key in durable.locks && !bugs.W2) {
        send req.caller, ePutItemResp, (reqId = req.reqId, locked = true, version = 0);
        return;
      }
      v = WriteItem(req.key, req.value, 0);
      send req.caller, ePutItemResp, (reqId = req.reqId, locked = false, version = v);
    }

    // apiDeleteItem: deleteItemLocal refuses a key with a lock row, then PartitionStore.deleteItem
    // removes the row and raises max_deleted_v to its v.
    on eDeleteItem do (req: tDeleteItemReq) {
      var removed: tItemRow;
      if (req.key in durable.locks) {
        send req.caller, eDeleteItemResp, (reqId = req.reqId, locked = true, deleted = false);
        return;
      }
      if (!(req.key in durable.items)) {
        send req.caller, eDeleteItemResp, (reqId = req.reqId, locked = false, deleted = false);
        return;
      }
      removed = durable.items[req.key];
      durable.items -= (req.key);
      if (removed.v > durable.maxDeletedV) {
        durable.maxDeletedV = removed.v;
      }
      announce eItemWritten, (partition = this, key = req.key, deleted = true, value = 0, v = removed.v, tx = 0);
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

    // txPrepare: prepareLocal. A lock of another transaction gives pending_conflict, and a failed
    // condition gives condition_failed. Each rejection writes no lock. Otherwise the lock block
    // writes one lock row for each key. A repeated prepare finds its own locks and passes.
    on eTxPrepare do (req: tPrepareReq) {
      var i: int;
      var op: tTxOp;
      while (i < sizeof(req.items)) {
        op = req.items[i];
        i = i + 1;
        if (op.key in durable.locks) {
          if (durable.locks[op.key].tx != req.tx) {
            send req.caller, eTxPrepareResp, (reqId = req.reqId, partition = this, outcome = PREPARE_REJECTED);
            return;
          }
          continue;
        }
        if ((op.condition == EXISTS && !(op.key in durable.items)) || (op.condition == NOT_EXISTS && op.key in durable.items)) {
          send req.caller, eTxPrepareResp, (reqId = req.reqId, partition = this, outcome = PREPARE_REJECTED);
          return;
        }
      }
      foreach (op in req.items) {
        if (!(op.key in durable.locks)) {
          durable.locks[op.key] = (tx = req.tx, op = op);
          announce eLockWritten, (partition = this, key = op.key, tx = req.tx);
        }
      }
      send req.caller, eTxPrepareResp, (reqId = req.reqId, partition = this, outcome = PREPARE_ACCEPTED);
    }

    // txCommit: commitLocal. With no lock row of the transaction left, it answers the idempotent
    // success. Otherwise the keys of the request must be the keys of the lock rows, or it throws
    // commit_keyset_mismatch. It applies the operation of each lock row, and deletes one lock row
    // for each key.
    on eTxCommit do (req: tCommitReq) {
      var owned: set[tKey];
      var key: tKey;
      var requested: set[tKey];
      foreach (key in keys(durable.locks)) {
        if (durable.locks[key].tx == req.tx) {
          owned += (key);
        }
      }
      if (sizeof(owned) == 0) {
        send req.caller, eTxCommitResp, (reqId = req.reqId, partition = this, ok = true);
        return;
      }
      foreach (key in req.itemKeys) {
        requested += (key);
      }
      if (owned != requested) {
        send req.caller, eTxCommitResp, (reqId = req.reqId, partition = this, ok = false);
        return;
      }
      foreach (key in owned) {
        WriteItem(key, durable.locks[key].op.value, req.tx);
        announce eApplied, (partition = this, key = key, tx = req.tx);
        durable.locks -= (key);
        announce eLockDeleted, (partition = this, key = key, tx = req.tx);
      }
      send req.caller, eTxCommitResp, (reqId = req.reqId, partition = this, ok = true);
    }

    // txCancel: cancelLocal deletes the lock rows of the transaction under the keys of the request.
    on eTxCancel do (req: tCancelReq) {
      var key: tKey;
      foreach (key in req.itemKeys) {
        if (key in durable.locks && durable.locks[key].tx == req.tx) {
          durable.locks -= (key);
          announce eReleased, (partition = this, key = key, tx = req.tx);
          announce eLockDeleted, (partition = this, key = key, tx = req.tx);
        }
      }
      send req.caller, eTxCancelResp, (reqId = req.reqId, partition = this);
    }
  }

  // PartitionStore.upsertItem: a new row starts at max_deleted_v + 1, and an existing row runs
  // v = v + 1. Returns the new v.
  fun WriteItem(key: tKey, value: tValue, tx: int): int {
    var row: tItemRow;
    if (key in durable.items) {
      row = durable.items[key];
      row.v = row.v + 1;
    } else if (bugs.V1) {
      row.v = 1;
    } else {
      row.v = durable.maxDeletedV + 1;
    }
    row.value = value;
    durable.items[key] = row;
    announce eItemWritten, (partition = this, key = key, deleted = false, value = value, v = row.v, tx = tx);
    return row.v;
  }
}
