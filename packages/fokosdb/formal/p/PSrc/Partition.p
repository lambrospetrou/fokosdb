// The SQLite state of a partition: the `items` table, and `max_deleted_v` of the
// `deletion_metadata` row.
type tPartitionDurable = (items: map[tKey, tItemRow], maxDeletedV: int);

// PartitionDO with its PartitionStore. Each handler is one block of the code.
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
    // apiPutItem: putItemLocal and PartitionStore.upsertItem. A new row starts at
    // max_deleted_v + 1, and an existing row runs v = v + 1.
    on ePutItem do (req: tPutItemReq) {
      var row: tItemRow;
      if (req.key in durable.items) {
        row = durable.items[req.key];
        row.v = row.v + 1;
      } else if (bugs.V1) {
        row.v = 1;
      } else {
        row.v = durable.maxDeletedV + 1;
      }
      row.value = req.value;
      durable.items[req.key] = row;
      announce eItemWritten, (partition = this, key = req.key, deleted = false, value = row.value, v = row.v);
      send req.caller, ePutItemResp, (reqId = req.reqId, version = row.v);
    }

    // apiDeleteItem: deleteItemLocal and PartitionStore.deleteItem. A removed row raises
    // max_deleted_v to its v.
    on eDeleteItem do (req: tDeleteItemReq) {
      var removed: tItemRow;
      if (!(req.key in durable.items)) {
        send req.caller, eDeleteItemResp, (reqId = req.reqId, deleted = false);
        return;
      }
      removed = durable.items[req.key];
      durable.items -= (req.key);
      if (removed.v > durable.maxDeletedV) {
        durable.maxDeletedV = removed.v;
      }
      announce eItemWritten, (partition = this, key = req.key, deleted = true, value = 0, v = removed.v);
      send req.caller, eDeleteItemResp, (reqId = req.reqId, deleted = true);
    }

    // apiGetItem: readItemLocally reads the committed row.
    on eGetItem do (req: tGetItemReq) {
      var row: tItemRow;
      if (!(req.key in durable.items)) {
        send req.caller, eGetItemResp, (reqId = req.reqId, found = false, value = 0, version = 0);
        return;
      }
      row = durable.items[req.key];
      send req.caller, eGetItemResp, (reqId = req.reqId, found = true, value = row.value, version = row.v);
    }
  }
}
