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
type tBugs = (V1: bool);

fun NoBugs(): tBugs {
  return (V1 = false,);
}

// The single-item RPCs of PartitionDO.
type tPutItemReq = (caller: machine, reqId: int, key: tKey, value: tValue);
type tPutItemResp = (reqId: int, version: int);
type tDeleteItemReq = (caller: machine, reqId: int, key: tKey);
type tDeleteItemResp = (reqId: int, deleted: bool);
type tGetItemReq = (caller: machine, reqId: int, key: tKey);
type tGetItemResp = (reqId: int, found: bool, value: tValue, version: int);

event ePutItem: tPutItemReq;
event ePutItemResp: tPutItemResp;
event eDeleteItem: tDeleteItemReq;
event eDeleteItemResp: tDeleteItemResp;
event eGetItem: tGetItemReq;
event eGetItemResp: tGetItemResp;

// Monitor event: a partition changed an item row. A put gives the new value and its v. A delete
// gives the v of the row that it removed. A delete of an absent row changes nothing and is not
// announced.
type tItemWritten = (partition: machine, key: tKey, deleted: bool, value: tValue, v: int);
event eItemWritten: tItemWritten;
