// One partition and one client: a put, a delete, a put, and a get of a1.
fun RunItems(bugs: tBugs) {
  var a1: tKey;
  var ops: seq[tItemOp];
  var partition: machine;
  a1 = (hk = "a", sk = "1");
  ops += (sizeof(ops), (kind = PUT_ITEM, key = a1, value = 1));
  ops += (sizeof(ops), (kind = DELETE_ITEM, key = a1, value = 0));
  ops += (sizeof(ops), (kind = PUT_ITEM, key = a1, value = 2));
  ops += (sizeof(ops), (kind = GET_ITEM, key = a1, value = 0));
  partition = new Partition(bugs);
  new Client((partition = partition, ops = ops));
}

machine TestItems {
  start state Init {
    entry {
      RunItems(NoBugs());
    }
  }
}

machine TestBugV1 {
  start state Init {
    entry {
      var bugs: tBugs;
      bugs = NoBugs();
      bugs.V1 = true;
      RunItems(bugs);
    }
  }
}
