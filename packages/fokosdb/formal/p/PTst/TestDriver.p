fun A1(): tKey { return (hk = "a", sk = "1"); }
fun B1(): tKey { return (hk = "b", sk = "1"); }
fun B2(): tKey { return (hk = "b", sk = "2"); }

fun ItemOp(kind: tClientOpKind, key: tKey, value: tValue): tClientOp {
  return (kind = kind, key = key, value = value, token = 0, items = default(seq[tTxOp]));
}

fun TxOp(token: int, items: seq[tTxOp]): tClientOp {
  return (kind = TRANSACT_WRITE, key = default(tKey), value = 0, token = token, items = items);
}

fun Put(key: tKey, condition: tCondition, value: tValue): tTxOp {
  return (key = key, condition = condition, value = value);
}

// Root A holds hash key a, and root B holds hash key b. One coordinator. Each client runs its own
// list of calls.
fun StartSystem(bugs: tBugs, scripts: seq[seq[tClientOp]]) {
  var topology: tTopology;
  var coordinator: machine;
  var script: seq[tClientOp];
  topology["a"] = new Partition(bugs);
  topology["b"] = new Partition(bugs);
  coordinator = new Coordinator((bugs = bugs, topology = topology));
  foreach (script in scripts) {
    new Client((topology = topology, coordinator = coordinator, ops = script));
  }
}

// One client: a put, a delete, a put, and a get of a1.
fun RunItems(bugs: tBugs) {
  var ops: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  ops += (sizeof(ops), ItemOp(PUT_ITEM, A1(), 1));
  ops += (sizeof(ops), ItemOp(DELETE_ITEM, A1(), 2));
  ops += (sizeof(ops), ItemOp(PUT_ITEM, A1(), 3));
  ops += (sizeof(ops), ItemOp(GET_ITEM, A1(), 0));
  scripts += (0, ops);
  StartSystem(bugs, scripts);
}

// Client 1: T1 puts a1 and b1. Client 2: T2 puts b2.
fun RunWriteHappy(bugs: tBugs) {
  var t1: seq[tTxOp];
  var t2: seq[tTxOp];
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  t1 += (0, Put(A1(), NO_CONDITION, 11));
  t1 += (1, Put(B1(), NO_CONDITION, 12));
  t2 += (0, Put(B2(), NO_CONDITION, 21));
  c1 += (0, TxOp(1, t1));
  c2 += (0, TxOp(2, t2));
  scripts += (0, c1);
  scripts += (1, c2);
  StartSystem(bugs, scripts);
}

// Client 1: T1 puts a1 and b1, then a get of b1. Client 2: T2 puts a1 with not_exists and b2, then
// a put of a1.
fun RunWriteConflict(bugs: tBugs) {
  var t1: seq[tTxOp];
  var t2: seq[tTxOp];
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  t1 += (0, Put(A1(), NO_CONDITION, 11));
  t1 += (1, Put(B1(), NO_CONDITION, 12));
  t2 += (0, Put(A1(), NOT_EXISTS, 21));
  t2 += (1, Put(B2(), NO_CONDITION, 22));
  c1 += (0, TxOp(1, t1));
  c1 += (1, ItemOp(GET_ITEM, B1(), 0));
  c2 += (0, TxOp(2, t2));
  c2 += (1, ItemOp(PUT_ITEM, A1(), 31));
  scripts += (0, c1);
  scripts += (1, c2);
  StartSystem(bugs, scripts);
}

machine TestItems {
  start state Init { entry { RunItems(NoBugs()); } }
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

machine TestWriteHappy {
  start state Init { entry { RunWriteHappy(NoBugs()); } }
}

machine TestWriteConflict {
  start state Init { entry { RunWriteConflict(NoBugs()); } }
}

machine TestBugW1 {
  start state Init {
    entry {
      var bugs: tBugs;
      bugs = NoBugs();
      bugs.W1 = true;
      RunWriteConflict(bugs);
    }
  }
}

machine TestBugW2 {
  start state Init {
    entry {
      var bugs: tBugs;
      bugs = NoBugs();
      bugs.W2 = true;
      RunWriteConflict(bugs);
    }
  }
}

machine TestBugW3 {
  start state Init {
    entry {
      var bugs: tBugs;
      bugs = NoBugs();
      bugs.W3 = true;
      RunWriteConflict(bugs);
    }
  }
}
