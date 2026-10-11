fun A1(): tKey { return (hk = "a", sk = "1"); }
fun B1(): tKey { return (hk = "b", sk = "1"); }
fun B2(): tKey { return (hk = "b", sk = "2"); }

fun ItemOp(kind: tClientOpKind, key: tKey, value: tValue): tClientOp {
  return (kind = kind, key = key, value = value, token = 0, items = default(seq[tTxOp]), retries = 0, replay = false);
}

fun TxOp(token: int, items: seq[tTxOp]): tClientOp {
  return (kind = TRANSACT_WRITE, key = default(tKey), value = 0, token = token, items = items, retries = 0, replay = false);
}

fun Put(key: tKey, condition: tCondition, value: tValue): tTxOp {
  return (key = key, operation = OP_PUT, condition = condition, value = value);
}

fun Delete(key: tKey): tTxOp {
  return (key = key, operation = OP_DELETE, condition = NO_CONDITION, value = 0);
}

fun Check(key: tKey, condition: tCondition): tTxOp {
  return (key = key, operation = OP_CHECK, condition = condition, value = 0);
}

// The fine steps of the clock in every test case, and the offset moves in a test case with clock
// skew. A fine step is real time that passes, and an offset move is skew between two clocks
// (TS_PER_TICK in Types.p).
fun FINE_STEPS(): int { return 2; }
fun OFFSET_MOVES(): int { return 3; }

// The faults of a test case. `down` names the roots that drop every call. `skew` moves the offsets
// of the clocks.
type tFaults = (restarts: int, losses: int, answersOnly: bool, down: set[string], recoverBlackout: bool, operator: bool,
                skew: bool);

fun NoFaults(): tFaults {
  return (restarts = 0, losses = 0, answersOnly = false, down = default(set[string]), recoverBlackout = false, operator = false,
          skew = false);
}

// Root A holds hash key a, and root B holds hash key b. One coordinator, the Environment, which is
// also the clock, and an Operator when the faults ask for one. Each client runs its own list of calls.
fun StartSystem(bugs: tBugs, faults: tFaults, scripts: seq[seq[tClientOp]]) {
  var topology: tTopology;
  var coordinator: machine;
  var env: machine;
  var operator: machine;
  var script: seq[tClientOp];
  var dos: seq[machine];
  var partitions: seq[machine];
  var down: set[machine];
  var hk: string;
  var offsetMoves: int;
  env = new Environment();
  if (faults.operator) {
    operator = new Operator();
  }
  topology["a"] = new Partition((bugs = bugs, env = env, clock = env, operator = operator));
  topology["b"] = new Partition((bugs = bugs, env = env, clock = env, operator = operator));
  coordinator = new Coordinator((bugs = bugs, topology = topology, env = env, clock = env, recoverBlackout = faults.recoverBlackout));
  foreach (hk in keys(topology)) {
    partitions += (sizeof(partitions), topology[hk]);
    if (hk in faults.down) {
      down += (topology[hk]);
    }
  }
  dos = partitions;
  dos += (sizeof(dos), coordinator);
  if (faults.skew) {
    offsetMoves = OFFSET_MOVES();
  }
  send env, eEnvSetup, (config = (restarts = faults.restarts, losses = faults.losses, answersOnly = faults.answersOnly, down = down,
                                  fineSteps = FINE_STEPS(), offsetMoves = offsetMoves),
                        dos = dos);
  if (faults.operator) {
    send operator, eOperatorSetup, (coordinator = coordinator, partitions = partitions, env = env);
  }
  foreach (script in scripts) {
    new Client((topology = topology, coordinator = coordinator, env = env, ops = script));
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
  StartSystem(bugs, NoFaults(), scripts);
}

// Client 1: T1 puts a1 and b1, with `retries` retries and with or without a replay. Client 2: T2 puts
// b2.
fun RunWriteHappy(bugs: tBugs, faults: tFaults, retries: int, replay: bool) {
  var t1: seq[tTxOp];
  var t2: seq[tTxOp];
  var op: tClientOp;
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  t1 += (0, Put(A1(), NO_CONDITION, 11));
  t1 += (1, Put(B1(), NO_CONDITION, 12));
  t2 += (0, Put(B2(), NO_CONDITION, 21));
  op = TxOp(1, t1);
  op.retries = retries;
  op.replay = replay;
  c1 += (0, op);
  c2 += (0, TxOp(2, t2));
  scripts += (0, c1);
  scripts += (1, c2);
  StartSystem(bugs, faults, scripts);
}

// Client 1: T1 puts a1 and b1, then a get of b1. Client 2: T2 puts a1 with not_exists and b2, then
// a put of a1.
fun RunWriteConflict(bugs: tBugs, faults: tFaults) {
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
  StartSystem(bugs, faults, scripts);
}

// Client 1: T1 puts a1 and b1, with two retries. Client 2: T2 puts a1.
fun RunConcurrentDrives(bugs: tBugs) {
  var t1: seq[tTxOp];
  var t2: seq[tTxOp];
  var op: tClientOp;
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  var faults: tFaults;
  t1 += (0, Put(A1(), NO_CONDITION, 11));
  t1 += (1, Put(B1(), NO_CONDITION, 12));
  t2 += (0, Put(A1(), NO_CONDITION, 21));
  op = TxOp(1, t1);
  op.retries = 2;
  c1 += (0, op);
  c2 += (0, TxOp(2, t2));
  scripts += (0, c1);
  scripts += (1, c2);
  faults = NoFaults();
  faults.restarts = 1;
  faults.losses = 1;
  faults.answersOnly = true;
  StartSystem(bugs, faults, scripts);
}

// One client: T1 puts a1 and b1.
fun RunSingle(bugs: tBugs, faults: tFaults) {
  var t1: seq[tTxOp];
  var c1: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  t1 += (0, Put(A1(), NO_CONDITION, 11));
  t1 += (1, Put(B1(), NO_CONDITION, 12));
  c1 += (0, TxOp(1, t1));
  scripts += (0, c1);
  StartSystem(bugs, faults, scripts);
}

// Client 1: T1 checks that a1 exists and puts b1, T3 deletes b1, then a put of b1. Client 2: T2
// puts a1, then T4 puts b1.
fun RunClocks(bugs: tBugs) {
  var t: seq[tTxOp];
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  t += (0, Check(A1(), EXISTS));
  t += (1, Put(B1(), NO_CONDITION, 12));
  c1 += (0, TxOp(1, t));
  t = default(seq[tTxOp]);
  t += (0, Delete(B1()));
  c1 += (1, TxOp(3, t));
  c1 += (2, ItemOp(PUT_ITEM, B1(), 31));
  t = default(seq[tTxOp]);
  t += (0, Put(A1(), NO_CONDITION, 21));
  c2 += (0, TxOp(2, t));
  t = default(seq[tTxOp]);
  t += (0, Put(B1(), NO_CONDITION, 41));
  c2 += (1, TxOp(4, t));
  scripts += (0, c1);
  scripts += (1, c2);
  StartSystem(bugs, FaultsSkew(), scripts);
}

// Client 1: a single-partition transaction with no token puts b1 and b2. Client 2: T1 puts a1 and
// b1.
fun RunSingleShot(bugs: tBugs) {
  var s: seq[tTxOp];
  var t1: seq[tTxOp];
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  s += (0, Put(B1(), NO_CONDITION, 51));
  s += (1, Put(B2(), NO_CONDITION, 52));
  t1 += (0, Put(A1(), NO_CONDITION, 11));
  t1 += (1, Put(B1(), NO_CONDITION, 12));
  c1 += (0, TxOp(0, s));
  c2 += (0, TxOp(1, t1));
  scripts += (0, c1);
  scripts += (1, c2);
  StartSystem(bugs, FaultsSkew(), scripts);
}

fun FaultsSkew(): tFaults {
  var f: tFaults;
  f = NoFaults();
  f.skew = true;
  return f;
}

fun FaultsWrite(): tFaults {
  var f: tFaults;
  f = NoFaults();
  f.restarts = 2;
  f.losses = 2;
  return f;
}

fun FaultsRetry(): tFaults {
  var f: tFaults;
  f = NoFaults();
  f.losses = 2;
  f.answersOnly = true;
  return f;
}

// The coordinator drops recoverTransactionForParticipant until it swept the transaction, and two
// calls get lost. A lock stays after its transaction completed only when a prepare reaches a
// partition after the cancel, so these faults run with the two conflicting clients of
// RunWriteConflict.
fun FaultsStale(operator: bool): tFaults {
  var f: tFaults;
  f = NoFaults();
  f.losses = 2;
  f.recoverBlackout = true;
  f.operator = operator;
  return f;
}

fun FaultsHold(): tFaults {
  var f: tFaults;
  f = NoFaults();
  f.restarts = 1;
  f.down += ("b");
  return f;
}

machine TestItems { start state Init { entry { RunItems(NoBugs()); } } }
machine TestWriteHappy { start state Init { entry { RunWriteHappy(NoBugs(), NoFaults(), 0, false); } } }
machine TestWriteConflict { start state Init { entry { RunWriteConflict(NoBugs(), NoFaults()); } } }
machine TestWriteFaults { start state Init { entry { RunWriteConflict(NoBugs(), FaultsWrite()); } } }
machine TestWriteRetry { start state Init { entry { RunWriteHappy(NoBugs(), FaultsRetry(), 2, true); } } }
machine TestConcurrentDrives { start state Init { entry { RunConcurrentDrives(NoBugs()); } } }
machine TestStale { start state Init { entry { RunWriteConflict(NoBugs(), FaultsStale(false)); } } }
machine TestRepair { start state Init { entry { RunWriteConflict(NoBugs(), FaultsStale(true)); } } }
machine TestHold { start state Init { entry { RunSingle(NoBugs(), FaultsHold()); } } }
machine TestClocks { start state Init { entry { RunClocks(NoBugs()); } } }
machine TestSingleShot { start state Init { entry { RunSingleShot(NoBugs()); } } }

machine TestBugNewRowVersionFromOne {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.newRowVersionFromOne = true; RunItems(b); } }
}
machine TestBugCommitOnOneAccept {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.commitOnOneAccept = true; RunWriteConflict(b, NoFaults()); } }
}
machine TestBugPutIgnoresLock {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.putIgnoresLock = true; RunWriteConflict(b, NoFaults()); } }
}
machine TestBugCommittedBeforeApply {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.committedBeforeApply = true; RunWriteConflict(b, NoFaults()); } }
}
machine TestBugTokenRowIgnored {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.tokenRowIgnored = true; RunWriteHappy(b, FaultsRetry(), 2, true); } }
}
machine TestBugStaleCancelsOnDriving {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.staleCancelsOnDriving = true; RunWriteConflict(b, FaultsWrite()); } }
}
machine TestBugCancelInAnyState {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.cancelInAnyState = true; RunConcurrentDrives(b); } }
}
machine TestBugNoPreparingHold {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.noPreparingHold = true; RunSingle(b, FaultsHold()); } }
}
machine TestBugPrepareSkipsWatermark {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.prepareSkipsWatermark = true; RunClocks(b); } }
}
machine TestBugSingleShotIgnoresLock {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.singleShotIgnoresLock = true; RunSingleShot(b); } }
}
machine TestBugNewRowTsFromOwnStamp {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.newRowTsFromOwnStamp = true; RunClocks(b); } }
}
