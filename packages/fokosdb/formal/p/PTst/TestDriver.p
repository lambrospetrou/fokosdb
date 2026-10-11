fun A1(): tKey { return (hk = "a", sk = "1"); }
fun B1(): tKey { return (hk = "b", sk = "1"); }
fun B2(): tKey { return (hk = "b", sk = "2"); }
fun C1(): tKey { return (hk = "c", sk = "1"); }

fun ItemOp(kind: tClientOpKind, key: tKey, value: tValue): tClientOp {
  return (kind = kind, key = key, value = value, token = 0, items = default(seq[tTxOp]), retries = 0, replay = false,
          readKeys = default(seq[tKey]), generatedToken = 0);
}

fun TxOp(token: int, items: seq[tTxOp]): tClientOp {
  return (kind = TRANSACT_WRITE, key = default(tKey), value = 0, token = token, items = items, retries = 0, replay = false,
          readKeys = default(seq[tKey]), generatedToken = 0);
}

fun ReadOp(readKeys: seq[tKey]): tClientOp {
  return (kind = TRANSACT_GET, key = default(tKey), value = 0, token = 0, items = default(seq[tTxOp]), retries = 0,
          replay = false, readKeys = readKeys, generatedToken = 0);
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
// of the clocks. `maxTicksAfterFaults` is the number of ticks after the end of the faults and after
// the last change of an alarm (TICKS_AFTER_FAULTS in Environment.p). `split` is the hash split of
// root A.
type tFaults = (restarts: int, losses: int, answersOnly: bool, down: set[string], recoverBlackout: bool, operator: bool,
                skew: bool, maxTicksAfterFaults: int, split: bool);

fun NoFaults(): tFaults {
  return (restarts = 0, losses = 0, answersOnly = false, down = default(set[string]), recoverBlackout = false, operator = false,
          skew = false, maxTicksAfterFaults = TICKS_AFTER_FAULTS(), split = false);
}

// Root A holds hash keys a and c, and root B holds hash key b. A hash split of root A moves hash key
// a to its child 0 and hash key c to its child 1. One coordinator, the Environment, which is also the
// clock, and an Operator when the faults ask for one. Each client runs its own list of calls.
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
  var split: machine;
  env = new Environment();
  if (faults.operator) {
    operator = new Operator();
  }
  topology["a"] = new Partition((bugs = bugs, env = env, clock = env, operator = operator, now = 0, tsClock = default(tClock)));
  topology["b"] = new Partition((bugs = bugs, env = env, clock = env, operator = operator, now = 0, tsClock = default(tClock)));
  partitions += (0, topology["a"]);
  partitions += (1, topology["b"]);
  topology["c"] = topology["a"];
  coordinator = new Coordinator((bugs = bugs, topology = topology, env = env, clock = env, recoverBlackout = faults.recoverBlackout));
  foreach (hk in faults.down) {
    down += (topology[hk]);
  }
  if (faults.split) {
    split = topology["a"];
  }
  dos = partitions;
  dos += (sizeof(dos), coordinator);
  if (faults.skew) {
    offsetMoves = OFFSET_MOVES();
  }
  send env, eEnvSetup, (config = (restarts = faults.restarts, losses = faults.losses, answersOnly = faults.answersOnly, down = down,
                                  fineSteps = FINE_STEPS(), offsetMoves = offsetMoves, maxTicksAfterFaults = faults.maxTicksAfterFaults,
                                  split = split),
                        dos = dos);
  if (faults.operator) {
    send operator, eOperatorSetup, (coordinator = coordinator, partitions = partitions, env = env);
  }
  foreach (script in scripts) {
    new Client((bugs = bugs, topology = topology, coordinator = coordinator, env = env, ops = script));
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

// Client 1: T1 checks that a1 does not exist and puts b1. Client 2: T2 puts a1. When T1 commits
// first, T2 can still prepare a1 later at a timestamp at or below the one of T1.
fun RunAbsentCheck(bugs: tBugs, faults: tFaults) {
  var t1: seq[tTxOp];
  var t2: seq[tTxOp];
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  t1 += (0, Check(A1(), NOT_EXISTS));
  t1 += (1, Put(B1(), NO_CONDITION, 12));
  t2 += (0, Put(A1(), NO_CONDITION, 21));
  c1 += (0, TxOp(1, t1));
  c2 += (0, TxOp(2, t2));
  scripts += (0, c1);
  scripts += (1, c2);
  StartSystem(bugs, faults, scripts);
}

// One client: a put of a1, then T1 checks that a1 exists and puts b1. A lost answer of the prepare
// to A lets the first prepare arrive after T1 committed, and lock a1 again. The coordinator drops
// recoverTransactionForParticipant until it swept T1, so the stale job then gets not_found for a
// lock older than the idempotency window.
fun RunLateCheck(bugs: tBugs) {
  var t1: seq[tTxOp];
  var c1: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  var f: tFaults;
  t1 += (0, Check(A1(), EXISTS));
  t1 += (1, Put(B1(), NO_CONDITION, 12));
  c1 += (0, ItemOp(PUT_ITEM, A1(), 5));
  c1 += (1, TxOp(1, t1));
  scripts += (0, c1);
  f = FaultsStale(false);
  f.answersOnly = true;
  StartSystem(bugs, f, scripts);
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

// One client: T1 puts a1 and b1, then R1 reads a1 and b1 on the two-phase path.
fun RunReadHappy(bugs: tBugs) {
  var t1: seq[tTxOp];
  var r1: seq[tKey];
  var c1: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  t1 += (0, Put(A1(), NO_CONDITION, 11));
  t1 += (1, Put(B1(), NO_CONDITION, 12));
  r1 += (0, A1());
  r1 += (1, B1());
  c1 += (0, TxOp(1, t1));
  c1 += (1, ReadOp(r1));
  scripts += (0, c1);
  StartSystem(bugs, NoFaults(), scripts);
}

// Client 1: W1 creates a1 and puts b1. Client 2: W2 deletes a1 and puts b2. Client 3: R1 reads a1,
// b1, and b2 on the two-phase path.
fun RunReadVsWrites(bugs: tBugs, faults: tFaults) {
  var w: seq[tTxOp];
  var r1: seq[tKey];
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var c3: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  w += (0, Put(A1(), NO_CONDITION, 11));
  w += (1, Put(B1(), NO_CONDITION, 12));
  c1 += (0, TxOp(1, w));
  w = default(seq[tTxOp]);
  w += (0, Delete(A1()));
  w += (1, Put(B2(), NO_CONDITION, 22));
  c2 += (0, TxOp(2, w));
  r1 += (0, A1());
  r1 += (1, B1());
  r1 += (2, B2());
  c3 += (0, ReadOp(r1));
  scripts += (0, c1);
  scripts += (1, c2);
  scripts += (2, c3);
  StartSystem(bugs, faults, scripts);
}

// The writers and the read of RunReadVsWrites, for the order of Appendix A of the RFC. That order
// needs two long delays: phase 1 on B waits for all of W1, and phase 2 on A waits for the prepare of
// W2 and its commit on A. Client 1 runs W1 and then W2, which drops the orders with W2 first, and
// clients 2 to 4 each run R1, which gives three chances in one run.
fun RunDeleteBetweenPhases(bugs: tBugs) {
  var w: seq[tTxOp];
  var r1: seq[tKey];
  var c1: seq[tClientOp];
  var reader: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  w += (0, Put(A1(), NO_CONDITION, 11));
  w += (1, Put(B1(), NO_CONDITION, 12));
  c1 += (0, TxOp(1, w));
  w = default(seq[tTxOp]);
  w += (0, Delete(A1()));
  w += (1, Put(B2(), NO_CONDITION, 22));
  c1 += (1, TxOp(2, w));
  r1 += (0, A1());
  r1 += (1, B1());
  r1 += (2, B2());
  reader += (0, ReadOp(r1));
  scripts += (0, c1);
  scripts += (1, reader);
  scripts += (2, reader);
  scripts += (3, reader);
  StartSystem(bugs, NoFaults(), scripts);
}

// Client 1: a single-item put W1 creates a1, then R1 reads a1 and b2 on the two-phase path. Client
// 2: W2 deletes a1 and puts b2, then a single-item put W3 creates a1 again. A defect that gives the
// new row of W3 the v of the row of W1 hides the delete and the recreate from the comparison of the
// two phases.
fun RunReusedVersionRead(bugs: tBugs) {
  var w: seq[tTxOp];
  var r1: seq[tKey];
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  r1 += (0, A1());
  r1 += (1, B2());
  c1 += (0, ItemOp(PUT_ITEM, A1(), 11));
  c1 += (1, ReadOp(r1));
  w += (0, Delete(A1()));
  w += (1, Put(B2(), NO_CONDITION, 22));
  c2 += (0, TxOp(2, w));
  c2 += (1, ItemOp(PUT_ITEM, A1(), 31));
  scripts += (0, c1);
  scripts += (1, c2);
  StartSystem(bugs, NoFaults(), scripts);
}

// Client 1: R1 reads b1 and b2 on the snapshot path. Client 2: T1 puts b1 and b2. Client 3: T2 puts
// a1 and b1.
fun RunSnapshot(bugs: tBugs) {
  var t: seq[tTxOp];
  var r1: seq[tKey];
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var c3: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  r1 += (0, B1());
  r1 += (1, B2());
  c1 += (0, ReadOp(r1));
  t += (0, Put(B1(), NO_CONDITION, 11));
  t += (1, Put(B2(), NO_CONDITION, 12));
  c2 += (0, TxOp(1, t));
  t = default(seq[tTxOp]);
  t += (0, Put(A1(), NO_CONDITION, 21));
  t += (1, Put(B1(), NO_CONDITION, 22));
  c3 += (0, TxOp(2, t));
  scripts += (0, c1);
  scripts += (1, c2);
  scripts += (2, c3);
  StartSystem(bugs, NoFaults(), scripts);
}

// Client 1: T1 puts a1 and b1, T3 puts a1, then a get of a1. Client 2: T2 deletes a1 and puts c1.
// After the split, a1 is on child 0 and c1 on child 1, so T2 has a part on each child.
fun RunSplitWrites(bugs: tBugs, faults: tFaults) {
  var t: seq[tTxOp];
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  t += (0, Put(A1(), NO_CONDITION, 11));
  t += (1, Put(B1(), NO_CONDITION, 12));
  c1 += (0, TxOp(1, t));
  t = default(seq[tTxOp]);
  t += (0, Put(A1(), NO_CONDITION, 31));
  c1 += (1, TxOp(3, t));
  c1 += (2, ItemOp(GET_ITEM, A1(), 0));
  t = default(seq[tTxOp]);
  t += (0, Delete(A1()));
  t += (1, Put(C1(), NO_CONDITION, 22));
  c2 += (0, TxOp(2, t));
  scripts += (0, c1);
  scripts += (1, c2);
  StartSystem(bugs, faults, scripts);
}

// Client 1: W1 creates a1 and puts c1. Client 2: W2 deletes a1 and puts b2. Client 3: R1 reads a1,
// c1, and b2 on the two-phase path. After the split, W1 and the read of root A have a part on each
// child.
fun RunSplitReads(bugs: tBugs) {
  var w: seq[tTxOp];
  var r1: seq[tKey];
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var c3: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  w += (0, Put(A1(), NO_CONDITION, 11));
  w += (1, Put(C1(), NO_CONDITION, 12));
  c1 += (0, TxOp(1, w));
  w = default(seq[tTxOp]);
  w += (0, Delete(A1()));
  w += (1, Put(B2(), NO_CONDITION, 22));
  c2 += (0, TxOp(2, w));
  r1 += (0, A1());
  r1 += (1, C1());
  r1 += (2, B2());
  c3 += (0, ReadOp(r1));
  scripts += (0, c1);
  scripts += (1, c2);
  scripts += (2, c3);
  StartSystem(bugs, FaultsSplit(), scripts);
}

// The single-partition paths on root A. Client 1: a transaction with no token puts a1 and c1, then
// R1 reads a1 and c1. Client 2: a transaction with no token puts a1, then R2 reads a1. Before the
// split, root A owns every key. After it, a router forwards the calls of client 2 to child 0, and
// answers not_applicable to the calls of client 1.
fun RunSplitSingleOwner(bugs: tBugs) {
  var t: seq[tTxOp];
  var r: seq[tKey];
  var op: tClientOp;
  var c1: seq[tClientOp];
  var c2: seq[tClientOp];
  var scripts: seq[seq[tClientOp]];
  t += (0, Put(A1(), NO_CONDITION, 51));
  t += (1, Put(C1(), NO_CONDITION, 52));
  op = TxOp(0, t);
  op.generatedToken = 1;
  c1 += (0, op);
  r += (0, A1());
  r += (1, C1());
  c1 += (1, ReadOp(r));
  t = default(seq[tTxOp]);
  t += (0, Put(A1(), NO_CONDITION, 61));
  c2 += (0, TxOp(0, t));
  r = default(seq[tKey]);
  r += (0, A1());
  c2 += (1, ReadOp(r));
  scripts += (0, c1);
  scripts += (1, c2);
  StartSystem(bugs, FaultsSplit(), scripts);
}

fun FaultsSplit(): tFaults {
  var f: tFaults;
  f = NoFaults();
  f.split = true;
  return f;
}

fun FaultsSplitWrite(): tFaults {
  var f: tFaults;
  f = FaultsWrite();
  f.split = true;
  return f;
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
  // Root b never answers, so the coordinator keeps a deadline for good, and each tick only repeats
  // its cancel attempts to b. The locks of root a resolve within 3 ticks, and 6 keeps a margin.
  f.maxTicksAfterFaults = 6;
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
machine TestAbsentCheck { start state Init { entry { RunAbsentCheck(NoBugs(), FaultsRetry()); } } }
machine TestLateCheck { start state Init { entry { RunLateCheck(NoBugs()); } } }
machine TestReadHappy { start state Init { entry { RunReadHappy(NoBugs()); } } }
machine TestReadVsWrites { start state Init { entry { RunReadVsWrites(NoBugs(), NoFaults()); } } }
machine TestReadFaults { start state Init { entry { RunReadVsWrites(NoBugs(), FaultsWrite()); } } }
machine TestSnapshot { start state Init { entry { RunSnapshot(NoBugs()); } } }
machine TestSplitWrites { start state Init { entry { RunSplitWrites(NoBugs(), FaultsSplit()); } } }
machine TestSplitReads { start state Init { entry { RunSplitReads(NoBugs()); } } }
machine TestSplitFaults { start state Init { entry { RunSplitWrites(NoBugs(), FaultsSplitWrite()); } } }
machine TestSplitSingleOwner { start state Init { entry { RunSplitSingleOwner(NoBugs()); } } }

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
machine TestBugQuarantineStampedChecks {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.quarantineStampedChecks = true; RunLateCheck(b); } }
}
// The defect needs no fault: a prepare of T2 after T1 committed is enough.
machine TestBugAbsentCheckKeepsNoStamp {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.absentCheckKeepsNoStamp = true; RunAbsentCheck(b, NoFaults()); } }
}
machine TestBugReadSkipsPhaseTwo {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.readSkipsPhaseTwo = true; RunReadVsWrites(b, NoFaults()); } }
}
machine TestBugReadIgnoresPendingWrite {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.readIgnoresPendingWrite = true; RunReadVsWrites(b, NoFaults()); } }
}
machine TestBugReadIgnoresMaxDeletedV {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.readIgnoresMaxDeletedV = true; RunDeleteBetweenPhases(b); } }
}
// The defect newRowVersionFromOne in a read transaction. W3 creates a1 again after the delete of W2,
// and with the defect the new row has the v of the row of W1. Its name must not start with
// tcBugNewRowVersionFromOne, because `p check -tc` runs every test case whose name starts with the
// given name.
machine TestBugReusedVersionRead {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.newRowVersionFromOne = true; RunReusedVersionRead(b); } }
}
machine TestBugChildSkipsDeletionMetadata {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.childSkipsDeletionMetadata = true; RunSplitWrites(b, FaultsSplit()); } }
}
machine TestBugImportingChildServesPrepare {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.importingChildServesPrepare = true; RunSplitWrites(b, FaultsSplit()); } }
}
machine TestBugRouterServesGetItem {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.routerServesGetItem = true; RunSplitWrites(b, FaultsSplit()); } }
}
machine TestBugImportSkipsPendingTx {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.importSkipsPendingTx = true; RunSplitWrites(b, FaultsSplit()); } }
}
machine TestBugRouterDeletesCopiesAtCutover {
  start state Init { entry { var b: tBugs; b = NoBugs(); b.routerDeletesCopiesAtCutover = true; RunSplitWrites(b, FaultsSplit()); } }
}

// A restart of a partition that waits for a forward. This machine is the client, the Environment,
// and the clock, so it decides when each call continues. It holds the first pull of each child, sends
// a get of a1 to child 0, and restarts that child while its read-through call to the source waits.
// The get must fail before the source call continues. Then the pulls continue and the split ends.
// After that, the machine does the same on the router for a put, a delete, a get, a single-shot
// write, and a snapshot read of a1: it restarts the router while the call to the child waits. One
// more get loses its request to the child after the restart. No answer and no second error of these
// calls can reach the caller. The child still applies each write, so a last get reads the value of
// the single-shot write.
enum tForwardRestartPhase { HOLD_IMPORTS, RESTART_READ_THROUGH, WAIT_SPLIT, RESTART_FORWARD, VERIFY_WRITE, RESTART_DONE }

machine TestForwardRestart {
  var root: machine;
  var children: seq[machine];
  var heldImports: set[machine];
  var ready: set[machine];
  var phase: tForwardRestartPhase;
  var victim: machine;
  var held: machine;
  var failed: bool;
  var openCalls: int;
  var next: int;

  start state Init {
    entry {
      root = new Partition((bugs = NoBugs(), env = this, clock = this, operator = null, now = 0, tsClock = default(tClock)));
      send root, eRequestSplit;
      goto Running;
    }
  }

  state Running {
    on eDoCreated do (child: machine) { children += (sizeof(children), child); }
    on eRpcOpened do { openCalls = openCalls + 1; }
    on eRpcClosed do {
      openCalls = openCalls - 1;
      assert openCalls >= 0, "a call closed more than once";
      Progress();
    }
    on eArmed do (a: tArmed) {
      if (a.armed) {
        ready -= (a.who);
      } else {
        ready += (a.who);
      }
      Progress();
    }
    on eMayLose do (q: tMayLose) {
      if (phase == HOLD_IMPORTS && q.target == root) {
        heldImports += (q.rpc);
        if (sizeof(heldImports) == HASH_SPLIT_N()) {
          phase = RESTART_READ_THROUGH;
          victim = children[0];
          announce eCallStarted, (client = this, reqId = 0);
          send NewRpc(this, victim, this), eGetItem, (caller = this, reqId = 0, key = A1());
        }
      } else if ((phase == RESTART_READ_THROUGH && q.target == root)
                 || (phase == RESTART_FORWARD && q.target in children)) {
        assert held == null, "the request forwarded more than once";
        held = q.rpc;
        send victim, eRestart;
      } else {
        send q.rpc, eLossDecision, LOSE_NONE;
      }
    }
    on eRpcFailed do (f: tRpcFailed) {
      assert (phase == RESTART_READ_THROUGH || phase == RESTART_FORWARD) && !failed && held != null,
        "a request failed outside the restart";
      assert f.reqId == 0 && f.target == victim, "the restart failed the wrong request";
      failed = true;
      announce eCallEnded, (client = this, reqId = 0);
      if (phase == RESTART_FORWARD && next == 5) {
        send held, eLossDecision, LOSE_REQUEST;
      } else {
        send held, eLossDecision, LOSE_NONE;
      }
    }
    on eGetItemResp do (r: tGetItemResp) {
      assert phase == VERIFY_WRITE, "a get returned an answer before the last get";
      assert r.reqId == 0 && r.found && r.value == 51, "the last get did not read the single-shot write";
      announce eCallEnded, (client = this, reqId = 0);
      phase = RESTART_DONE;
    }
    on ePutItemResp, eDeleteItemResp, eExecuteSingleShotResp, eReadSnapshotResp do {
      assert false, "a restarted request returned a late answer";
    }
  }

  fun Progress() {
    var rpc: machine;
    if (phase == RESTART_READ_THROUGH && failed && openCalls == sizeof(heldImports)) {
      phase = WAIT_SPLIT;
      foreach (rpc in heldImports) {
        send rpc, eLossDecision, LOSE_NONE;
      }
      heldImports = default(set[machine]);
    } else if (phase == WAIT_SPLIT && openCalls == 0 && sizeof(ready) == HASH_SPLIT_N() + 1) {
      phase = RESTART_FORWARD;
      StartRequest();
    } else if (phase == RESTART_FORWARD && failed && openCalls == 0) {
      next = next + 1;
      if (next == 6) {
        phase = VERIFY_WRITE;
        announce eCallStarted, (client = this, reqId = 0);
        send NewRpc(this, root, this), eGetItem, (caller = this, reqId = 0, key = A1());
      } else {
        StartRequest();
      }
    }
  }

  fun StartRequest() {
    var rpc: machine;
    var items: seq[tTxOp];
    var readKeys: seq[tKey];
    victim = root;
    held = default(machine);
    failed = false;
    announce eCallStarted, (client = this, reqId = 0);
    rpc = NewRpc(this, root, this);
    if (next == 0) {
      send rpc, ePutItem, (caller = this, reqId = 0, key = A1(), value = 11);
    } else if (next == 1) {
      send rpc, eDeleteItem, (caller = this, reqId = 0, key = A1());
    } else if (next == 2) {
      send rpc, eGetItem, (caller = this, reqId = 0, key = A1());
    } else if (next == 3) {
      items += (0, Put(A1(), NO_CONDITION, 51));
      send rpc, eExecuteSingleShot, (caller = this, reqId = 0, items = items);
    } else if (next == 4) {
      readKeys += (0, A1());
      send rpc, eReadSnapshot, (caller = this, reqId = 0, readKeys = readKeys);
    } else {
      send rpc, eGetItem, (caller = this, reqId = 0, key = A1());
    }
  }
}
