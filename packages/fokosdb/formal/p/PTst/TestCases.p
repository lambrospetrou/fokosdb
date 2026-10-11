module FokosDB = { Client, Coordinator, Partition, RpcCall, Environment, Operator };

// A test case named tcBug<Defect> must fail with the monitor of its seeded defect. Every other test
// case must pass. Each test case checks every monitor, except where a comment says otherwise.

test tcItems [main = TestItems]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestItems });
test tcBugNewRowVersionFromOne [main = TestBugNewRowVersionFromOne]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestBugNewRowVersionFromOne });

test tcWriteHappy [main = TestWriteHappy]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestWriteHappy });
test tcWriteConflict [main = TestWriteConflict]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestWriteConflict });
test tcBugCommitOnOneAccept [main = TestBugCommitOnOneAccept]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestBugCommitOnOneAccept });
test tcBugPutIgnoresLock [main = TestBugPutIgnoresLock]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestBugPutIgnoresLock });
// The defect answers committed in COMMITTING, so AnswerMatchesDecision fails at the answer. The test case
// leaves that monitor out, so that it shows that ReadAfterCommit finds the stale read.
test tcBugCommittedBeforeApply [main = TestBugCommittedBeforeApply]:
  assert VersionIncreases, Atomicity, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestBugCommittedBeforeApply });

test tcWriteFaults [main = TestWriteFaults]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestWriteFaults });
test tcWriteRetry [main = TestWriteRetry]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestWriteRetry });
test tcConcurrentDrives [main = TestConcurrentDrives]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestConcurrentDrives });
test tcStale [main = TestStale]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestStale });
test tcRepair [main = TestRepair]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestRepair });
// A cancel to a partition that never answers keeps the transaction in CANCELLING, so the test case
// does not check TransactionsComplete.
test tcHold [main = TestHold]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered in (union FokosDB, { TestHold });
test tcBugTokenRowIgnored [main = TestBugTokenRowIgnored]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestBugTokenRowIgnored });
test tcBugStaleCancelsOnDriving [main = TestBugStaleCancelsOnDriving]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestBugStaleCancelsOnDriving });
test tcBugCancelInAnyState [main = TestBugCancelInAnyState]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestBugCancelInAnyState });
test tcBugNoPreparingHold [main = TestBugNoPreparingHold]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered in (union FokosDB, { TestBugNoPreparingHold });

test tcClocks [main = TestClocks]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestClocks });
test tcSingleShot [main = TestSingleShot]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestSingleShot });
test tcBugPrepareSkipsWatermark [main = TestBugPrepareSkipsWatermark]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestBugPrepareSkipsWatermark });
test tcBugSingleShotIgnoresLock [main = TestBugSingleShotIgnoresLock]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestBugSingleShotIgnoresLock });
test tcBugNewRowTsFromOwnStamp [main = TestBugNewRowTsFromOwnStamp]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit, SingleApply, WriteSerializable,
    LocksResolve, ClientAnswered, TransactionsComplete in (union FokosDB, { TestBugNewRowTsFromOwnStamp });
