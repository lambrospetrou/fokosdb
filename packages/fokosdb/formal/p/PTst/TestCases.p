module FokosDB = { Client, Coordinator, Partition, RpcCall };

// A test case named tcBug<Id> must fail with the monitor of its seeded defect. Every other test
// case must pass.
test tcItems [main = TestItems]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit in (union FokosDB, { TestItems });
test tcBugV1 [main = TestBugV1]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit in (union FokosDB, { TestBugV1 });

test tcWriteHappy [main = TestWriteHappy]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit in (union FokosDB, { TestWriteHappy });
test tcWriteConflict [main = TestWriteConflict]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit in (union FokosDB, { TestWriteConflict });
test tcBugW1 [main = TestBugW1]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit in (union FokosDB, { TestBugW1 });
test tcBugW2 [main = TestBugW2]:
  assert VersionIncreases, Atomicity, AnswerMatchesDecision, LockExclusion, ReadAfterCommit in (union FokosDB, { TestBugW2 });
// W3 answers committed in COMMITTING, so AnswerMatchesDecision fails at the answer. The test case
// leaves that monitor out, so that it shows that ReadAfterCommit finds the stale read.
test tcBugW3 [main = TestBugW3]:
  assert VersionIncreases, Atomicity, LockExclusion, ReadAfterCommit in (union FokosDB, { TestBugW3 });
