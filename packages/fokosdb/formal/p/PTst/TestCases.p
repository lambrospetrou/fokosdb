module FokosDB = { Client, Partition };

// A test case named tcBug<Id> must fail with the monitor of its seeded defect. Every other test
// case must pass.
test tcItems [main = TestItems]: assert VersionIncreases in (union FokosDB, { TestItems });
test tcBugV1 [main = TestBugV1]: assert VersionIncreases in (union FokosDB, { TestBugV1 });
