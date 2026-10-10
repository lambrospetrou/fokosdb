-------------------------- MODULE ReadTtlRecreate ---------------------------
EXTENDS Integers, Sequences, TLC

\* Why this model exists
\* A transaction read must return values from one consistent state.
\* At commit 1b5aace, a TTL sweep deleted key a without increasing delete_revision.
\* A later insert could give a the same version as its old row.
\* The reader compared presence, version, and delete_revision. It did not compare the payload.
\* This model tests whether that check can accept old a with new b.
\*
\* What this model includes
\* Keys a and b live in different partitions. Both start with payload 0.
\* The writer gives each key payload 1. The value -1 means that a row is absent.
\* The Clock makes a's TTL due. The Sweeper then deletes a.
\* The Writer prepares both keys and commits a before b.
\* The Reader samples both keys twice. It can visit the keys in either order.
\* The history records physical states, including the state between the two local commits.
\*
\* Version and limits
\* The model selects TTL, write, and read rules from commit 1b5aace.
\* Commit edcbb56 changes the deletion metadata and version rules.
\* This model does not describe that later code or every possible read schedule.
\* The model has one sweep, one write transaction, and one read transaction.
\* It omits the timer scheduler, network errors, migration, and other transactions.
\* TLC finds allowed step orders. It does not measure their frequency in the runtime.
\*
\* Code paths for the selected rules
\* partition-store.ts: deleteExpiredItems and upsertItem.
\* transaction-participant.ts: prepareLocal and readForTransactionLocal.
\* client/db.ts: #readTransaction.

\* Each key belongs to a different partition.
Keys == {"a", "b"}
\* Both initial payloads have value 0.
Old == [k \in Keys |-> 0]
\* The modeled read compares presence, version, and delete revision.
\* It does not compare the payload.
Stamp(s) == <<s.found, IF s.found THEN s.version ELSE 0, s.revision>>
\* A sample holds the payload, presence, version, delete revision, and pending-write flag.
\* The reader replaces this default for each key before it validates the result.
EmptySample == [value |-> 0, found |-> TRUE, version |-> 1, revision |-> 0, pending |-> FALSE]

(* --algorithm ReadTtlRecreate
variables
  \* now is an abstract clock. Tick moves it to the TTL time.
  now = 0,
  \* expired says whether the sweep deleted a.
  expired = FALSE,
  \* maxDeleteTs is the timestamp watermark for the absent key a.
  maxDeleteTs = 0,
  \* lastReadTsB is the read timestamp watermark for the live key b.
  lastReadTsB = 0,
  \* items maps each key to its payload. The value -1 means no row exists.
  items = Old,
  \* found says whether each partition holds its key's row.
  found = [k \in Keys |-> TRUE],
  \* versions stores each row's version. The sweep clears a's version.
  versions = [k \in Keys |-> 1],
  \* revisions stores each partition's delete_revision. The sweep leaves it unchanged.
  revisions = [k \in Keys |-> 0],
  \* locked contains the keys that the writer prepared but has not committed.
  locked = {},
  \* first holds the reader's first sample for each key.
  first = [k \in Keys |-> EmptySample],
  \* second holds the reader's second sample for each key.
  second = [k \in Keys |-> EmptySample],
  \* remaining contains the keys that the reader has not sampled in this pass.
  remaining = Keys,
  \* accepted becomes TRUE if the two passes pass the read check.
  accepted = FALSE,
  \* history lists pairs of stored payloads. Only the invariant reads this ghost variable.
  history = <<Old>>;

\* The Clock makes the TTL due. The row stays present until the Sweeper runs.
process Clock = "clock"
begin
Tick:
  now := now + 1;
end process;

\* The Sweeper deletes a when its TTL is due and a has no lock.
\* It changes the timestamp watermark but leaves delete_revision unchanged.
process Sweeper = "sweeper"
begin
Sweep:
  await now >= 1 /\ found["a"] /\ "a" \notin locked;
  items["a"] := -1;
  found["a"] := FALSE;
  versions["a"] := 0;
  maxDeleteTs := 1;
  expired := TRUE;
  history := Append(history, items);
end process;

\* The Writer uses timestamp 2. This exceeds the timestamp watermark for both keys.
\* It locks both keys and commits a before b in separate local steps.
\* This order keeps old a plus new b out of every recorded physical state.
process Writer = "writer"
begin
PrepareA:
  \* Timestamp 2 places this write after the TTL deletion.
  await expired /\ 2 > maxDeleteTs;
  locked := locked \cup {"a"};
PrepareB:
  \* Timestamp 2 also exceeds b's last read timestamp.
  await 2 > lastReadTsB;
  locked := locked \cup {"b"};
CommitA:
  \* A has no row. Its insert reuses version 1.
  items["a"] := 1;
  found["a"] := TRUE;
  versions["a"] := 1;
  locked := locked \ {"a"};
  history := Append(history, items);
CommitB:
  \* B still has a row. Its update increases the version.
  items["b"] := 1;
  versions["b"] := versions["b"] + 1;
  lastReadTsB := 2;
  locked := locked \ {"b"};
  history := Append(history, items);
end process;

\* The Reader samples both keys in two passes. Each pass can visit either key first.
\* A pending write aborts the read. Otherwise, the reader compares the two stamps.
process Reader = "reader"
begin
ReadFirst:
  while remaining # {} do
    with k \in remaining do
      first[k] := [value |-> items[k], found |-> found[k], version |-> versions[k],
                   revision |-> revisions[k], pending |-> k \in locked];
      remaining := remaining \ {k};
    end with;
  end while;
CheckFirst:
  if \E k \in Keys : first[k].pending then
    goto Done;
  else
    remaining := Keys;
  end if;
ReadSecond:
  while remaining # {} do
    with k \in remaining do
      second[k] := [value |-> items[k], found |-> found[k], version |-> versions[k],
                    revision |-> revisions[k], pending |-> k \in locked];
      remaining := remaining \ {k};
    end with;
  end while;
Validate:
  accepted := \A k \in Keys : ~second[k].pending /\ Stamp(first[k]) = Stamp(second[k]);
end process;
end algorithm; *)

\* TypeOK bounds the values used by this model.
TypeOK == /\ now \in 0..1 /\ maxDeleteTs \in 0..1 /\ lastReadTsB \in {0, 2} /\ locked \subseteq Keys
          /\ versions \in [Keys -> 0..2] /\ revisions = [k \in Keys |-> 0]
          /\ items \in [Keys -> -1..1] /\ accepted \in BOOLEAN
\* The modeled TTL sweep leaves delete_revision unchanged.
SweepPreservesRevision == revisions = [k \in Keys |-> 0]
\* The sweep sets a's timestamp watermark to 1.
SweepSetsWatermark == expired => maxDeleteTs = 1
\* An accepted pair of payloads must match a state in history.
\* The history includes the state after each local commit, not only the final two-key state.
\* If no physical state matches the pair, no committed state matches it.
NoImpossibleRead == accepted => \E i \in 1..Len(history) : history[i] = [k \in Keys |-> first[k].value]
=============================================================================
