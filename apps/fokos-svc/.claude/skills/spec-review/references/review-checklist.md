# Review checklist

Run each pass over the whole document. A pass is a lens, not a section of the
report. Findings from all passes go into one severity-ordered report.

Do not answer these questions in the report. Use them to find defects.

---

## Pass 1 — Correctness

The spec does what it says, and what it says is possible.

- **Self-consistency.** Does section N contradict section M? Do the diagram, the
  prose, the table, and the code snippet agree? Does an example match the rule it
  demonstrates?
- **Terminology.** Is one thing given two names, or two things given one name? A
  renamed concept mid-document hides a real change of meaning.
- **State machines.** Is every state reachable? Can every state exit? Is each
  transition triggered by exactly one condition? What happens on a repeated or
  out-of-order trigger? Which transitions are illegal, and what enforces that?
- **Boundaries.** Empty input, one item, the maximum item count, the maximum
  payload size, a missing optional field, a null, a zero, a negative number, the
  first page, the last page, an empty page.
- **Types and encodings.** Does the encoding preserve the order the spec needs?
  Is a key comparison byte-wise or text-wise? Where does the type change between
  the client, the wire, and SQLite?
- **Interface contract.** For each new or changed operation: inputs, outputs,
  errors, and which errors the caller must retry. Are the error codes named?
- **Backward compatibility.** What happens to data written by the current code?
  What happens to an in-flight request during the deploy? Is there a version
  field, and who writes it?
- **Proofs.** If the spec claims an invariant, follow the argument. A claim such
  as "this cannot happen" needs the mechanism that prevents it, not a promise.

## Pass 2 — Consistency of transactions and operations

The pass that finds data loss. Give it the most time.

- **Atomicity.** Which writes must apply together? What partial state exists
  between them? Can a reader see that partial state? Can a crash leave it?
- **Isolation.** What does a concurrent reader see during the operation? Is a
  read-modify-write protected, or does it lose an update?
- **Durability order.** Does the spec persist the decision before it sends the
  outbound call? Every state change must be written before the effect it causes
  becomes visible. Find each place the spec sends first and writes after.
- **Point of no return.** After which step must the operation finish? Who
  finishes it if the driver dies at that step? What resumes it, and when?
- **Idempotency.** Replay every step twice. Prepare, commit, cancel, the retry of
  a partial fan-out, and the recovery path must all be idempotent. Which key
  makes them idempotent? Does that key stay stable across a retry, a resize, or a
  new deploy?
- **Locks.** Who takes a lock, who releases it, and what releases it when the
  taker never returns? Can two paths release the same lock? Can a lock outlive
  the transaction that owns it?
- **Conflict detection.** Which timestamps or versions decide a conflict? Are
  they monotonic? Who advances them? What happens when two operations read the
  same value and both write?
- **Interaction with existing operations.** Take each existing operation
  (`putItem`, `deleteItem`, `getItem`, `queryItems`, the transactional variants,
  recovery alarms, splits, migration) and ask what happens when it runs at the
  same time as the new one. Name any pair the spec does not cover.
- **Ordering.** Does the operation depend on messages arriving in order? Workers
  RPC gives no cross-object order guarantee. Find each hidden order assumption.
- **Cross-object atomicity.** Two Durable Objects cannot commit together without
  a protocol. Find each place the spec assumes they can.

## Pass 3 — Performance

- **Cost per operation.** Count the round trips, the storage reads, the storage
  writes, and the bytes moved for the common path. Compare against the operation
  the spec replaces. Say the number.
- **Growth.** How does the cost grow with the item count, the item size, the
  transaction size, the partition count, and the concurrency? Find each accidental
  O(n) scan and each fan-out to all partitions.
- **Hot spots.** Does one key, one partition, one coordinator, or one alarm take
  all the load? A single Durable Object is single-threaded.
- **Blocking.** What holds a lock, blocks the input gate, or awaits a remote call
  while it holds state? Long remote calls under a lock serialize the whole object.
- **Storage.** How much does each new column, index, or row add per item? Does
  anything grow without a bound? Name the garbage collector, or name the gap.
- **Payload limits.** Does the request or response fit the payload limit for the
  maximum case, not the example case?
- **Cache and batching.** What is batched, and what is the batch size? Is a cache
  invalidated on every path that changes the source of truth?

## Pass 4 — Reliability

- **Failure enumeration.** For each step, ask what happens when it fails, when it
  times out, and when it succeeds but the reply is lost. The lost reply is the
  case specs forget.
- **Recovery.** Who notices a stuck operation, how soon, and what does it do?
  Does recovery use the public path, with all its guards, or does it inline the
  work and skip them?
- **Retries.** Which errors are retryable? Is there a bound, a backoff, and a
  dead end? Does a retry storm feed itself?
- **Poison input.** Can one bad request stop an alarm, a queue, or a migration
  forever?
- **Crash points.** Walk the operation and ask, at each line, what a crash leaves
  behind. Is the leftover state visible, repairable, and repaired?
- **Observability.** After the failure, can an operator tell what happened? What
  is logged, counted, or stored to make the failure findable?
- **Testability.** Can each failure path be forced in a test? A path that cannot
  be tested will be wrong.
- **Migration and rollback.** What is the deploy order? Can the change be turned
  off after it writes data? What happens to data written by the new path when the
  old code reads it?

## Pass 5 — Availability

- **Blast radius.** When one component is down, what stops? One key, one
  partition, one table, or everything?
- **Single points.** Does one object, one lock, one coordinator, or one ledger
  hold every request? A named object is a single point.
- **Degradation.** Can reads continue when writes cannot? Can the system return
  stale data instead of an error, and does the spec say which operations may?
- **Windows.** During a split, a migration, a resize, or a deploy, which
  operations are rejected, and for how long? Is the window bounded? Does the
  client know to retry?
- **Overload.** What happens when the arrival rate is higher than the service
  rate? Is there a limit, a queue bound, or a rejection?
- **Dependencies.** Every new dependency is a new way to fail. For each one, ask
  what the system does when it is unavailable.

---

## FokosDB hazards

The failure modes this codebase repeats. Check each spec against all of them.

1. **Migration guard.** Every write and transaction RPC on `PartitionDO` must
   call `ensureMigration`. A child in `migration_migrating` holds incomplete data
   and incomplete locks. Read RPCs that tolerate stale data read from the parent
   instead.
2. **Split routing.** A parent in `split_started` or `split_completed` owns no
   keys. Writes and locks must reach the children. `cancel` must forward at both
   states, or pending rows leak forever.
3. **Swallowed fan-out errors.** When the code forwards to several children, it
   must attempt every child, collect the failures, and rethrow. A swallowed error
   moves the coordinator to a terminal state with work undone.
4. **Recovery through private paths.** Recovery must call the public `commit()`
   and `cancel()`, because the guards and the routing live there.
5. **`splitN` immutability.** Any change to the split factor after
   initialization breaks routing and loses data.
6. **`PartitionContext` on every RPC.** Durable Objects take no configuration at
   instantiation. New RPCs must carry and validate the context.
7. **Write-ahead order.** The coordinator writes each state transition to SQLite
   before it sends the outbound RPC.
8. **`PREPARED` is final.** A prepared transaction must commit. No path may
   cancel it.
9. **Idempotency token.** The `clientRequestToken` names the coordinator. Any
   change to naming, pooling, or the pool size breaks replay.
10. **Committed state only.** The `items` table holds committed rows.
    `pending_transactions` holds the locks. A spec that writes uncommitted data
    into `items` is wrong.
11. **Durable Object limits.** 10 GB per object, one thread, an alarm at a time.
    Check the current Cloudflare limits page; do not trust memory.
12. **Internal columns are not public.** `last_transaction_ts`, `data_kind`, and
    `est_row_bytes` must not become part of a public interface.
