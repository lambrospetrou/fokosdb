# RFC — Extensible partition execution and single-partition batch writes

**State:** Draft
**Date:** 2026-08-30
**Author:** Lambros

References:

- `docs/agent-plans/2026-08-30-bounded-stateful-transaction-coordination.md`
- `docs/agent-plans/2026-08-29-stateless-transaction-coordination.md`
- `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
- `docs/ideas/topology-propagation-via-piggyback.md`
- [Cloudflare Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Durable Objects alarms](https://developers.cloudflare.com/durable-objects/api/alarms/)
- [Cloudflare Durable Object state](https://developers.cloudflare.com/durable-objects/api/state/)
- [Cloudflare Workers RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)

---

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Timeline and Milestones](#3-timeline-and-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Frequently Asked Questions](#6-frequently-asked-questions)

---

## 1. Overview and Context

### 1.1 The problem

`FokosDB` routes items to `PartitionDO` instances. Each partition can split when its SQLite storage crosses a
configured threshold. The parent then forwards operations to its children.

`TransactionCoordinatorDO` does not use this storage scaling model. A fixed pool stores coordinator state in
`tc_state`, `tc_participants`, and `tc_items`. Each coordinator has the Durable Object storage limit. The pool size
must provide enough storage and request capacity for the database.

The bounded coordinator plan adds garbage collection and derives the pool size from `rootTreesN`. Garbage
collection controls retained data, but pool sizing remains an operational capacity decision. A prolonged failure
can also retain non-terminal coordinator state beyond the normal retention window.

FokosDB needs a general extension that runs application logic inside the partition execution environment. The
logic must use FokosDB item operations for all durable state. The existing split and migration system can then move
that state without a separate coordinator pool.

### 1.2 Proposed capability

The public client gets a custom command operation:

```ts
await db.runCustom(
  { hashKey, sortKey },
  "driveTransaction",
  customArg,
);
```

FokosDB routes the command by the supplied item key. A configured `PartitionDO` subclass runs the named command.
The command receives a routed item client. The item client exposes the existing `putItem`, `getItem`, `deleteItem`,
and `queryItems` behavior.

The logic and the storage have separate lifecycles. The logic runs in the partition that receives the command.
Each storage operation routes to the current owner of its item. The storage operation can forward after a split.

The extension also supplies a periodic command hook. The base `PartitionDO` scheduler invokes this hook. The hook
shares the one Durable Object alarm with migration, splitting, transaction recovery, and key promotion.

### 1.3 Single-partition batch writes

FokosDB also gets `putItemSinglePartition()`. The operation accepts multiple puts. It applies them only when one
physical partition owns all keys.

The owning partition evaluates all conditions before it writes any item. It then applies all items in one local
SQLite transaction. When the keys resolve to more than one physical partition, the operation writes nothing and
returns an error.

A custom command can use this operation for its first durable state. The command then knows that the complete
initial state became visible atomically in one partition.

### 1.4 Terms

- **Anchor key:** The hash key and sort key that route a custom command.
- **Command owner:** The partition that runs the custom command.
- **Item owner:** The current partition that stores an item.
- **Routed item client:** The item API supplied to a custom command.
- **Control row:** The item that serializes one custom state machine.
- **Supporting row:** An immutable or replaceable item referenced by a control row.
- **Scheduled work:** Durable metadata that asks the base scheduler to run a command after a specified time.

The command owner and an item owner can differ after a split. The routed item client hides this difference from the
storage operation.

---

## 2. Goals and Requirements

### 2.1 In scope

1. FokosDB must route a custom command from an anchor key.
2. A `PartitionDO` subclass must supply the custom command implementation.
3. A custom command must store durable state through existing FokosDB item operations.
4. Each item operation must route to the current item owner.
5. A write to a migrating child must return a retryable migration error.
6. A migration error must not become an item-not-found result.
7. A custom command must be able to request scheduled work.
8. The base `PartitionDO` scheduler must own the Durable Object alarm.
9. `putItemSinglePartition()` must apply multiple puts atomically in one physical partition.
10. `putItemSinglePartition()` must write nothing when the keys resolve to multiple partitions.
11. The topology configuration must be able to disable hash-key promotion.
12. A disabled promotion mode must detect an unsplittable hash key before repeated hash splits create router chains.
13. The API must support a dedicated FokosDB instance for transaction coordinator state.
14. Coordinator recovery must route by a logical coordinator key instead of a physical Durable Object ID.
15. Custom behavior must preserve FokosDB split, migration, routing, and item consistency rules.

### 2.2 Correctness requirements

1. A custom command must write durable state before it sends an outbound effect authorized by that state.
2. An outbound effect must be idempotent when the command can retry it.
3. A state transition must identify the state-machine generation that it changes.
4. A token-based state machine must identify both the token and its current transaction ID.
5. A custom command must not treat a lost write response as proof that the write failed.
6. A custom command must not keep an unverified item result as authoritative state across an `await`.
7. A custom command must route each storage operation again after an `await`.
8. A range query must not act as the linearization point for a distributed state transition.
9. One control row must serialize a custom state machine that uses multiple supporting rows.
10. The control row must describe the expected supporting rows before the command uses them.
11. A scheduled command must be idempotent and crash-safe.
12. A scheduled command must process a bounded amount of work in one invocation.

### 2.3 Availability requirements

1. Reads that support the current migration fallback can read from the parent during child migration.
2. Writes must wait until the item owner completes migration.
3. A custom command must return a retryable error after its retry budget ends.
4. An unavailable remote dependency must not block a partition split without a bound.
5. A parent that became a router must not modify its stale local item copies.
6. The current item owner must be able to recover unfinished custom work after migration.

### 2.4 Performance requirements

1. A command that runs on the item owner must use local storage for locally owned items.
2. The routed item client must not start normal operations from the root when the command already has a valid
   partition context.
3. `putItemSinglePartition()` must use one SQLite transaction after the server confirms one destination.
4. Scheduled work must use an index. It must not scan all partition items on each alarm.
5. The implementation must measure the extra forwarding cost after a split.
6. The implementation must measure the request capacity of custom commands that await remote RPCs.

### 2.5 Out of scope

- **Arbitrary custom SQL tables.** The first version migrates state through existing FokosDB items only.
- **A raw SQLite client.** Raw access can bypass routing, migration, conditions, and size accounting.
- **Dynamic code upload.** The Worker deployment contains all custom command code.
- **A direct asynchronous callback after every item RPC.** A durable outbox is a separate design.
- **Automatic merge of partitions.** A split remains permanent under the current topology model.
- **Client-side direct leaf routing.** The topology propagation design covers that optimization.
- **Cross-table atomic writes.** `putItemSinglePartition()` applies to one FokosDB instance.
- **Item TTL implementation.** This design can use item TTL after that feature exists.

---

## 3. Timeline and Milestones

TODO: Define shippable milestones after the API contracts and the scheduled-work schema are selected.

---

## 4. Proposed Solution

### 4.1 High-level overview

A Worker supplies a `PartitionDO` subclass. The subclass maps command names to deployed functions. The base class
owns routing, migration guards, split forwarding, the item client, and the scheduler.

```text
Worker
  |
  | runCustom(anchorKey, command, argument)
  v
root partition
  |
  | split forwarding when required
  v
command owner
  |
  | routed item operations
  +--------------------> local item owner
  |
  +--------------------> child item owner
  |
  +--------------------> range subtree
```

A custom command does not own a permanent local database. It owns a logical state machine stored in FokosDB items.
The routed item client finds the current item owner for each operation.

A hash split keeps all items with one hash key together. A range promotion can divide the supporting rows by sort
key. The custom state machine therefore uses a control row for all state transitions. The supporting rows do not
replace the control row as the decision authority.

A dedicated coordinator table disables hash-key promotion. One transaction then remains under one hash key. Hash
splits can distribute different transaction keys between child partitions.

### 4.2 Technical details

#### 4.2.1 `runCustom()`

The initial public shape is:

```ts
runCustom(
  key: { hashKey: Key; sortKey?: Key },
  operation: string,
  argument: unknown,
): Promise<unknown>;
```

The implementation must encode and validate the anchor key at the FokosDB boundary. `argument` and the result must
use Workers RPC serializable values.

The base partition RPC must do these steps:

1. Validate the `PartitionContext`.
2. Check the migration state.
3. Route the anchor key through the current split topology.
4. Invoke the subclass dispatcher only on the selected command owner.
5. Supply the routed item client and command metadata.
6. Return the command result or a classified error.

When a parent is a router, it must forward the complete command. It must not invoke the subclass dispatcher on its
stale item copies.

The Worker cannot pass the command function over Workers RPC. A function argument becomes a callback to the sender.
The function would not become code in the destination Durable Object. The deployment must contain the dispatcher.

TODO: Define the typed command map, the result type, and the exact subclass hook name.

#### 4.2.2 Routed item client

The routed item client exposes the semantics of these FokosDB operations:

- `putItem`
- `getItem`
- `deleteItem`
- `queryItems`
- `putItemSinglePartition`
- scheduled-work operations

The item client must start from the current partition context. It must use the local store when this partition owns
the item. It must use the existing forwarding path when a child owns the item.

The item client must use the public item validation, encoding, condition, and response rules. It must not call a new
`FokosDB` client that routes from a root partition.

When an old command continues after a split, its next storage operation can reach a child. When the child is
migrating, a write fails with the existing retryable migration behavior. The command can retry the storage operation
or return a retryable command error.

The item client gives per-item consistency. It does not make multiple routed operations atomic. The custom protocol
must use a control row or `putItemSinglePartition()` when it needs a larger atomic unit.

#### 4.2.3 `putItemSinglePartition()`

The operation accepts one or more put requests:

```ts
putItemSinglePartition({
  items: PutItemOptions[],
}): Promise<PutItemSinglePartitionResult>;
```

The FokosDB client performs normal key and data validation. The first routing result is only a hint. The receiving
partition makes the authoritative routing decision.

The receiving partition must use the same routing shape as `PartitionDO.routeSingleDestination()`:

- When all items are local, the partition applies them locally.
- When one child owns all items, the partition forwards the complete request to that child.
- When local and forwarded items are mixed, the operation fails before a write.
- When more than one child owns the items, the operation fails before a write.

The destination partition must call the migration guard before it evaluates any item. It must not await between the
final routing decision and the local SQLite transaction.

Inside the SQLite transaction, the destination partition must:

1. Reject duplicate item keys in the request.
2. Read the state required by all item conditions.
3. Evaluate every condition.
4. Stop without writes when one condition fails.
5. Apply every put when all conditions pass.
6. Update the size estimate for each hash key.

After the transaction commits, the partition must run the split and promotion checks required by the written items.
A failure before the transaction commits leaves every item unchanged.

The operation is not idempotent without conditions. A retry after a lost response can apply each put again and
increment its item version. A caller that needs idempotency must use `item_not_exists`, an expected version, or a
protocol-specific generation check.

TODO: Define `PutItemSinglePartitionResult`, the structured routing error, and condition-failure results.

#### 4.2.4 Optional scoped `transactionSync`

A custom command can need a local transaction that includes reads, puts, and deletes. A scoped `transactionSync`
could provide this capability when every key remains in one physical partition.

The callback must be synchronous. The API must reject an asynchronous callback. The callback must not get raw
access to FokosDB internal tables.

This option is not selected. Section 4.3.1 records the open question.

#### 4.2.5 Control rows and supporting rows

A custom state machine that uses multiple items must have one control row. The control row is the only authority for
the current state and decision.

A transaction coordinator can use this provisional layout:

```text
hashKey = coordinatorKey
sortKey = "$claim"

hashKey = coordinatorKey
sortKey = "$tx/<transactionId>/state"

hashKey = coordinatorKey
sortKey = "$tx/<transactionId>/participant/<partition-name>"

hashKey = coordinatorKey
sortKey = "$tx/<transactionId>/item/<index>"
```

A token transaction uses the claim row to map the token to its current transaction ID. The command does not update
this row after the claim. A tokenless transaction can omit the claim row because its coordinator key is its
transaction ID.

The control-row key contains the transaction ID. A later transaction that reuses an expired token gets a different
control-row key. A delayed old command can then modify only the old transaction state.

The control row contains at least:

- the transaction ID;
- the protocol state;
- the transaction timestamp;
- the operations hash for a token transaction;
- the expected item count;
- the expected participant count;
- completion and retention data.

The first publication uses this sequence:

1. Generate the transaction ID.
2. Build the claim row, control row, and all supporting rows.
3. Apply all rows with one `putItemSinglePartition()` call.
4. Require `item_not_exists` for each new row.
5. Send the first prepare RPC after the batch succeeds.

A lost batch response leaves the publication outcome unknown. The command reads the claim and control rows. It
continues only when both rows identify the expected transaction ID and operation set.

Each control-row transition must check the expected item version. The transaction ID in the item key prevents a
later token generation from reusing the same control row. A failed version condition is a read of concurrent
progress. The loser must load the current row and follow its outcome.

A query over supporting rows must validate the row count and generation against the control-row manifest. A query
result does not decide the transaction.

#### 4.2.6 Split and migration behavior

A custom command can await remote RPCs. The await permits other requests and alarms to run. A split can therefore
advance while the command is active.

All state transitions use routed item operations. The command must not write through a cached local store after an
`await`. The next write routes to the current item owner.

The split sequence is:

1. The command writes a durable state transition.
2. The command sends the outbound RPC for that transition.
3. A split can start while the command awaits the response.
4. The child migrates all ordinary items for its key range.
5. The old command resumes.
6. Its next item operation routes to the child.
7. A migrating child rejects the write.
8. The command retries after migration or returns a retryable error.

The parent must stop local writes after `split_started`. The child must reject writes until migration completes. The
existing FokosDB rules provide these two guards.

A read during migration can use the existing parent fallback where that operation supports it. The read must not
interpret migration as absence.

The implementation can stop new custom commands at `split_queued`. This reduces active work before migration. This
rule does not replace routed storage because an older command can still be active.

#### 4.2.7 Hash-key promotion configuration

The topology configuration must support a mode that disables hash-key promotion. The exact option name remains
open.

When promotion is disabled, a hash split cannot distribute one oversized hash key. The split moves the full key to
one child. A repeated split can create a router chain without reducing that child's storage.

The disabled mode therefore needs a per-hash-key storage limit. A write that crosses the limit must return a
specific capacity error. Reads and deletes must remain available so the caller can remove data.

The split policy must detect a partition whose excess storage comes from one unsplittable hash key. It must not
repeat hash splits that cannot reduce the key's storage.

A dedicated transaction coordinator table uses the disabled mode. Its transaction size limit bounds all rows under
one coordinator key below the configured per-key storage limit.

TODO: Define the configuration field names, the per-key threshold, and the capacity error contract.

#### 4.2.8 Range promotion when enabled

A general custom table can keep hash-key promotion enabled. The routed item client then sends each item operation to
the range partition that owns its sort key.

A query can walk multiple range children. The returned rows are not one atomic snapshot across those children. The
custom state machine must use one control row as its linearization point.

The control row's sort key selects its physical owner. Supporting rows can live in other range children. A command
that needs them pays the required forwarding and query fan-out.

`putItemSinglePartition()` fails when its items span range children. The operation must not change this rule to
support range fan-out.

#### 4.2.9 Scheduled work

A Durable Object has one alarm. The subclass must not override the base alarm or call `setAlarm()` independently.
The base scheduler must combine the next custom wake time with all existing background jobs.

The base infrastructure needs a durable scheduled-work index. Each record must include enough data to route the work
after a split. At minimum, it identifies the anchor key, command, generation, and due time.

The split migration must move each scheduled-work record to the child that owns its anchor key. A child must not run
the command before item migration completes.

The scheduler must:

1. Select a bounded number of due records from an index.
2. Invoke the command through the normal routing and migration guards.
3. Treat each invocation as at least once.
4. Remove or reschedule a record with a generation condition.
5. Continue with other records when one record fails.
6. Set the next alarm before it returns from a failed cycle.

TODO: Define the scheduled-work schema, batch size, retry policy, and dead-letter behavior.

#### 4.2.10 Custom command concurrency

Two calls can run the same logical command. A client retry can also overlap an older call whose response was lost.
The command must not depend on one in-memory invocation.

The control row serializes the command. Each state transition uses compare-and-set conditions. An invocation that
loses the condition reads the current control row and follows that state.

`blockConcurrencyWhile()` must not protect the complete command. Cloudflare applies a 30-second timeout to that
method. It would also serialize unrelated commands while one command waits for remote I/O.

A synchronous local transaction can protect one local phase. It must not contain remote I/O.

#### 4.2.11 Errors and retries

The API needs structured equivalents for these outcomes:

- the command name is unknown;
- the argument is invalid;
- the anchor partition is migrating;
- the item owner is migrating;
- the command moved during a split;
- `putItemSinglePartition()` found multiple destinations;
- a condition failed;
- one hash key crossed its disabled-promotion limit;
- the custom command reached its retry budget;
- the custom command failed permanently.

Migration, ownership movement, overload, and an unavailable remote dependency can be retryable. Invalid input,
unknown commands, and an unsplittable key at its hard limit are not retryable without a caller change.

A lost response from a write leaves the outcome unknown. The caller must read or retry with the same generation. It
must not retry as a new generation until it resolves the old one.

TODO: Define exact error types, messages, and retry metadata.

#### 4.2.12 Transaction coordinator use

A dedicated FokosDB instance can replace the static coordinator pool. It uses a `PartitionDO` subclass that contains
the transaction command dispatcher.

The coordinator key is:

- the client request token for a token transaction;
- the transaction ID for a tokenless transaction.

Each participant lock must store the logical coordinator key and the transaction ID. Recovery derives the
coordinator table context and calls the recovery command through normal FokosDB routing.

Recovery must verify that the control row contains the same transaction ID as the lock. A later transaction can reuse
a token after its retention window. The old lock must not adopt the later transaction's outcome.

The coordinator command keeps the existing write-ahead rules:

1. It publishes the complete initial state before prepare.
2. It writes `PREPARING` before it sends prepare RPCs.
3. It writes `PREPARED` before it sends commit RPCs.
4. A `PREPARED` transaction must eventually commit.
5. It writes `CANCELLING` before it sends cancel RPCs.
6. It records participant confirmation before it removes recovery data.

The coordinator can delete supporting rows after all participants confirm. The control row remains for the
idempotency window. Item TTL can remove it after the TTL feature exists.

Storage splitting bounds each coordinator partition below its configured threshold. Garbage collection still
controls aggregate storage and the token retention contract.

#### 4.2.13 Performance

Before a split, a command that owns all its state uses local item storage. Its remote protocol keeps the current
shape of one command hop, one prepare round trip, and one commit round trip.

After a split, the current Worker router first reaches a root partition. A root can forward the command to a leaf.
An old command can also forward later item operations. These paths add sequential Durable Object RPCs.

The topology propagation design can reduce intermediate routing hops. It does not change correctness.

TODO: Measure these paths:

- a local custom command with one item read and one item write;
- `putItemSinglePartition()` with the maximum item count;
- a custom command forwarded through one hash parent;
- a command that resumes during child migration;
- a range-enabled command that queries supporting rows across children;
- one coordinator transaction on the current pool and on the custom command design.

#### 4.2.14 Deployment, migration, and rollback

TODO: Define the deployment order, schema migrations, compatibility rules, and rollback behavior.

The deployment plan must cover old participant locks that contain a physical coordinator DO ID. It must also cover
scheduled-work records created by each deployed command version.

#### 4.2.15 Testing

Tests must run in the Workers runtime. They must cover a normal `PartitionDO` class and a custom subclass.

`putItemSinglePartition()` tests must prove:

- one local destination applies all puts;
- one child destination forwards and applies all puts;
- two destinations apply no puts;
- a mixed local and child route applies no puts;
- one failed condition applies no puts;
- duplicate keys fail before writes;
- a migrating destination returns a retryable error;
- a crash cannot expose a partial batch;
- a lost response is safe when the caller uses an idempotent condition.

Custom command tests must prove:

- the base class rejects an unknown command;
- an argument and result cross Workers RPC correctly;
- the command uses local storage before a split;
- an item operation forwards after a split;
- a write retries after child migration;
- a command never modifies stale parent items;
- two concurrent commands serialize through the control row;
- a lost write response does not create a second generation;
- a range query validates supporting rows against its manifest.

Scheduled-work tests must prove:

- the base alarm runs due custom work;
- custom work does not replace a migration or split alarm;
- a scheduled record migrates to the owning child;
- a migrating child delays the command;
- an invocation can run more than once without duplicate effects;
- one poison record does not stop other records;
- the scheduler re-arms after its automatic alarm retries end.

Transaction coordinator tests must inject a split at each write-ahead boundary. They must prove that a token replay
and participant recovery find the same transaction and outcome after migration.

### 4.3 Open Questions

#### 4.3.1 Does the routed item client expose a scoped `transactionSync()`?

Option A exposes only `putItemSinglePartition()`. This keeps the extension API small and preserves the existing item
contracts.

Option B also exposes a synchronous transaction callback over a restricted item interface. This supports mixed
reads, puts, and deletes in one local transaction. It needs a key declaration and a server routing check before the
callback starts.

The answer changes the extension API and the atomic operations available to custom commands.

#### 4.3.2 Does an old command continue through forwarding or relocate?

Option A lets the old command retry routed item operations from the parent. This can finish without a complete
command retry, but each forwarded operation adds an RPC.

Option B returns a retryable ownership-moved result after the current durable phase. A new command then runs on the
owning leaf.

The answer changes retry latency, command complexity, and the time that old parents run custom compute.

#### 4.3.3 What is the typed command registration API?

The API can use a subclass dispatcher, a static command map, or one overridden method with a discriminated command
union. The selected API must preserve request and result types at the FokosDB boundary.

#### 4.3.4 What disables hash-key promotion?

The topology needs an explicit configuration value. The answer must also define the per-hash-key hard limit and the
behavior of the split policy at that limit.

#### 4.3.5 How does scheduled work migrate?

The index can use an internal SQL table, reserved FokosDB items, or another base-owned structure. The answer must
preserve due-time order and route each record by its anchor key.

#### 4.3.6 How does a deployment version scheduled commands?

A stored scheduled command can outlive the code version that created it. The record can carry a command schema
version, or the deployment can keep backward-compatible command readers.

#### 4.3.7 Which retry budget belongs inside `runCustom()`?

The command can retry migration and remote errors internally, or it can return after the first retryable error. The
answer changes tail latency and duplicate concurrent invocations.

#### 4.3.8 What limits apply to `putItemSinglePartition()`?

The API needs limits for item count, request bytes, and result bytes. The coordinator publication can contain one
claim row, one control row, one row per operation, and one row per participant. The selected limits must support the
maximum transaction state or define a second crash-safe publication protocol.

---

## 5. Alternative Options

### 5.1 Keep the bounded static coordinator pool

Keep `TransactionCoordinatorDO`. Add the garbage collection, retention, and pool sizing from the bounded coordinator
plan. This is a smaller change. It keeps the pool as a separate capacity decision.

### 5.2 Use a FokosDB decision ledger with Worker compute

Store transaction decisions in a FokosDB table and run the protocol driver in the Worker. This uses normal table
splitting, but the durable decision adds one storage round trip to the protocol.

### 5.3 Permit arbitrary custom SQL tables

Let the subclass create and query any local schema. This needs a generic split migration contract for every table,
index, KV key, cursor, and cleanup rule. The first version does not include this option.

### 5.4 Block each complete command with `blockConcurrencyWhile()`

Hold the partition input gate while the command awaits remote work. This prevents split interleaving, but it blocks
unrelated commands. Cloudflare also resets the Durable Object when the callback crosses the 30-second limit.

### 5.5 Run a direct callback after each item RPC

Run custom asynchronous code after a normal item operation commits. A callback failure makes the item result
ambiguous. An immediate response can lose the callback, while a delayed error can cause a caller to repeat it.

A durable outbox can support this behavior in a separate design.

---

## 6. Frequently Asked Questions

### Why run the logic inside a partition if its storage operations can become remote?

The common path keeps the control row and supporting rows in the command owner. The command then gets local durable
writes between remote protocol phases. Forwarding preserves progress when a split changes ownership.

### Does routed storage make the full custom command serializable?

No. Each item operation is strongly consistent. The control row and its compare-and-set transitions serialize the
custom state machine.

### Why add `putItemSinglePartition()` when write transactions already exist?

The operation provides a smaller primitive. It uses one partition and one SQLite transaction. It does not create a
coordinator, participant locks, or a two-phase protocol.

### Can `putItemSinglePartition()` fan out to children?

It can forward the complete request when one child owns every key. It must fail before writes when multiple
partitions own the keys.

### What happens if the partition splits after the batch routing decision?

The destination performs no `await` between its final routing decision and the SQLite transaction. The split cannot
advance during that synchronous block.

### Can a command store all state in memory?

No. The Durable Object can stop after any `await`. Durable items and scheduled work must contain all recovery state.

### Can a command use range splitting?

Yes. Each storage operation routes by its full key. The command must use a control row because supporting rows can
span range children.

### Why disable hash-key promotion for transaction coordinator state?

All rows for one transaction use one coordinator hash key. Keeping them together preserves local state transitions
and avoids query fan-out. The transaction size limit keeps that hash key below its configured hard limit.

### Does splitting remove the need for coordinator garbage collection?

No. Splitting bounds storage per Durable Object. Garbage collection controls total retained storage and the token
idempotency window.

### Can the subclass set its own alarm?

No. One Durable Object has one alarm. The base scheduler must combine custom work with all FokosDB background jobs.
