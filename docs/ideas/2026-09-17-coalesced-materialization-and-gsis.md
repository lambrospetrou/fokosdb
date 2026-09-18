# RFC — Coalesced materialization for latest-image streams and global secondary indexes

**State:** Draft
**Date:** 2026-09-17
**Author:** Lambros Petrou

**Status:** Nothing is built. This document records the current design for further discussion.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Timeline and milestones](#3-timeline-and-milestones)
- [4. Proposed solution](#4-proposed-solution)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

FokosDB has no global secondary index or public change stream. A global secondary index, or GSI,
must organize base items by a different partition key. A latest-image stream must expose the newest
unconsumed state of each base item.

The latest-image stream is not a change data capture log. If one item changes from `A` to `B` to `C`
before consumption, the stream can return only `C`. The stream can also omit a temporary deletion
when the item is recreated before consumption.

A GSI needs more state than the latest-image stream. When an indexed item changes from key `A` to
key `C`, the GSI must remove `A` and write `C`. The latest image alone does not identify `A`.

Base items can move through multiple `PartitionDO` instances during repartition. An old source can
hold an unexported revision after a newer owner has exported another image. A consumer must not
depend on delivery order from those physical partitions.

The design adds one coalesced materialization layer. The layer uses the stable base item identity as
its route key. It keeps the latest image and the last applied state for each GSI. Base partitions
send monotonic revisions, so the layer rejects stale updates from old partition owners.

### 1.1 Glossary

| Term | Meaning |
| --- | --- |
| Base item | One item in the user table, identified by its base hash key and base sort key. |
| Base revision | A monotonic revision for one base item across repartitions. |
| Export outbox | A coalesced durable marker that identifies the newest unexported base item revision. |
| Materialization item | The durable latest state for one base item in the materialization layer. |
| Materialization partition | One `MaterializationDO` in the independent materialization shard group. |
| Latest image | The newest live image or deletion tombstone that the materialization layer accepted. |
| Applied image | The GSI key and revision that one GSI has completed for one base item. |
| In-flight image | The immutable GSI target that a materialization partition is applying. |
| GSI entry tombstone | A hidden GSI row that rejects a delayed operation with an older revision. |

## 2. Goals and requirements

### 2.1 In scope

- The base write must capture one coalesced export marker in the item mutation transaction.
- The baseline export protocol must send one complete live image or one deletion tombstone.
- One materialization item must keep the newest accepted state for one stable base item identity.
- A base revision must increase for one item across hash splits, range splits, and key promotions.
- Old base partitions must be able to export after a repartition without causing state regression.
- A latest-image subscription must read the materialization items without one copy per subscription.
- A GSI must derive its desired entry from the latest image and its previous entry from applied state.
- A GSI transition must resume after a crash without a permanent duplicate or omission.
- The materialization shard group must split without losing the latest image or GSI progress.
- Online GSI creation must use the materialization layer as its backfill source.
- A future pointer payload must come before source-derived GSI projections in the extension order.

### 2.2 Out of scope

- The design does not keep every base mutation. It does not provide a complete CDC history.
- The latest-image stream does not promise delivery of each intermediate image.
- The latest-image stream does not promise delivery of a deletion followed by a recreation.
- The GSI does not provide a strongly consistent read of the base table.
- The proposed GSI move does not prevent a temporary duplicate during asynchronous propagation.
- The design does not provide a global order across different base items.
- The design does not make a multi-item base transaction atomically visible in a GSI.
- The baseline does not validate GSI keys in the base partition.

### 2.3 Requirements

1. The base partition must write its item mutation and export marker in one storage transaction.
2. A live export marker must identify the current item row and revision without a second image copy.
3. The baseline exporter must read and send the complete live image for the marked revision.
4. A transaction prepare, failed condition, check, or cancel must not create a materialization update.
5. A delete of an absent item must not create a materialization update.
6. A TTL delete must create a deletion image through the same mutation path as a user delete.
7. Migration of an item must not create a logical item mutation.
8. The materialization layer must accept an update only when its base revision is newer.
9. The export acknowledgement must name the exported revision.
10. The base partition must keep a newer outbox row when it receives an older acknowledgement.
11. A GSI maintainer must freeze an in-flight target before it sends the first GSI operation.
12. A new latest image must not change an existing in-flight target.
13. The GSI maintainer must write durable phase state before each outbound operation.
14. A GSI operation must include the base item identity, base revision, and operation deadline.
15. A GSI entry must reject an older revision and an expired operation.
16. A GSI delete must write a revision fence before physical cleanup removes the old entry.
17. A GSI key violation must not reject a base write in the baseline.
18. A materialization split must move the latest image and every GSI state for the item together.
19. A failed GSI operation must not reject its materialization ingestion or a committed base write.
20. A materialization persistence failure must stop the acknowledgement of a base export.
21. A base write must fail when the partition cannot preserve both the item and its export marker.
22. A recovery pass must limit its concurrency, item count, and byte count.
23. Every page, batch, and background step must have item and byte bounds.
24. Every background operation must be idempotent and resumable.

## 3. Timeline and milestones

TODO: Define the implementation milestones after the open questions in section 4.18 are resolved.

The unified repartition flow in `docs/agent-plans/2026-09-17-unified-repartition-flow.md` is a
prerequisite. Its cutover record and import protocol must carry the revision high watermark.

## 4. Proposed solution

### 4.1 High-level overview

The base table and the materialization layer use independent shard groups. A stable base item key
routes every exported image to one logical materialization item. A base-table split does not change
that route.

```text
Base PartitionDO
  item mutation
  + coalesced durable export marker
          |
          | exporter reads the marked live image
          | complete image or tombstone
          | monotonic base revision
          v
MaterializationDO shard group
  one latest row per base item
  one applied state per base item and GSI
  one immutable in-flight transition per base item and GSI
          |
          +-------------------------+
          |                         |
          v                         v
 Latest-image subscriptions      GSI shard groups
```

The base partition keeps only the newest export marker for an item. The live item row remains the
only local image copy. The materialization layer keeps only the newest accepted image. An outage
therefore increases the dirty item count, not the mutation count.

Each GSI compares its applied key with the key derived from the latest image. The GSI can skip all
intermediate images when no transition has started. Once a transition starts, the maintainer must
finish its immutable target before it starts a newer target.

### 4.2 Base mutation capture

`PartitionStore` must provide one content-mutation boundary for all item mutations. The boundary
must cover these paths:

- `PartitionDO.apiPutItem`.
- `PartitionDO.apiDeleteItem`.
- `TransactionParticipant.commitLocal`.
- `TransactionParticipant.executeSingleShot`.
- `PartitionStore.deleteExpiredItems`.

The boundary receives the base key, the previous state, the new state, the cause, and an optional
transaction ID. It allocates the base revision and writes one export marker.

```sql
CREATE TABLE materialization_outbox (
    hk       BLOB    NOT NULL,
    sk       BLOB    NOT NULL,
    revision INTEGER NOT NULL,
    deleted  INTEGER NOT NULL,
    PRIMARY KEY (hk, sk)
) WITHOUT ROWID, STRICT;
```

The item row remains the only complete live image in the base partition. The export marker stores
only the key, revision, and deletion state. A newer local mutation replaces an older marker for the
same item.

The baseline exporter uses this sequence for a live marker:

1. It reads the marker and the current item in one local snapshot.
2. It verifies that the item revision equals the marker revision.
3. It sends the complete encoded image and revision to the materialization partition.
4. It deletes the marker only when its stored revision still equals the acknowledged revision.

A deletion marker needs no image. The materialization layer already keeps the previous image and the
GSI applied state.

When revision 11 is in flight and revision 12 arrives, the outbox keeps revision 12. The
acknowledgement for revision 11 uses a compare-and-delete operation:

```sql
DELETE FROM materialization_outbox
WHERE hk = ?
  AND sk = ?
  AND revision = 11;
```

A split source must keep the item data that an unacknowledged live marker references. The source can
remove that data after the materialization layer acknowledges an equal or newer revision.

The base partition repeats a ready notification while an outbox row exists. A lost notification
does not lose the row. The notification contains no item payload.

### 4.3 Base revisions and repartition

The current `items.item_id` value must not identify an event. `PartitionStore` can reuse the value
after a deletion. The value is also local to one partition lineage.

The public item version `v` must not order materialization updates. A new row starts at version 1,
and a deletion removes its version.

Each base partition keeps a monotonic mutation clock. A content mutation gets the next value from
this clock. A repartition cutover stores the source high watermark in the same transaction as the
routing cutover.

Each target initializes its clock at the source high watermark. The first target mutation uses a
larger value. Sibling partitions can use overlapping values because one base item follows only one
child.

```text
P0 revisions: 1 ... 100
P0 -> P1 cutover at 100
P1 revisions: 101 ... 180
P1 -> P2 cutover at 180
P2 revisions: 181 ...
```

The event identity is the pair `(sourcePartitionId, sourceSequence)`. The base revision is the
ordering fence for one base item.

A source partition keeps its unacknowledged export rows after cutover. It continues its export job
while it acts as a router. The repartition cleanup must not remove an unacknowledged export row.

A target can export a newer image before the source exports its older image. The materialization
layer accepts the newer revision and rejects the older revision. This rule removes a delivery-order
dependency between base partitions.

### 4.4 Materialization routing and ingestion

The materialization shard group uses the stable tuple `(tableId, baseHashKey, baseSortKey)` as its
route key. The route does not include a physical base partition ID.

A materialization partition accepts a versioned payload variant:

```ts
type ApplyLatestImageRequest = {
	schema: 1;
	tableId: string;
	baseHashKey: KeyBytes;
	baseSortKey: KeyBytes;
	revision: number;
	capturePlanVersion: number;
	payload:
		| { mode: "full"; image: EncodedItemImage }
		| { mode: "tombstone" }
		| { mode: "pointer" }
		| { mode: "targets"; targets: DesiredGsiState[] };
};
```

The baseline supports `full` and `tombstone`. A future extension adds `pointer`. A later extension
can add `targets` for source-derived GSI keys and projections. The protocol must not make a complete
image mandatory in every payload variant.

The operation compares `revision` with the stored revision. A newer revision replaces the latest
state and gets a new local `change_seq`. An equal revision is an idempotent replay. An older revision
returns a stale acknowledgement and changes no state.

A stale acknowledgement is successful for export cleanup. It proves that the materialization layer
already holds a newer image.

A `pointer` payload tells the materialization layer to read the newest state from the base table. A
returned image can be newer than the pointer revision. This is valid for latest-image semantics.

A `targets` payload carries complete desired states for the active GSIs. It does not reconstruct a
partial base image in the materialization layer.

### 4.5 Materialization storage

The materialization partition keeps one latest row for each base item:

```sql
CREATE TABLE latest_items (
    base_hk         BLOB    NOT NULL,
    base_sk         BLOB    NOT NULL,
    revision        INTEGER NOT NULL,
    change_seq      INTEGER NOT NULL,
    deleted         INTEGER NOT NULL,
    data_kind       INTEGER,
    ttl_at          INTEGER,
    data             ANY,
    PRIMARY KEY (base_hk, base_sk)
) WITHOUT ROWID, STRICT;

CREATE INDEX latest_items_by_change
    ON latest_items (change_seq, base_hk, base_sk);
```

The base owner assigns `revision`. The materialization partition assigns `change_seq` when it
accepts a newer revision. A public subscription uses `change_seq` as its local cursor order.

A deletion remains as a row with `deleted = 1`. A later recreation replaces that row with a live
image and a newer revision.

The baseline stores the complete encoded image for every live row. A new GSI can therefore derive
its keys and projection without a base-table scan. The materialization layer applies all active GSI
plans to the same image.

A future pointer mode must record `latest_revision` and `full_image_revision` separately. The layer
must not present an older complete image as the image of a newer pointer revision.

The materialization partition keeps separate progress for each GSI:

```sql
CREATE TABLE gsi_item_state (
    index_id               TEXT    NOT NULL,
    base_hk                BLOB    NOT NULL,
    base_sk                BLOB    NOT NULL,

    applied_revision       INTEGER,
    applied_index_hk       BLOB,
    applied_index_sk       BLOB,

    desired_revision       INTEGER NOT NULL,
    desired_index_hk       BLOB,
    desired_index_sk       BLOB,
    desired_projection     BLOB,
    desired_present        INTEGER NOT NULL,

    inflight_revision      INTEGER,
    inflight_index_hk      BLOB,
    inflight_index_sk      BLOB,
    inflight_projection    BLOB,
    inflight_phase         TEXT,
    inflight_deadline      INTEGER,

    status                 TEXT    NOT NULL,
    next_attempt_at        INTEGER,
    last_error_code        TEXT,
    violation_revision     INTEGER,

    PRIMARY KEY (index_id, base_hk, base_sk)
) WITHOUT ROWID, STRICT;

CREATE INDEX gsi_item_state_due
    ON gsi_item_state (index_id, status, next_attempt_at);
```

The applied state needs the old GSI key and revision. It does not need the complete old base image.
The desired state holds the newest derived GSI target. A new image overwrites this state.

The in-flight state holds one immutable transition. The retry fields let the scheduler find due work
without a full join between `latest_items` and `gsi_item_state`.

### 4.6 GSI storage

Each GSI is an independent FokosDB shard group. It has its own roots, partitions, repartitions, and
query router.

A GSI key does not have to be unique. The physical GSI key adds the base key as a hidden suffix:

```text
GSI hash key = encodeTyped(GSI partition key)

GSI sort key = tuple(
    encodeTyped(GSI sort key),
    base hash key,
    base sort key
)
```

When the GSI has no public sort key, the physical sort key contains only the base key suffix. The
suffix makes one physical entry unique for one base item.

A live GSI entry stores:

- The base item identity.
- The base revision.
- The projected attributes.
- The live or tombstone state.
- The latest operation deadline that can affect the entry.

A GSI query excludes tombstone entries. A public sort-key condition applies only to the public GSI
sort-key prefix. The query cursor includes the GSI identity and definition version.

In the baseline, the materialization partition derives this desired state from the complete image:

```ts
type DesiredGsiState = {
	indexId: string;
	definitionVersion: number;
	present: boolean;
	indexHashKey?: KeyBytes;
	indexSortKey?: KeyBytes;
	projection?: ProjectedWireRow;
};
```

A missing GSI partition key makes the item absent from that GSI. A key type or size violation can
fail later when the materialization layer writes to a GSI partition. The system reports the failure
through an error log or metric. The failure does not reject the committed base write.

A future extension can periodically distribute the GSI configuration to all base partitions. The
partitions can then validate GSI keys during a base write and reject an invalid write. This
validation is not part of the baseline.

TODO: Define the GSI key expression types, ordered numeric encoding, projection API, and durable
handling of an asynchronous key violation.

### 4.7 GSI maintenance state machine

The materialization ingestion transaction derives each desired GSI state from the complete image.
It updates `gsi_item_state.desired_*` with the newest revision. The GSI maintainer compares the
applied state with the desired state. When no transition is active, it freezes the desired target.

```text
IDLE
  -> WRITE_NEW
  -> DELETE_OLD
  -> COMMIT_APPLIED
  -> IDLE
```

Before the first outbound operation, the maintainer writes the target revision, target key,
projection, deadline, and phase. The in-flight target is immutable.

Assume that the GSI has applied image `A` at revision 10. The materialization item contains image `C`
at revision 30. The maintainer writes `C`, deletes `A`, then records `C` as applied.

When images `B`, `C`, and `D` arrive before a transition starts, the maintainer can apply `A -> D`.
When `A -> B` has started, the maintainer must finish `A -> B`. It can then apply `B -> D`.

A new latest image updates only `latest_items`. It must not change `gsi_item_state.inflight_*`.

The maintainer deletes a `gsi_item_state` row only when the index no longer needs its revision fence.
TODO: Define the exact cleanup condition for a sparse item with no applied entry.

### 4.8 GSI key movement and eventual consistency

A move from GSI key `A` to GSI key `C` uses this order:

1. The maintainer writes the live entry under `C`.
2. The maintainer confirms the write under `C`.
3. The maintainer writes a tombstone under `A`.
4. The maintainer confirms the tombstone under `A`.
5. The maintainer records `C` as the applied state.

When the maintainer stops after step 1, both entries can be visible. The durable phase resumes and
writes the tombstone under `A`. The GSI converges to one live entry.

The write-first order prefers a temporary duplicate to a temporary omission. A GSI query is
eventually consistent with the base table.

When the API must prevent a temporary duplicate, the move must use a distributed transaction across
the old and new GSI partitions.

When the old and new entries use one GSI partition, that partition can apply the move in one local
transaction.

### 4.9 Delayed-operation fences

A durable phase does not stop an old network operation from arriving late. Each GSI write and delete
therefore carries the base item identity, base revision, and operation deadline.

A GSI partition applies an operation only when both conditions are true:

- The operation deadline has not passed.
- The incoming revision is equal to or newer than the stored revision at that physical entry.

A delete writes a tombstone at the physical old entry. The tombstone keeps the revision fence after
the live entry disappears.

The maintainer writes the deadline before it sends an operation. A later transition retains the old
entry tombstone until every older operation deadline and the clock-skew allowance have passed.

This sequence also handles an item that returns to an earlier GSI key:

```text
A at revision 10
A -> B at revision 20: tombstone A at revision 20
B -> A at revision 30: replace tombstone A with live A at revision 30
```

TODO: Define the operation deadline, the clock-skew allowance, and the tombstone cleanup interval.

### 4.10 Materialization repartition

A materialization repartition moves one complete logical unit for each affected base item:

- The `latest_items` row.
- Every `gsi_item_state` row.
- Every immutable in-flight target.
- Every phase and operation deadline.

At cutover, the source stops new work for the moved item. The target imports the complete unit and
replays an in-flight phase from its start. The GSI revision fences make the replay idempotent.

The source rejects or forwards an old work claim after cutover. The target starts new work only
after its import completes.

The source must retain the latest-stream cursor history that a subscription can still reference.
TODO: Define the cursor and retention protocol across a materialization repartition.

### 4.11 Latest-image subscriptions

A subscription holds one cursor for each materialization partition or partition epoch. It reads one
bounded watermark interval:

1. The reader gets the materialization partition high watermark `H`.
2. The reader scans rows where `cursor < change_seq AND change_seq <= H`.
3. The reader returns the latest live image or tombstone from each row.
4. The reader advances its cursor to `H` after it drains the interval.

When an item changes during the scan, its row gets a new `change_seq` above `H`. The next interval
returns the new image.

The delivery model is at least once. A subscriber identifies a delivered state with the base item
identity and base revision. A retry can return the same revision again.

A subscription does not keep a per-item applied image in the materialization layer. The per-item
applied state belongs only to a GSI. Each subscription keeps its independent partition cursors.

A sharded ready directory can publish each materialization partition high watermark. A ready record
contains no payload. The materialization partition remains the source of truth.

TODO: Define subscription creation, retention, leases, push delivery, pull delivery, and cursor
expiration.

### 4.12 Online GSI creation

A new GSI uses `latest_items` as its backfill source. It does not scan the base partition topology.

For each materialization partition, the creator uses this sequence:

1. It records a high watermark `H`.
2. It scans all current `latest_items` rows.
3. It creates GSI state and applies each eligible live row.
4. It processes rows whose `change_seq` is above `H`.
5. It catches up to a recorded activation watermark.
6. It reports the materialization partition as ready.

The control plane keeps the GSI hidden until all materialization partitions report readiness. A
concurrent item update either appears in the full scan or in the catch-up scan. A revision fence
prevents an older image from replacing a newer GSI state.

A table catalog stores the immutable GSI definition, definition version, and lifecycle state.
TODO: Define catalog rollout, GSI deletion, key violations, and the exact activation state machine.

### 4.13 Failure and recovery

#### Base partition stops before export

The export outbox remains durable. The source alarm repeats the export or ready notification.

#### Base item crosses multiple partitions before export

Each old source can export its image later. The materialization layer keeps only the greatest base
revision.

#### Materialization ingestion repeats

An equal revision is an idempotent replay. An older revision changes no state.

#### GSI maintainer stops during a move

The durable phase identifies the next operation. The maintainer repeats the operation with the same
revision and target.

#### A stale GSI operation arrives after a newer transition

The newer live entry or tombstone rejects the older revision. An expired operation also fails.

#### A materialization partition stops during import

The target repeats the import page and its in-flight phases. The import must checkpoint each bounded
page in the same transaction as the imported state.

### 4.14 Concurrency

A base partition can replace an outbox row while its previous revision is in flight. The revision
comparison on acknowledgement protects the newer row.

A materialization partition can accept a newer image while a GSI transition is in flight. The
latest row changes, but the frozen in-flight target does not change.

One worker owns a given GSI item transition at a time. A durable lease or the single-threaded
materialization owner enforces this rule.

TODO: Decide whether `MaterializationDO` sends GSI operations or separate `GsiMaintainerDO`
instances claim work.

### 4.15 Performance and storage

The common base write adds one mutation-clock update and one local marker upsert. It does not copy
the complete image into the outbox. It does not wait for the materialization layer or a GSI.

The export outbox has at most one row for each dirty base item in one old owner. A sequence of local
updates to one item does not add rows. The exporter reads the complete image only when it sends a
live revision.

The materialization layer stores one latest image for each base item. This duplicates the current
base item data. It does not store one image for each mutation or subscription.

The complete-image baseline sends the complete item for each exported live revision. In return, a
new GSI backfill reads only the materialization shard group. It does not read the base shard group.

Each GSI stores its entries and applied state. The applied state stores keys, revisions, and an
optional in-flight projection. It does not store the complete previous base image.

Every operation uses bounded pages and batches. The design does not need a payload prefetch buffer
that grows with the backlog.

The first payload optimization is pointer mode. Source-derived GSI targets come after pointer mode.

TODO: Measure write amplification, storage amplification, GSI convergence latency, stream
delivery latency, and new-GSI backfill cost.

### 4.16 Backpressure and admission

#### 4.16.1 Acknowledgement boundary

The materialization layer acknowledges a base export after it stores the latest state and all
desired GSI states. It must not wait for a GSI write.

```text
Base write
  -> base item and export marker commit
  -> materialization latest image and desired GSI states commit
  <- acknowledgement to the base partition
  -> asynchronous GSI writes
```

This boundary prevents a GSI outage from immediately blocking base exports. The base partition can
remove its marker after an equal or newer materialization revision is durable.

#### 4.16.2 Coalesced GSI pressure

During a GSI outage, one GSI item state can hold four logical revisions:

```text
latest base image:       D, revision 40
desired GSI state:       D, revision 40
in-flight GSI state:     B, revision 20
applied GSI state:       A, revision 10
```

A new base image updates the latest and desired states. It does not change the immutable in-flight
state or the applied state.

When the GSI recovers, the maintainer completes `A -> B`. It can then skip intermediate images and
apply `B -> D`.

When no transition is in flight, any number of updates to one item keep one latest row and one dirty
GSI state. When a transition is in flight, the item also keeps one immutable target.

#### 4.16.3 Growth from new items

A new base item creates a new `latest_items` row. A GSI outage therefore does not give a fixed total
storage bound.

Materialization storage grows with:

```text
current base item count
+ retained deletion tombstone count
+ dirty GSI item state count
```

The materialization shard group must split as this state grows. It must not wait for the unavailable
GSI before it starts or completes a split.

#### 4.16.4 Retry control and circuit breaking

A transient GSI failure sets a retry deadline. The scheduler uses exponential backoff, jitter, and a
bounded work step. It must not retry continuously.

The circuit breaker must open before the maintainer freezes new in-flight targets. While the circuit
is open, new base revisions continue to replace the desired state. The maintainer starts with the
newest desired state when the circuit closes.

The circuit state can apply to one GSI or one destination GSI partition. Section 4.18.9 keeps the
scope and retry policy open.

#### 4.16.5 Permanent GSI violations

A permanent key violation must not block other items. The GSI item state records the error code and
violating revision. The system emits an error log or metric once for that revision.

The scheduler must not retry the same permanent violation continuously. A newer desired revision
supersedes the violation. When the newer revision is valid, the maintainer resumes normal GSI
maintenance.

Section 4.18.10 keeps activation with violation records open.

#### 4.16.6 Pressure propagation

Backpressure crosses the durable layers in this order:

```text
GSI unavailable
  -> dirty GSI states grow by distinct affected item
  -> materialization partitions split
  -> materialization persistence failure stops export acknowledgements
  -> base export markers grow by distinct affected item
  -> base partitions split
  -> insufficient base storage headroom rejects base writes
```

A GSI failure alone must not reject materialization ingestion. It increases dirty item count, dirty
age, retry state, and convergence delay.

When a materialization partition cannot persist a newer image, it rejects the export. The base
partition keeps its marker and can replace it with a newer revision.

When a base partition cannot preserve both the item and its marker, it must reject the item write.
It must not commit an item mutation and omit its export marker.

The system must not drop a latest image to reduce pressure. Section 4.18.11 keeps the storage
headroom and admission thresholds open.

#### 4.16.7 Repartition during pressure

A materialization repartition must move dirty, in-flight, retry, and violation state. The migration
unit in section 4.10 includes:

- The latest image.
- The desired GSI state.
- The applied GSI state.
- The immutable in-flight state.
- The retry deadline.
- The violation state.

The target repeats the in-flight phase with the same revision and deadline. A GSI outage must not
block this migration.

A base source keeps an unacknowledged live marker and the referenced item image. A future pointer
probe can send only the key and revision first. When the materialization layer already has a newer
revision, it can acknowledge the marker without receiving the stale complete image.

#### 4.16.8 Recovery admission

After an outage, the maintainer must limit recovery traffic. Each work step has bounds for:

- Concurrent GSI writes.
- Items.
- Bytes.
- Writes to one destination GSI partition.

The maintainer can increase its credits while the GSI remains healthy. It reduces the credits after
an overload or transient error.

The scheduler must share recovery capacity across materialization partitions. One old backlog must
not consume all available GSI capacity.

#### 4.16.9 Metrics

The base export path reports:

- Dirty marker count.
- Oldest unacknowledged marker age.
- Export attempts and failures.
- Exported bytes.
- Stale revision acknowledgements.
- Marker storage bytes.

The materialization layer reports:

- Latest image count and bytes.
- Dirty item count for each GSI.
- Oldest dirty item age.
- Desired-to-applied lag.
- In-flight transition count.
- Ingestion failures.
- Storage headroom.

The GSI path reports:

- Successful live writes and tombstones.
- Transient failures by error code.
- Violations by index and error code.
- Open circuit count.
- Recovery write rate.
- Convergence delay.
- Temporary duplicate duration.

A revision difference is not a sufficient aggregate lag metric. Base revisions are local to item
lineages. Dirty age and dirty item count are the primary aggregate lag metrics.

#### 4.16.10 Public subscription pressure

A slow latest-image subscription must not block GSI maintenance or base writes. The subscription
keeps its own partition cursors. It does not create one payload copy for each update.

A slow subscription can delay deletion-tombstone cleanup. The stream must define a retention period,
cursor expiration, and a resnapshot path. A public subscription can expire. A GSI is a permanent
internal materialization target and must not expire because of lag.

### 4.17 Deployment, rollback, and testing

FokosDB has no deployed schema compatibility requirement in the current project design. The final
agent plan must define the migration order before implementation starts.

TODO: Define deployment, rollback, model checks, failure injection, repartition tests, GSI tests,
and latest-image subscription tests.

### 4.18 Open questions

#### 4.18.1 Pointer-mode semantics

The baseline stores the complete encoded latest image in `latest_items`. This avoids a base read
during GSI maintenance, stream delivery, and online GSI creation.

The first payload extension adds pointer mode. A pointer reduces transfer and duplicate storage. It
adds a base-table read and revision check to each consumer. The final design must define pointer
resolution, read-through revision advancement, and an optional inline-size threshold.

#### 4.18.2 Temporary duplicates

The current direction permits a temporary duplicate during a cross-partition GSI key move. The final
GSI contract must state whether this is acceptable.

When it is not acceptable, the implementation must use a distributed transaction for the move.

#### 4.18.3 GSI worker ownership

A materialization partition can drive its own GSI state machines. Separate maintainer partitions can
also claim the work. The choice changes split behavior, leases, RPC fan-out, and independent scaling.

#### 4.18.4 Revision representation

The current direction uses a numeric mutation clock that a target initializes from its source high
watermark. The final design must define the SQL type, overflow behavior, and merge behavior.

#### 4.18.5 Materialization stream cursors during repartition

The final design must specify how a subscription finishes a source epoch and starts its target
epochs. It must also specify how long the source retains rows for old cursors.

#### 4.18.6 GSI entry tombstone cleanup

The final design must select the operation deadline and cleanup allowance. The allowance must cover
all old operations that can still arrive.

#### 4.18.7 GSI definition and key model

The final design must define key expressions, key types, numeric order, projection modes, the public
query API, and the durable handling of asynchronous key violations.

#### 4.18.8 First release scope

The final design must decide whether the first release supports online GSI creation. A static GSI
created with the table has no online catalog rollout.

#### 4.18.9 Circuit breaker scope and retry policy

The final design must select the circuit scope, backoff policy, jitter policy, and recovery probe.
The scope can be one GSI or one destination GSI partition.

#### 4.18.10 GSI activation with violations

The final design must decide whether permanent violation records block a GSI from becoming `ACTIVE`.
It must also define how the control plane reports the violation count.

#### 4.18.11 Storage headroom and admission thresholds

The final design must define when a materialization partition stops export acknowledgements. It must
also define when a base partition rejects an item write to preserve its export marker.

## 5. Alternative options

### 5.1 Lossless GSI transition log

Each base partition can store every old-to-new GSI transition. A GSI consumer then applies all
transitions in source order.

This model does not need per-item applied state. An outage makes the log grow with the mutation
count. It also needs lineage order across base repartitions. The proposed design instead keeps the
latest image and per-GSI applied state.

### 5.2 Base-local latest state

Each base partition can keep the latest stream state beside its base items. The state then moves
each time the base item moves.

Old partitions can still hold unexported states. A consumer must merge those states in revision
order. The proposed independent materialization layer gives one stable logical owner for the latest
state.

### 5.3 Distributed transaction for each GSI move

A coordinator can atomically delete the old GSI entry and write the new entry. This prevents a
temporary duplicate or omission.

The transaction adds coordination to each cross-partition key move. The proposed state machine uses
local idempotent operations and accepts temporary GSI staleness.

## 6. Frequently asked questions

### 6.1 Do the GSI and the latest-image stream use one primitive?

Yes. Both use the coalesced materialization item. The stream reads the latest state. Each GSI also
keeps its independent applied state and in-flight transition.

### 6.2 Why is the last applied state per GSI?

Different GSIs can progress at different rates. One failed GSI must not prevent another GSI from
advancing its applied state.

### 6.3 Why is there no last applied image for each public subscription?

A subscription only needs a cursor for each materialization partition. A per-item subscription row
would make storage grow with the product of items and subscriptions.

### 6.4 Can an old base partition overwrite a new image after three splits?

No. The materialization layer accepts only a newer base revision. An old source can complete its
export, but its stale revision changes no materialization state.

### 6.5 Can a GSI contain two entries for one base item?

It can contain a temporary old and new entry during propagation. The durable transition must remove
the old entry. A revision fence prevents a delayed old operation from restoring it.

### 6.6 When does a GSI move need two-phase commit?

It needs two-phase commit when the API must prevent a temporary duplicate or omission. Eventual
final correctness does not need an atomic cross-partition move.

### 6.7 Does the materialization layer keep every deletion?

No. A deletion is the latest image until a newer recreation replaces it. The layer does not keep a
complete mutation history.

### 6.8 Can a new GSI avoid a base-table backfill?

Yes. It scans all current `latest_items` rows and then catches up through `change_seq`. This works
only when the materialization layer already contains all base items.

### 6.9 Why does the baseline send complete images?

A complete image lets the materialization layer apply all current GSIs and backfill a new GSI. A new
GSI does not need to coordinate a scan across the base partition topology.

### 6.10 Which payload optimization comes first?

Pointer mode comes first. Source-derived GSI targets come after pointer mode.

### 6.11 What happens when a GSI key is invalid?

The baseline accepts the base write. The asynchronous GSI write reports the violation in an error
log or metric. A future extension can propagate GSI definitions to base partitions and reject an
invalid base write.

## 7. References

- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/shared/partition/migration.ts`
- `docs/agent-plans/2026-09-17-unified-repartition-flow.md`
- `docs/ideas/fokos-sharding/2026-09-09-fokos-partition-runtime.md`
- [Using Global Secondary Indexes in DynamoDB][dynamodb-gsi]
- [Change data capture for DynamoDB Streams][dynamodb-streams]
- [Scribe: How Meta transports terabytes per second in real time][scribe]
- [Durable Objects limits][durable-objects-limits]
- [Workers limits][workers-limits]

[dynamodb-gsi]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
[dynamodb-streams]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
[scribe]: https://www.vldb.org/pvldb/vol18/p4817-karpathiotakis.pdf
[durable-objects-limits]: https://developers.cloudflare.com/durable-objects/platform/limits/
[workers-limits]: https://developers.cloudflare.com/workers/platform/limits/
