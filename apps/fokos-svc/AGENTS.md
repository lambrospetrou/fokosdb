# FokosDB

FokosDB is a globally strongly-consistent key-value database built on Cloudflare Durable Objects, inspired by DynamoDB's API and transaction model. It is a library/service.

## Critical tips

- Always run tests `npm run test` in a subagent to not pollute the context with the verbose output.
- Your knowledge of Cloudflare Workers APIs and limits may be outdated. Always retrieve current documentation before any [Workers](https://developers.cloudflare.com/workers/) and [Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/) tasks. For all limits and quotas, retrieve from the product's `/platform/limits/` page. eg. `/workers/platform/limits`.

## Commands

| Command               | Purpose                   |
| --------------------- | ------------------------- |
| `npx wrangler dev`    | Local development         |
| `npx wrangler deploy` | Deploy to Cloudflare      |
| `npx wrangler types`  | Generate TypeScript types |

Run `wrangler types` after changing bindings in wrangler.jsonc.

## Architecture

Two Durable Object classes do all the work:

- **`PartitionDO`** (`src/lib/do-partition.ts`) — stores items in SQLite. One DO per partition shard. Handles single-item reads/writes and participates in 2PC as a transaction resource manager. Automatically splits into child partitions when storage thresholds are met.
- **`TransactionCoordinatorDO`** (`src/lib/do-transaction-coordinator.ts`) — one DO per write transaction (named by idempotency token). Drives 2-phase commit across multiple PartitionDOs. Ephemeral for read transactions.

The `FokosDB` class (`src/lib/db.ts`) is the client-side entry point. It routes requests to the correct partition using `PartitionTopologyRouterImpl` and delegates transactions to `TransactionCoordinatorDO`.

### Data Model

Items are keyed by `hashKey` (required) + `sortKey` (optional, defaults to `""`). Data is `Uint8Array | string`. Items have a `version` counter (incremented on every write) and an optional TTL.

## Partition Topology & Routing

- At startup, `rootTreesN` root partitions are created (e.g. 10).
- Routing uses hashing to map `hashKey` to a root partition index.
- Partition IDs are opaque hex-encoded bytes and encodes the data partition location in the entire partitions topology. The opaque partition ID should only be accessed through the `PartitionIdHelper` class.
- **`PartitionContext` is passed in every RPC call** — DOs cannot be configured at instantiation time in Workers RPC, so the topology config (splitN, ns, databaseName, etc.) travels with every request. The DO validates the context matches its stored one.
- The `PartitionTopologyRouterImpl` is used by the client (`FokosDB`) to pick partitions. `PartitionTopologyImpl` is used inside the DOs for split management.

## Partition Splitting

When a PartitionDO's SQLite size exceeds `hashSplitConditions.maxSizeMb`, it queues a hash split:

1. **`split_queued`**: After a write, `maybeQueueSplit` detects the threshold and queues. An alarm fires.
2. **`split_started`**: `startSplit` initializes `N` child DOs via `initFromSplit`. The parent becomes a forwarding proxy. Children begin migrating data in background.
3. Child migration: children call `getItemsBatch` + `getPartitionTransactionMetadata` on the parent via paginated RPC batches (~20 MB per batch). The parent filters only rows belonging to that child using the same hash function.
4. **`split_completed`**: Once all children acknowledge migration complete, the parent transitions. Reads during migration go directly to parent (`getItemDirect`). Writes are rejected with a 503 during migration.

**Critical**: `splitN` must NOT change after initialization — it would break routing and cause data loss.

## Transaction Protocol (2PC)

Modeled after the [_"Distributed Transactions at Scale in Amazon DynamoDB"_ USENIX ATC 2023 paper (Idziorek et al.)](https://www.usenix.org/system/files/atc23-idziorek.pdf) and the [_Amazon DynamoDB: A Scalable, Predictably Performant, and Fully Managed NoSQL Database Service_ USENIX ATC 2022 paper (Elhemali et al.)](https://www.usenix.org/system/files/atc22-elhemali.pdf).

**Write transactions (`transactWriteItems`)**:

- TC state machine: `CREATED → PREPARING → PREPARED → COMMITTING → COMMITTED` (or `→ CANCELLING → CANCELLED`)
- Every state transition writes to SQLite **before** sending outbound RPCs (write-ahead).
- `PREPARED` is the point of no return — a PREPARED transaction MUST eventually commit.
- Conflict detection: `last_transaction_ts` column on items; `max_deleted_ts` in `deletion_metadata` for items that were deleted.
- Non-transactional writes (`putItem`/`deleteItem`) are **rejected** (not delayed) if a pending transaction holds the item's lock.
- TC recovery: PartitionDO alarms poke stale TCs via `recoverTransaction()`; TC alarm retries stale in-flight transactions.
- Idempotency: `clientRequestToken` → TC DO name = idempotency token.

**Read transactions (`transactGetItems`)**:

- Two-phase double-read: read once, check no pending writes, read again, compare `lastCommittedTs`. If anything changed → abort.
- Ephemeral TC (random UUID name). No SQLite state persisted. If TC crashes mid-read, client retries.

**Key invariants**:

1. `items` table always contains committed state only.
2. `pending_transactions` holds locks for in-flight transactions.
3. `prepare`, `commit`, `cancel` are all idempotent.
4. TC never transitions from PREPARED to CANCELLING.

## Testing

Tests run in the actual Cloudflare Workers runtime via `@cloudflare/vitest-pool-workers`. Each test suite creates isolated namespaces using `crypto.randomUUID()` prefixes. Integration tests are in `test/transactions.test.ts`; partition/topology tests in `src/lib/do-partition.test.ts`.
