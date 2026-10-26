# fokosdb

FokosDB is a globally strongly-consistent key-value database built on Cloudflare Durable Objects,
with an API and transaction model modelled on DynamoDB.

> [!CAUTION]
> **Do NOT use this in production, yet.** Breaking changes are still landing.

## Install

Every commit on `main` is published as an installable preview build. Pin one by its commit SHA:

```sh
npm install https://pkg.pr.new/lambrospetrou/fokosdb@afce6cb
```

`@main` always resolves to the newest build on `main`. It is convenient, but it is a moving pointer,
so an install is not reproducible:

```sh
npm install https://pkg.pr.new/fokosdb@main
```

A preview build reports its version as `0.0.0-preview-<sha>`. It is removed six months after it is
published, or one month after its last download.

If you want the manually published version on NPM:

```sh
npm install fokosdb@dev
```

## Subpath imports

The package publishes four entry points. There is no bare `fokosdb` import. To use FokosDB, you need
only `fokosdb/client` and `fokosdb/server`.

| Import                    | What it gives you                                                 | Where it runs              |
| ------------------------- | ----------------------------------------------------------------- | -------------------------- |
| `fokosdb/client`          | `FokosDB`, its option and error types                             | Your Worker's request path |
| `fokosdb/server`          | `PartitionDO`, `TransactionCoordinatorDO`                         | The Durable Objects        |
| `fokosdb/sharding/client` | `FokosShardingClient`, routing types and errors                   | Your Worker's request path |
| `fokosdb/sharding/server` | `FokosShardingRuntime`, for your own sharded Durable Object class | Your Durable Objects       |

Every entry runs inside `workerd`. "Client" means the Worker-side caller that routes requests to the
partitions — not a browser. Only `fokosdb/server` carries the Durable Object implementations, so a
Worker that talks to an already-deployed FokosDB deployment can import `fokosdb/client` alone.

```ts
import { FokosDB } from "fokosdb/client";

const db = new FokosDB({
	// The identity of the table. Never change these values after the table has data.
	table: {
		name: "my-table",
		ns: "FOKOSDB_PARTITION_DO",
		nsTx: "FOKOSDB_TRANSACTION_COORDINATOR_DO",
		rootTreesN: 10,
		hashSplitN: 4,
	},
	// These options can change between deploys.
	hashSplitConditions: { maxSizeMb: 1000 },
});

await db.putItem({ hashKey: "user#1", sortKey: "profile", data: "hello" });
const result = await db.getItem({ hashKey: "user#1", sortKey: "profile" });
```

`FokosDB` gets the Durable Object bindings from the `env` of `cloudflare:workers`, so you can create
the client one time at module scope or for each request.

### The identity of a table

The options in `table` select the Durable Objects of the table. Every client of the table must give
the same values, and the values must never change after the table has data.

| Option              | Required | What it selects                                                                         |
| ------------------- | -------- | --------------------------------------------------------------------------------------- |
| `name`              | Yes      | The names of the Durable Objects. It must not start with `fokos.`.                      |
| `ns`                | Yes      | The binding of the `PartitionDO` namespace.                                             |
| `nsTx`              | Yes      | The binding of the `TransactionCoordinatorDO` namespace.                                |
| `nsMat`             | No       | The binding of the `MaterializationDO` namespace. Required when `captureChanges` is on. |
| `rootTreesN`        | Yes      | The number of root partitions, from 1 to 65,000.                                        |
| `hashSplitN`        | Yes      | The number of children of each hash split, from 2 to 255.                               |
| `coordinatorRootsN` | No       | The number of root transaction coordinators. Default: `2 * rootTreesN`, at most 65,000. |
| `jurisdiction`      | No       | The jurisdiction of every Durable Object of the table. See the section below.           |

> [!WARNING]
> If you change `name`, `ns`, `nsTx` or `jurisdiction`, the client connects to other, empty Durable
> Objects, and the library cannot detect it. If you change `rootTreesN`, `hashSplitN` or
> `coordinatorRootsN`, the existing partitions or coordinators reject each request with
> `partition_context_mismatch`.
> In both cases the data stays in the old Durable Objects.

All other options are outside `table` and can change: `rangeSplitN`, `hashSplitConditions`,
`rangeSplitConditions`, `rangeAncestorsConfig`, `locationHint`, `limits`, `singlePartitionFastPath`,
`retry` and `partitionMigratingRetryDeadlineMs`. A change to `rangeSplitN` applies only to the range
splits that start after it. Never decrease a key size limit in `limits` after items with larger keys exist.

### Change the table options during a deploy

Each partition and coordinator stores `rangeSplitN`, `hashSplitConditions`, `rangeSplitConditions`,
`rangeAncestorsConfig`, `locationHint` and `limits` from the requests it receives. `policyVersion`
(default `0`) orders these changes:

- A request with a higher `policyVersion` replaces the stored options.
- A request with a lower `policyVersion` does not change them.
- A request with an equal `policyVersion` replaces them when they are different. The last request wins.

During a gradual deploy, two Worker versions can send requests at the same time. If they send
different options with the same version, each partition replaces its options each time the version
changes. To prevent this, increase `policyVersion` each time you change these options:

```ts
const db = new FokosDB({
	table: { name: "my-table", ns: "FOKOSDB_PARTITION_DO", nsTx: "FOKOSDB_TRANSACTION_COORDINATOR_DO", rootTreesN: 10, hashSplitN: 4 },
	hashSplitConditions: { maxSizeMb: 2000 },
	policyVersion: 1,
});
```

> [!WARNING]
> To go back to earlier options, deploy them with a higher `policyVersion`. If you roll back to a
> Worker version with a lower `policyVersion`, the partitions that received the higher version keep
> its options.

### Jurisdictions and identity hazard

A table can specify `jurisdiction: "eu" | "fedramp" | "us"` in the `table` option of `FokosDB`.
The jurisdiction restricts all partition and transaction coordinator objects of the table to that geographic or regulatory area.

- https://developers.cloudflare.com/durable-objects/reference/data-location/#supported-locations

> [!WARNING]
> **The jurisdiction is part of the identity of the table and is immutable.**
> Cloudflare gives different Durable Object IDs for different jurisdictions.
> If you add, change, or remove `jurisdiction` on an existing table:
>
> - The table connects to empty Durable Objects.
> - The previous data remains in the old Durable Objects and becomes unreachable.
> - The library cannot detect this change because the two sets of objects do not share context.

## Getting started with FokosDB

### You must re-export the Durable Object classes

Wrangler resolves a Durable Object binding against your Worker's own entry module. Importing the
classes is not enough — re-export them, or the deploy fails with an unresolved class name.

```ts
// src/index.ts
export { PartitionDO, TransactionCoordinatorDO } from "fokosdb/server";

export default {
	async fetch(request, env, ctx) {
		/* ... */
	},
} satisfies ExportedHandler<Env>;
```

Then declare the bindings and the migration:

```jsonc
{
	"durable_objects": {
		"bindings": [
			{ "name": "FOKOSDB_PARTITION_DO", "class_name": "PartitionDO" },
			{ "name": "FOKOSDB_TRANSACTION_COORDINATOR_DO", "class_name": "TransactionCoordinatorDO" },
		],
	},
	"migrations": [{ "tag": "v1", "new_sqlite_classes": ["PartitionDO", "TransactionCoordinatorDO"] }],
}
```

Subclassing `PartitionDO` works, and a subclass needs its own binding, its own entry in
`new_sqlite_classes`, and its own re-export.

### Cache the topology with Workers Cache

These steps apply to `FokosDB` only.

Without the topology cache, each request from a `FokosDB` client goes to a root partition, and the
root forwards it to the partition that owns the key. With the topology cache, the client, the
partitions, and the coordinators read the topology of the table, and send each request to the
deepest known partition.

`FokosDBAutoAdminDO` records each completed split of the table. The `FokosTopologyCache` entrypoint
serves this topology, and [Workers Cache](https://developers.cloudflare.com/workers/cache/) keeps each
answer for 10 seconds. After that, Workers Cache serves the stale answer for up to 600 seconds while it
gets a new one. A stale topology is safe: a partition forwards a request that it does not own.

You need Wrangler 4.107.0 or later, and a compatibility date of 2025-11-17 or later (or the
`enable_ctx_exports` compatibility flag).

1. Re-export `FokosDBAutoAdminDO` and `FokosTopologyCache` from the main module of your Worker:

   ```ts
   // src/index.ts
   export { PartitionDO, TransactionCoordinatorDO, FokosDBAutoAdminDO, FokosTopologyCache } from "fokosdb/server";
   ```

2. Bind the admin Durable Object, add its migration, and enable Workers Cache for the topology
   entrypoint only. The other entrypoints of your Worker keep the cache off, so their requests do not
   pay a cache lookup.

   ```jsonc
   {
   	"durable_objects": {
   		"bindings": [
   			{ "name": "FOKOSDB_PARTITION_DO", "class_name": "PartitionDO" },
   			{ "name": "FOKOSDB_TRANSACTION_COORDINATOR_DO", "class_name": "TransactionCoordinatorDO" },
   			{ "name": "FOKOSDB_ADMIN_DO", "class_name": "FokosDBAutoAdminDO" },
   		],
   	},
   	"migrations": [
   		{ "tag": "v1", "new_sqlite_classes": ["PartitionDO", "TransactionCoordinatorDO"] },
   		{ "tag": "v2", "new_sqlite_classes": ["FokosDBAutoAdminDO"] },
   	],
   	"exports": {
   		"FokosTopologyCache": { "type": "worker", "cache": { "enabled": true } },
   	},
   }
   ```

3. Give the binding of the admin Durable Object in `nsAdmin`, and the export name of the entrypoint
   in `topologyEntrypoint`:

   ```ts
   const db = new FokosDB({
   	table: {
   		name: "my-table",
   		ns: "FOKOSDB_PARTITION_DO",
   		nsTx: "FOKOSDB_TRANSACTION_COORDINATOR_DO",
   		rootTreesN: 10,
   		hashSplitN: 4,
   	},
   	nsAdmin: "FOKOSDB_ADMIN_DO",
   	topologyEntrypoint: "FokosTopologyCache",
   });
   ```

`topologyEntrypoint` needs `nsAdmin`. Keep the same `nsAdmin` for the life of the table.

> [!NOTE]
> `wrangler dev` and the vitest pool accept the `exports` map, but they do not cache. Each call runs
> the entrypoint again.

### Global secondary indexes (GSIs)

A GSI is a second view of the items of a table, with a different hash key and sort key. FokosDB
updates each GSI in the background after each write. Thus a GSI read is eventually consistent: a
query of a GSI can miss a recent write.

> [!WARNING]
> As of 2026-10-26, you can add GSIs only when you create a FokosDB table. You cannot add or
> remove a GSI after that. Support to add and remove a GSI on the fly will come later.
>
> Each materialization partition stores the GSI list of its first request, and uses only the stored
> list. If you change `fokosTargets()` later, the partitions log an error and keep the stored list.
> If you turn on `captureChanges` on a table that has data, the GSIs never get the older items.

1. Make a subclass of `MaterializationDO`, and declare the GSIs in `fokosTargets()`. The `targetId`
   is the name of the GSI. This example declares two GSIs on a table of orders:

   ```ts
   // src/index.ts
   import { MaterializationDO, type FokosTargetDefinition } from "fokosdb/server";

   export { PartitionDO, TransactionCoordinatorDO } from "fokosdb/server";

   export class OrdersMaterializationDO extends MaterializationDO {
   	protected override fokosTargets(): readonly FokosTargetDefinition[] {
   		return [
   			// The orders of a customer, sorted by `createdAt`. Each entry holds the full item.
   			{
   				kind: "gsi",
   				targetId: "byCustomer",
   				hashKey: { ref: "data", path: "$.customerId" },
   				sortKey: { ref: "data", path: "$.createdAt" },
   			},
   			// The orders of a status, with no sort key. Each entry holds only `customerId` and `total`.
   			{
   				kind: "gsi",
   				targetId: "byStatus",
   				hashKey: { ref: "data", path: "$.status" },
   				projection: [
   					{ expr: { ref: "data", path: "$.customerId" }, as: "customerId" },
   					{ expr: { ref: "data", path: "$.total" }, as: "total" },
   				],
   			},
   		];
   	}
   }
   ```

2. Bind the subclass, and add it to a migration:

   ```jsonc
   {
   	"durable_objects": {
   		"bindings": [
   			{ "name": "FOKOSDB_PARTITION_DO", "class_name": "PartitionDO" },
   			{ "name": "FOKOSDB_TRANSACTION_COORDINATOR_DO", "class_name": "TransactionCoordinatorDO" },
   			{ "name": "FOKOSDB_ORDERS_MATERIALIZATION_DO", "class_name": "OrdersMaterializationDO" },
   		],
   	},
   	"migrations": [{ "tag": "v1", "new_sqlite_classes": ["PartitionDO", "TransactionCoordinatorDO", "OrdersMaterializationDO"] }],
   }
   ```

3. Give the binding in `table.nsMat`, and set `captureChanges: true` when you create the table:

   ```ts
   const db = new FokosDB({
   	table: {
   		name: "orders",
   		ns: "FOKOSDB_PARTITION_DO",
   		nsTx: "FOKOSDB_TRANSACTION_COORDINATOR_DO",
   		nsMat: "FOKOSDB_ORDERS_MATERIALIZATION_DO",
   		rootTreesN: 10,
   		hashSplitN: 4,
   	},
   	captureChanges: true,
   });
   ```

4. Write to the table, and query each GSI with `db.gsi(targetId).queryItems`. It takes the same
   options as `db.queryItems`, and the key conditions apply to the GSI keys:

   ```ts
   await db.putItem({
   	hashKey: "order#1",
   	data: { customerId: "customer#7", status: "OPEN", createdAt: "2026-10-26T10:00:00Z", total: 42 },
   });

   // The orders of customer#7 in October 2026, newest first.
   const orders = await db.gsi("byCustomer").queryItems({
   	queries: [{ hashKey: "customer#7", sortKeyCondition: { op: "begins_with", prefix: "2026-10" }, scanIndexForward: false }],
   });
   // orders.items[0]: { hashKey: "customer#7", sortKey: "2026-10-26T10:00:00Z", baseHashKey: "order#1", data: { ... }, ... }

   // The open orders.
   const open = await db.gsi("byStatus").queryItems({ queries: [{ hashKey: "OPEN" }] });
   // open.items[0].data: { customerId: "customer#7", total: 42 }
   ```

The rules of a GSI:

- A table has at most 10 GSIs. A `targetId` is not empty, has no `~`, and is unique.
- `hashKey` and `sortKey` are expressions on the base item. Each must give a non-empty string or a
  non-empty `Uint8Array`.
- When the `hashKey` value is missing or `null`, the item has no entry in the GSI. When the GSI has
  a `sortKey` and its value is missing or `null`, the item has no entry in the GSI.
- When a key value has a different type, for example a number, the item has no entry in the GSI until
  a later write of the item gives a valid key. The materialization partition logs the failure.
- Without `projection`, an entry holds the full item. With `projection`, an entry holds a JSON object
  of the projected values. A projected value must not be a `Uint8Array`.
- Each entry has the GSI keys in `hashKey` and `sortKey`, and the keys of the base item in
  `baseHashKey` and `baseSortKey`. `version` is the version of the entry, not of the base item.
- In a `filter` or a `projection` of a GSI query, `{ ref: "sortKey" }` reads the GSI sort key.
- More than one item can have the same GSI keys. A GSI does not make a key unique.
- When the GSI key of an item changes, a query can return the old entry and the new entry for a short time.
- The GSI entries are in the `PartitionDO` namespace of the table. `db.destroy()` also deletes each GSI.

## Package layout

`src/` splits four ways. `client/` and `server/` are the FokosDB build entries. `sharding/` is the
sharding library, which uses no FokosDB module, and has the two sharding entries. `shared/` holds
everything both FokosDB sides use and is not published on its own — tsdown compiles it into whichever
entry reaches it, so a consumer never sees it.

```
src/
  client/     FokosDB and its public types
  server/     the Durable Object classes
  sharding/   the sharding library: index-client.ts and index-server.ts
  shared/     key codec, expressions, partition topology, transaction types, …
```

`xxhash-wasm`, `durable-utils` and `cloudflare:workers` stay external in the build. Resolving
`xxhash-wasm` at build time would bake in its Node loader instead of the `workerd` one it ships a
package export condition for.

## Scripts

| Command                         | Purpose                                                                                                        |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `pnpm build`                    | Build `dist/client` and `dist/server` with tsdown, which also reports entry sizes and guards the client bundle |
| `pnpm test`                     | Typecheck, key invariants, then vitest inside real `workerd`                                                   |
| `pnpm lint:pkg`                 | `publint` on the packaged output                                                                               |
| `pnpm cf-typegen`               | Regenerate `worker-configuration.d.ts`                                                                         |
| `pnpm bench:expression`         | Expression engine benchmarks                                                                                   |
| `pnpm bench:workerd expression` | Expression compile and SQLite statement times inside a `PartitionDO`                                           |
| `pnpm bench:alloc:expression`   | Heap bytes of the expression compile, in Node                                                                  |

`wrangler.jsonc` here is never deployed. It gives vitest and `wrangler types` an entry point
(`test/worker-entry.ts`) that exports the library Durable Objects.
