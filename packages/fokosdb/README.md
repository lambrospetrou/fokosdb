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
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
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

## You must re-export the Durable Object classes

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
			{ "name": "PARTITION_DO", "class_name": "PartitionDO" },
			{ "name": "TRANSACTION_COORDINATOR_DO", "class_name": "TransactionCoordinatorDO" },
		],
	},
	"migrations": [{ "tag": "v1", "new_sqlite_classes": ["PartitionDO", "TransactionCoordinatorDO"] }],
}
```

Subclassing `PartitionDO` works, and a subclass needs its own binding, its own entry in
`new_sqlite_classes`, and its own re-export.

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

| Command                 | Purpose                                                                                                        |
| ----------------------- | -------------------------------------------------------------------------------------------------------------- |
| `pnpm build`            | Build `dist/client` and `dist/server` with tsdown, which also reports entry sizes and guards the client bundle |
| `pnpm test`             | Typecheck, key invariants, then vitest inside real `workerd`                                                   |
| `pnpm lint:pkg`         | `publint` on the packaged output                                                                               |
| `pnpm cf-typegen`       | Regenerate `worker-configuration.d.ts`                                                                         |
| `pnpm bench:expression` | Expression engine benchmarks                                                                                   |

`wrangler.jsonc` here is never deployed. It gives vitest and `wrangler types` an entry point
(`test/worker-entry.ts`) that exports the library Durable Objects.
