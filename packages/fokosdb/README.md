# fokosdb

FokosDB is a globally strongly-consistent key-value database built on Cloudflare Durable Objects,
with an API and transaction model modelled on DynamoDB.

> [!CAUTION]
> **Do NOT use this in production, yet.** Breaking changes are still landing.

## Install

The package is not on npm yet. Every commit on `main` is published as an installable preview build,
so pin one by its commit SHA:

```sh
npm install https://pkg.pr.new/lambrospetrou/fokosdb/fokosdb@7527ec6
```

The URL of each build is posted as a commit comment. A preview build is removed six months after it
is published, or one month after its last download, so re-pin a newer build from time to time.

## Two subpath imports

The package publishes exactly two entry points. There is no bare `fokosdb` import.

| Import           | What it gives you                                                          | Where it runs              |
| ---------------- | -------------------------------------------------------------------------- | -------------------------- |
| `fokosdb/client` | `FokosDB`, `PartitionTopologyRouterImpl`, `PartitionContextCreator`, types | Your Worker's request path |
| `fokosdb/server` | `PartitionDO`, `TransactionCoordinatorDO`                                  | The Durable Objects        |

Both entries run inside `workerd`. "Client" means the Worker-side caller that routes requests to the
partitions — not a browser. Only `fokosdb/server` carries the Durable Object implementations, so a
Worker that talks to an already-deployed FokosDB deployment can import `fokosdb/client` alone.

```ts
import { FokosDB, PartitionContextCreator, PartitionTopologyRouterImpl } from "fokosdb/client";

const partitionContext = PartitionContextCreator.create({
	tableName: "my-table",
	ns: "PARTITION_DO",
	nsTx: "TRANSACTION_COORDINATOR_DO",
	rootTreesN: 10,
	hashSplitN: 4,
	hashSplitConditions: { maxSizeMb: 1000 },
});

const db = new FokosDB({
	topology: new PartitionTopologyRouterImpl(partitionContext),
	transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
});

await db.putItem({ hashKey: "user#1", sortKey: "profile", data: "hello" });
const result = await db.getItem({ hashKey: "user#1", sortKey: "profile" });
```

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

`src/` splits three ways. `client/` and `server/` are the two build entries; `shared/` holds
everything both sides use and is not published on its own — tsdown compiles it into whichever entry
reaches it, so a consumer never sees it.

```
src/
  client/   FokosDB and the routing surface
  server/   the Durable Object classes
  shared/   key codec, expressions, partition topology, transaction types, …
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
