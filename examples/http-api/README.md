# Example — HTTP API worker

A deployable Worker that exposes any number of FokosDB tables through a REST API, built with Hono
and validated with valibot. It imports **both** entry points: `fokosdb/client` for the request path
and `fokosdb/server` for the Durable Object classes.

## Running it inside this repo

```sh
pnpm install          # from the repo root
pnpm build            # build the library first — the example imports dist/, not src/
pnpm --filter "@fokosdb-example/http-api" dev
pnpm --filter "@fokosdb-example/http-api" test
```

`pnpm dev` needs a `FOKOS_API_TOKENS` secret. Copy `.dev.vars.example` to `.dev.vars` and set a
comma-separated token list. The vitest suite supplies its own token through the miniflare bindings in
`vitest.config.ts`, so `pnpm test` runs without a `.dev.vars` file.

## Running it outside this repo

`package.json` declares the library as `"fokosdb": "workspace:*"`. That specifier only resolves
inside this pnpm workspace. To lift this example into its own repository, replace it with a real
published range:

```jsonc
{
	"dependencies": {
		"fokosdb": "^0.1.0",
	},
}
```

## Durable Objects

`index.ts` re-exports the classes because wrangler resolves bindings against the Worker's own entry
module:

```ts
export { PartitionDO, TransactionCoordinatorDO } from "fokosdb/server";
```

`CustomPartitionDO` is a `PartitionDO` subclass, declared here to exercise the subclassing path. It
carries its own binding and its own entry in the `new_sqlite_classes` migration, exactly as any
subclass must.

## HTTP surface

| Method   | Path                             | Purpose                                                                                    |
| -------- | -------------------------------- | ------------------------------------------------------------------------------------------ |
| `GET`    | `/api/hello/:name`               | Health check behind the auth middleware                                                    |
| `POST`   | `/api/rpc/:tableName/:rpcAction` | `putItem`, `getItem`, `deleteItem`, `queryItems`, `transactWriteItems`, `transactGetItems` |
| `DELETE` | `/api/databases/:tableName`      | Destroy every partition of a table                                                         |

Every request needs an `x-fokos-secret-token` header matching one of `FOKOS_API_TOKENS`.

`test/hurl/` holds end-to-end suites that run against a live deployment; see the `test:hurl` scripts
in `package.json`.
