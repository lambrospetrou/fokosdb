# Example — typed keys

A type-only regression check for `FokosTypeOverrides`, the module-augmentation hook that lets an
application narrow `HashKey`/`SortKey` from `string | Uint8Array` to just one of the two across its
whole codebase (see the doc comment on `FokosTypeOverrides` in
`packages/fokosdb/src/shared/types.ts`).

There is no runtime code here. `narrow-keys.ts` augments `FokosTypeOverrides` to `string` and then
uses `@ts-expect-error` to assert that `Uint8Array` is rejected everywhere a key appears — `putItem`
options, `SortKeyCondition`, `FokosStd.sortKeySuccessor`. `db-operations.ts` does the same against
`FokosDB`'s six public operations (`putItem`, `getItem`, `deleteItem`, `transactWriteItems`,
`transactGetItems`, `queryItems`), on both the request and the result side, relying on the
augmentation from `narrow-keys.ts` applying to this whole package — TypeScript declaration merging is
program-wide, not file-scoped. If the narrowing ever regresses, one of those lines stops being a type
error and `tsc` fails the build.

This has to be its own package rather than a test file inside `packages/fokosdb`: module augmentation
is program-wide, so applying it inside the library's own compilation would narrow FokosDB's internal
code too, which still has to accept both key types at the wire boundary.

## Running it inside this repo

```sh
pnpm install          # from the repo root
pnpm build            # build the library first — this example imports dist/, not src/
pnpm --filter "@fokosdb-example/typed-keys" check
```
