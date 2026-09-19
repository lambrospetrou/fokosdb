/**
 * Regression check for FokosDB's HashKey/SortKey narrowing via module augmentation (see
 * `FokosTypeOverrides` in packages/fokosdb/src/shared/types.ts).
 *
 * This lives in its own package, not inside packages/fokosdb, because the augmentation below is
 * program-wide: applied inside the library's own compilation it would narrow FokosDB's internal code
 * too, which still has to handle both key types at the wire boundary. A separate package that imports
 * the built `fokosdb/client` entry point is an isolated TypeScript program, so the augmentation here
 * cannot leak back into the library.
 *
 * There is nothing to run: `tsc --noEmit` (the `check`/`test` scripts) is the whole test. Each
 * `@ts-expect-error` line is the assertion — it fails the build if the line stops being a type error,
 * i.e. if the narrowing regresses.
 */
import "fokosdb/client";

declare module "fokosdb/client" {
	interface FokosTypeOverrides {
		HashKey: string;
		SortKey: string;
	}
}

import { FokosStd, type HashKey, type PutItemOptions, type SortKey, type SortKeyCondition } from "fokosdb/client";

const hashKey: HashKey = "user#1";
const sortKey: SortKey = "profile";

// @ts-expect-error - Uint8Array must no longer be assignable to HashKey once the app narrows it to string.
const _badHashKey: HashKey = new Uint8Array();
// @ts-expect-error - same for SortKey.
const _badSortKey: SortKey = new Uint8Array();

const _putOptions: PutItemOptions = { hashKey, sortKey, data: "hello" };
// @ts-expect-error - putItem's hashKey option must reject bytes too, not just the bare HashKey alias.
const _badPutOptions: PutItemOptions = { hashKey: new Uint8Array(), data: "hello" };

const _condition: SortKeyCondition = { op: "begins_with", prefix: "profile" };
// @ts-expect-error - a sort-key condition value must reject bytes once narrowed.
const _badCondition: SortKeyCondition = { op: "begins_with", prefix: new Uint8Array() };

// FokosStd.sortKeySuccessor is generic over SortKey, so it must narrow along with everything else.
const _successor: string | undefined = FokosStd.sortKeySuccessor("profile");
// @ts-expect-error
FokosStd.sortKeySuccessor(new Uint8Array());
