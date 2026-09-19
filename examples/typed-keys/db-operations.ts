/**
 * Confirms the narrowing from narrow-keys.ts (its FokosTypeOverrides augmentation applies to this
 * whole program, this file included — TypeScript declaration merging is program-wide, not
 * file-scoped) actually reaches FokosDB's six public operations, not just the standalone option
 * types: putItem, getItem, deleteItem, transactWriteItems, transactGetItems, queryItems. Every one of
 * them takes its hash/sort keys through a type that composes HashKey/SortKey (PutItemOptions, ItemKey,
 * TransactWriteItem, ...), so this is confirming that composition holds end to end, on both the
 * request and the result side.
 *
 * `db` is declared, never constructed — building a real FokosDB needs Durable Object bindings this
 * package has none of. Nothing here executes; `tsc --noEmit` is the whole test.
 */
import type { FokosDB } from "fokosdb/client";

declare const db: FokosDB;

// putItem
void db.putItem({ hashKey: "user#1", sortKey: "profile", data: "hi" });
// @ts-expect-error - putItem must reject a bytes hashKey once the app narrows HashKey to string.
void db.putItem({ hashKey: new Uint8Array(), data: "hi" });

// getItem — both the option and the result's keys must be narrowed.
async function checkGetItem() {
	const res = await db.getItem({ hashKey: "user#1", sortKey: "profile" });
	const hk: string = res.item.hashKey;
	const sk: string | undefined = res.item.sortKey;
}
void checkGetItem;
// @ts-expect-error - getItem must reject a bytes hashKey once narrowed.
void db.getItem({ hashKey: new Uint8Array() });

// deleteItem
void db.deleteItem({ hashKey: "user#1", sortKey: "profile" });
// @ts-expect-error - deleteItem must reject a bytes hashKey once narrowed.
void db.deleteItem({ hashKey: new Uint8Array() });

// transactWriteItems
void db.transactWriteItems({
	items: [
		{ operation: "put", hashKey: "user#1", sortKey: "profile", data: "hi" },
		{ operation: "delete", hashKey: "user#2" },
	],
});
void db.transactWriteItems({
	items: [
		// @ts-expect-error - a put operation's hashKey must reject bytes once narrowed.
		{ operation: "put", hashKey: new Uint8Array(), data: "hi" },
	],
});

// transactGetItems — the result's keys must be narrowed too.
async function checkTransactGetItems() {
	const res = await db.transactGetItems({ items: [{ hashKey: "user#1", sortKey: "profile" }] });
	const hk: string = res.items[0].hashKey;
}
void checkTransactGetItems;
// @ts-expect-error - transactGetItems must reject a bytes hashKey once narrowed.
void db.transactGetItems({ items: [{ hashKey: new Uint8Array() }] });

// queryItems — same for a query's hashKey, and for the result's item keys.
async function checkQueryItems() {
	const res = await db.queryItems({ queries: [{ hashKey: "user#1", sortKeyCondition: { op: "begins_with", prefix: "profile" } }] });
	const hk: string = res.items[0].hashKey;
}
void checkQueryItems;
// @ts-expect-error - queryItems must reject a bytes hashKey once narrowed.
void db.queryItems({ queries: [{ hashKey: new Uint8Array() }] });
