import type { FokosDB } from "../../../src/client/db.js";

export type TextKey = {
	hashKey: string;
	sortKey?: string;
};

export type TextSnapshot = {
	text: string;
	version: number;
};

/** Return null for an absent item. Reject an existing item whose kind is not text. */
export async function readText(db: FokosDB, key: TextKey): Promise<TextSnapshot | null> {
	// Step 1: read the item and interpret its result.
	throw new Error("Step 1: implement readText");
}

/** Store the text at this key and return the version reported by the database. */
export async function writeText(db: FokosDB, key: TextKey, text: string): Promise<number> {
	// Steps 2 and 3: create or replace the item with one put operation.
	throw new Error("Step 2: implement writeText");
}

/** Return whether this delete operation removed an item. */
export async function removeText(db: FokosDB, key: TextKey): Promise<boolean> {
	// Step 4: delete this key and interpret the result.
	throw new Error("Step 4: implement removeText");
}
