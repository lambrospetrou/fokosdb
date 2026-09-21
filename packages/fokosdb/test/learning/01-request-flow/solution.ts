import type { FokosDB } from "../../../src/client/db.js";
import type { TextKey, TextSnapshot } from "./challenge.js";

export async function readText(db: FokosDB, key: TextKey): Promise<TextSnapshot | null> {
	const result = await db.getItem(key);
	if (!result.found) return null;
	if (result.item.kind !== "text") throw new TypeError("Expected a text item");
	return { text: result.item.data, version: result.item.version };
}

export async function writeText(db: FokosDB, key: TextKey, text: string): Promise<number> {
	const result = await db.putItem({ ...key, data: text });
	return result.version;
}

export async function removeText(db: FokosDB, key: TextKey): Promise<boolean> {
	const result = await db.deleteItem(key);
	return result.deleted;
}
