import { describe, expect, it } from "vitest";
import { FokosDB } from "../../../src/client/db.js";
import { PartitionContextCreator } from "../../../src/shared/partition-topology/partition-context.js";
import { PartitionTopologyRouterImpl } from "../../../src/shared/partition-topology/router.js";
import { readText, removeText, writeText } from "./challenge.js";

function makeDB() {
	const context = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: `learning.request-flow.${crypto.randomUUID()}`,
		rootTreesN: 2,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: 100 },
	});
	return new FokosDB({ topology: new PartitionTopologyRouterImpl(context) });
}

const profile = { hashKey: "account#17", sortKey: "profile" };

describe("step 1 — read text", () => {
	it("returns null when the key has no item", async () => {
		const db = makeDB();
		await expect(readText(db, profile)).resolves.toBeNull();
	});

	it("reads text and its version from an item seeded through the database", async () => {
		const db = makeDB();
		await db.putItem({ ...profile, data: "first" });
		await db.putItem({ ...profile, data: "second" });
		await expect(readText(db, profile)).resolves.toEqual({ text: "second", version: 2 });
	});

	it("keeps an empty text value distinct from an absent item", async () => {
		const db = makeDB();
		await db.putItem({ ...profile, data: "" });
		await expect(readText(db, profile)).resolves.toEqual({ text: "", version: 1 });
	});

	it.each([
		{ label: "JSON", data: { name: "Ada" } },
		{ label: "bytes", data: new Uint8Array([65, 66]) },
	])("rejects an existing $label item instead of treating it as absent", async ({ data }) => {
		const db = makeDB();
		await db.putItem({ ...profile, data });
		await expect(readText(db, profile)).rejects.toBeInstanceOf(TypeError);
	});
});

describe("step 2 — write text", () => {
	it("stores data that can be read without the exercise adapter", async () => {
		const db = makeDB();
		await expect(writeText(db, profile, "Ada")).resolves.toBe(1);
		// Read directly so an adapter-local cache cannot satisfy the storage contract.
		await expect(db.getItem(profile)).resolves.toMatchObject({
			found: true,
			item: { ...profile, kind: "text", data: "Ada", version: 1 },
		});
		await expect(db.getItem({ hashKey: "account#18", sortKey: "profile" })).resolves.toMatchObject({ found: false });
	});

	it("supports a key without a sort key", async () => {
		const db = makeDB();
		const key = { hashKey: "settings" };
		await expect(writeText(db, key, "enabled")).resolves.toBe(1);
		await expect(db.getItem(key)).resolves.toMatchObject({
			found: true,
			item: { kind: "text", data: "enabled", version: 1 },
		});
	});
});

describe("step 3 — replace text and observe versions", () => {
	it("returns the database version when an item already exists", async () => {
		const db = makeDB();
		await db.putItem({ ...profile, data: "old" });
		await expect(writeText(db, profile, "new")).resolves.toBe(2);
		await expect(readText(db, profile)).resolves.toEqual({ text: "new", version: 2 });
	});

	it("counts another write even when its text is unchanged; reads do not increment the version", async () => {
		const db = makeDB();
		await writeText(db, profile, "same");
		await readText(db, profile);
		await expect(writeText(db, profile, "same")).resolves.toBe(2);
		await expect(readText(db, profile)).resolves.toEqual({ text: "same", version: 2 });
		await expect(readText(db, profile)).resolves.toEqual({ text: "same", version: 2 });
	});
});

describe("step 4 — delete one item", () => {
	it("removes a stored item and reports that a row was removed", async () => {
		const db = makeDB();
		await db.putItem({ ...profile, data: "Ada" });
		await expect(removeText(db, profile)).resolves.toBe(true);
		await expect(db.getItem(profile)).resolves.toMatchObject({ found: false });
	});

	it("returns false for an absent key, including a second delete", async () => {
		const db = makeDB();
		await expect(removeText(db, profile)).resolves.toBe(false);
		await db.putItem({ ...profile, data: "Ada" });
		await expect(removeText(db, profile)).resolves.toBe(true);
		await expect(removeText(db, profile)).resolves.toBe(false);
	});

	it("preserves a different item under the same hash key", async () => {
		const db = makeDB();
		const preferences = { hashKey: profile.hashKey, sortKey: "preferences" };
		await db.putItem({ ...profile, data: "Ada" });
		await db.putItem({ ...preferences, data: "dark" });
		await removeText(db, profile);
		await expect(db.getItem(preferences)).resolves.toMatchObject({
			found: true,
			item: { kind: "text", data: "dark", version: 1 },
		});
	});
});

describe("step 5 — transfer: reason about complete keys", () => {
	it("keeps two sort keys and a different hash key independent", async () => {
		const db = makeDB();
		const draft = { hashKey: "notebook#4", sortKey: "draft" };
		const title = { hashKey: "notebook#4", sortKey: "title" };
		const otherDraft = { hashKey: "notebook#5", sortKey: "draft" };

		await writeText(db, draft, "rough");
		await writeText(db, title, "Field notes");
		await writeText(db, otherDraft, "other");
		await writeText(db, draft, "revised");
		await removeText(db, title);

		await expect(readText(db, draft)).resolves.toEqual({ text: "revised", version: 2 });
		await expect(readText(db, title)).resolves.toBeNull();
		await expect(readText(db, otherDraft)).resolves.toEqual({ text: "other", version: 1 });
	});
});
