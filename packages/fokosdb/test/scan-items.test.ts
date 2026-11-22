/**
 * `db.scanItems`: the walk of the storage order across owned slices, the cursor, the start key, and
 * the segments.
 */
import { describe, expect, it } from "vitest";
import { FokosDB } from "../src/client/db.js";
import { FokosError } from "../src/shared/errors.js";
import { createTableConfig } from "../src/shared/partition-context.js";
import { CURSOR_VERSION, encodeCursor } from "../src/shared/query/cursor.js";
import { MAX_PARTITION_VISITS_PER_PAGE } from "../src/shared/query/page-budget.js";
import type { ScanItemsOptions, ScanItemsResult } from "../src/shared/types.js";
import { routingHashOf } from "../src/sharding/hash-primitives.js";
import { KeyCodec } from "../src/sharding/key-codec.js";
import { compareStorageKeys } from "../src/sharding/repartition-slice.js";
import { kb } from "./partition-do/helpers.js";
import { TestPartition } from "./partition-do/partition-harness.js";
import { testRoutingHashSeedResolver } from "./routing-hash.js";
import { makeDB, partitionRouter } from "./transactions/tx-helpers.js";

type Key = [string, string];

const KEYS: Key[] = Array.from({ length: 30 }, (_, i) => [`user-${i % 11}`, `s${String(i).padStart(2, "0")}`]);

const text = (items: ScanItemsResult["items"]) => items.map((item) => `${String(item.hashKey)}/${String(item.sortKey)}`);

function storageOrder(db: FokosDB, keys: readonly Key[]): string[] {
	const { shardGroup } = createTableConfig(db.options()).topology;
	const position = ([hk, sk]: Key) => ({
		routingHash: routingHashOf(kb(hk), shardGroup, testRoutingHashSeedResolver),
		hk: kb(hk),
		sk: kb(sk),
	});
	return [...keys].sort((a, b) => compareStorageKeys(position(a), position(b))).map(([hk, sk]) => `${hk}/${sk}`);
}

async function putAll(db: FokosDB, keys: readonly Key[]): Promise<void> {
	for (const [hashKey, sortKey] of keys) {
		await db.putItem({ hashKey, sortKey, data: `${hashKey}/${sortKey}` });
	}
}

/** Reads every page of a scan. */
async function drain(db: FokosDB, opts: ScanItemsOptions = {}): Promise<{ keys: string[]; pages: ScanItemsResult[] }> {
	const pages: ScanItemsResult[] = [];
	let cursor: string | undefined;
	do {
		const page = await db.scanItems({ ...opts, ...(cursor === undefined ? {} : { cursor, exclusiveStartKey: undefined }) });
		pages.push(page);
		cursor = page.cursor;
	} while (cursor !== undefined);
	return { keys: pages.flatMap((page) => text(page.items)), pages };
}

async function codeOf(run: () => Promise<unknown>): Promise<string> {
	try {
		await run();
	} catch (e) {
		expect(e).toBeInstanceOf(FokosError);
		return (e as FokosError).code;
	}
	throw new Error("the call did not fail");
}

describe("FokosDB scanItems", () => {
	it("returns each item once in the storage order across the roots, one page or many", async () => {
		const db = makeDB({ rootTreesN: 4 });
		await putAll(db, KEYS);
		const expected = storageOrder(db, KEYS);
		const one = await drain(db);
		expect(one.keys).toEqual(expected);
		expect(one.pages).toHaveLength(1);
		expect(one.pages[0]).toMatchObject({ count: KEYS.length, scannedCount: KEYS.length });

		const paged = await drain(db, { limit: 7 });
		expect(paged.keys).toEqual(expected);
		expect(paged.pages.length).toBeGreaterThanOrEqual(Math.ceil(KEYS.length / 7));

		// The response budget can stop a step before its first row, and the next page reads that row.
		const small = await drain(db, { maxResponseBytes: 1 });
		expect(small.keys).toEqual(expected);
		expect(small.pages.every((page) => page.items.length <= 1)).toBe(true);

		const counted = await drain(db, { select: "count", limit: 4, filter: { op: "eq", args: [{ ref: "hashKey" }, { val: "user-1" }] } });
		expect(counted.keys).toEqual([]);
		expect(counted.pages.reduce((n, page) => n + page.count, 0)).toBe(KEYS.filter(([hk]) => hk === "user-1").length);
		expect(counted.pages.reduce((n, page) => n + page.scannedCount, 0)).toBe(KEYS.length);
	});

	it("visits at most the visit limit per page, and ends a page at the next slice", async () => {
		const db = makeDB({ rootTreesN: MAX_PARTITION_VISITS_PER_PAGE + 30 });
		await putAll(db, KEYS.slice(0, 3));
		const { keys, pages } = await drain(db);
		expect(keys).toEqual(storageOrder(db, KEYS.slice(0, 3)));
		expect(pages).toHaveLength(2);
		expect(pages[0].meta.partitions).toHaveLength(MAX_PARTITION_VISITS_PER_PAGE);
		expect(pages[1].meta.partitions).toHaveLength(30);
	});

	it("starts after exclusiveStartKey, and refuses it with a cursor", async () => {
		const db = makeDB({ rootTreesN: 4 });
		await putAll(db, KEYS);
		const expected = storageOrder(db, KEYS);
		const [hashKey, sortKey] = expected[11].split("/");
		const after = await drain(db, { exclusiveStartKey: { hashKey, sortKey } });
		expect(after.keys).toEqual(expected.slice(12));
		// A start key with no sort key starts after the item with no sort key, so before every item of its hash key here.
		const ofKey = await drain(db, { exclusiveStartKey: { hashKey } });
		expect(ofKey.keys).toEqual(expected.slice(expected.findIndex((k) => k.startsWith(`${hashKey}/`))));

		// The types refuse base keys on a table scan. A JavaScript caller gets an error.
		const withBaseKey = { hashKey, baseHashKey: hashKey } as ScanItemsOptions["exclusiveStartKey"];
		expect(await codeOf(() => db.scanItems({ exclusiveStartKey: withBaseKey }))).toBe("scan_start_key_invalid");

		const page = await db.scanItems({ limit: 1 });
		expect(await codeOf(() => db.scanItems({ cursor: page.cursor, exclusiveStartKey: { hashKey } }))).toBe("scan_start_key_with_cursor");
	});

	it("checks the cursor: its table, its segment, its version, and its form", async () => {
		const db = makeDB({ rootTreesN: 4 });
		const other = makeDB({ rootTreesN: 4 });
		await putAll(db, KEYS);
		const page = await db.scanItems({ limit: 3, segment: 0, totalSegments: 2 });
		const cursor = page.cursor!;
		expect(await codeOf(() => other.scanItems({ cursor, segment: 0, totalSegments: 2 }))).toBe("cursor_fingerprint_mismatch");
		expect(await codeOf(() => db.scanItems({ cursor }))).toBe("cursor_fingerprint_mismatch");
		expect(await codeOf(() => db.scanItems({ cursor, segment: 1, totalSegments: 2 }))).toBe("cursor_fingerprint_mismatch");
		const query = encodeCursor({ version: CURSOR_VERSION, direction: "fwd", fingerprint: 1n, queryIdx: 0, inner: null });
		expect(await codeOf(() => db.scanItems({ cursor: query }))).toBe("cursor_malformed");
		expect(await codeOf(() => db.scanItems({ cursor: "not json" }))).toBe("cursor_malformed");
		const wire = JSON.parse(new TextDecoder().decode(Uint8Array.fromBase64(cursor, { alphabet: "base64url" })));
		const edited = (change: object) => new TextEncoder().encode(JSON.stringify({ ...wire, ...change })).toBase64({ alphabet: "base64url" });
		const opts = { segment: 0, totalSegments: 2 };
		expect(await codeOf(() => db.scanItems({ cursor: edited({ v: 99 }), ...opts }))).toBe("cursor_version_unknown");
		expect(await codeOf(() => db.scanItems({ cursor: edited({ incl: "yes" }), ...opts }))).toBe("cursor_malformed");
		expect(await codeOf(() => db.scanItems({ cursor: edited({ pos: { rh: "AAAA" } }), ...opts }))).toBe("cursor_malformed");
		expect(await codeOf(() => db.scanItems({ cursor: edited({ pos: { rh: "_____________w" } }), ...opts }))).toBe("cursor_malformed");

		// The filter and the projection can change between pages.
		const next = await db.scanItems({ cursor, ...opts, projection: [{ expr: { ref: "sortKey" } }] });
		expect(next.items.length).toBeGreaterThan(0);
	});

	it("splits the table into segments whose union is the full scan", async () => {
		const db = makeDB({ rootTreesN: 4 });
		await putAll(db, KEYS);
		const expected = storageOrder(db, KEYS);
		for (const totalSegments of [1, 3, 7]) {
			const parts: string[] = [];
			for (let segment = 0; segment < totalSegments; segment++) {
				parts.push(...(await drain(db, { segment, totalSegments, limit: 4 })).keys);
			}
			expect(parts, `totalSegments ${totalSegments}`).toEqual(expected);
		}
		expect(await codeOf(() => db.scanItems({ segment: 0 }))).toBe("scan_segment_invalid");
		expect(await codeOf(() => db.scanItems({ segment: 2, totalSegments: 2 }))).toBe("scan_segment_invalid");
		expect(await codeOf(() => db.scanItems({ segment: 0, totalSegments: 1_000_001 }))).toBe("scan_segment_invalid");
		const [hashKey] = expected[expected.length - 1].split("/");
		expect(await codeOf(() => db.scanItems({ exclusiveStartKey: { hashKey }, segment: 0, totalSegments: 1_000_000 }))).toBe(
			"scan_start_key_outside_segment",
		);
	});

	it("reads a promoted key from its range tree once, also when the tree splits between two pages", async () => {
		const db = new FokosDB({
			table: { name: `scan.${crypto.randomUUID()}`, ns: "PARTITION_DO", nsTx: "TRANSACTION_COORDINATOR_DO", rootTreesN: 1, hashSplitN: 2 },
			rangeSplitN: 2,
			rangeSplitConditions: { maxSizeMb: 1 },
		});
		await putAll(db, KEYS);
		const root = TestPartition.at(partitionRouter(db).rootContext(kb("user-3")));
		await root.rpc.debugForcePromoteKey(root.ctx, { hashKey: kb("user-3") });
		const rangeRoot = await root.awaitPromoted("user-3");
		const expected = storageOrder(db, KEYS);
		expect((await drain(db, { limit: 5 })).keys).toEqual(expected);

		const first = await db.scanItems({ limit: 2 });
		const filler = await rangeRoot.triggerRangeSplit((i) => `t${String(i).padStart(4, "0")}`);
		await rangeRoot.awaitSplitCompleted();
		let cursor = first.cursor;
		const keys = text(first.items);
		while (cursor !== undefined) {
			const page = await db.scanItems({ cursor, limit: 9 });
			keys.push(...text(page.items));
			cursor = page.cursor;
		}
		expect(keys).toEqual(storageOrder(db, [...KEYS, ...filler.map((sk): Key => ["user-3", sk])]));
	});

	it("returns each item once when a hash partition splits between two pages", async () => {
		const db = makeDB({ rootTreesN: 1, maxSizeMb: 1 });
		await putAll(db, KEYS);
		const first = await db.scanItems({ limit: 10 });
		const root = TestPartition.at(partitionRouter(db).rootContext(kb("user-0")));
		const filler = await root.triggerHashSplit();
		await root.awaitSplitCompleted();
		const keys = text(first.items);
		let cursor = first.cursor;
		while (cursor !== undefined) {
			const page = await db.scanItems({ cursor, limit: 10 });
			keys.push(...text(page.items));
			cursor = page.cursor;
		}
		const fillerKeys = new Set(
			filler.map((item) => `${KeyCodec.decode(item.hashKey) as string}/${KeyCodec.decode(item.sortKey!) as string}`),
		);
		expect(new Set(keys).size).toBe(keys.length);
		expect(keys.filter((key) => !fillerKeys.has(key))).toEqual(storageOrder(db, KEYS));
	});
});
