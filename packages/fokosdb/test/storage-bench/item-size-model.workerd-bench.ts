/**
 * Measures the storage model of `docs/agent-plans/2026-10-31-item-size-facts-and-range-split.md` on the
 * clustered layout of that document: `items` with `item_bytes` and no scan index, `latest_items` with
 * `item_bytes`, and `item_stats` with its partial promotion index. Each case creates these tables in a new
 * Durable Object, so the measurement does not depend on the current schema of the stores.
 *
 * Run it with `pnpm --filter fokosdb bench:workerd test/storage-bench/item-size-model.workerd-bench.ts`.
 * Use `-t` to run one part.
 */
import { runInDurableObject } from "cloudflare:test";
import { it } from "vitest";
import type { FokosDBPartitionDO } from "../../src/server/do-partition.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { testPartitionStub } from "../stub-helpers.js";
import { routingHashFor } from "../routing-hash.js";

const NOW_MS = 1_780_000_000_000;
const TTL_SECONDS = 2_000_000_000;

const ITEMS_SCHEMA = `
	CREATE TABLE m_items (
		routing_hash          BLOB    NOT NULL,
		hk                    BLOB    NOT NULL,
		sk                    BLOB    NOT NULL DEFAULT x'',
		item_id               INTEGER NOT NULL,
		data_kind             INTEGER NOT NULL DEFAULT 0,
		v                     INTEGER NOT NULL,
		last_read_ts          INTEGER NOT NULL DEFAULT 0,
		last_write_ts         INTEGER NOT NULL DEFAULT 0,
		ttl_epoch_utc_seconds INTEGER,
		item_bytes            INTEGER NOT NULL,
		data                  ANY     NOT NULL,
		PRIMARY KEY (routing_hash, hk, sk)
	) WITHOUT ROWID, STRICT;
	CREATE UNIQUE INDEX m_items_item_id ON m_items (item_id);
	CREATE INDEX m_items_ttl ON m_items (ttl_epoch_utc_seconds, routing_hash, hk, sk)
		WHERE ttl_epoch_utc_seconds IS NOT NULL;`;

const LATEST_SCHEMA = `
	CREATE TABLE m_latest_items (
		routing_hash  BLOB    NOT NULL,
		base_hk       BLOB    NOT NULL,
		base_sk       BLOB    NOT NULL DEFAULT x'',
		item_id       INTEGER NOT NULL,
		revision      INTEGER NOT NULL,
		deleted_at_ms INTEGER,
		newest_seq    INTEGER,
		data_kind     INTEGER,
		ttl_at        INTEGER,
		item_bytes    INTEGER NOT NULL DEFAULT 0,
		data          ANY,
		PRIMARY KEY (routing_hash, base_hk, base_sk),
		UNIQUE (item_id)
	) WITHOUT ROWID, STRICT;
	CREATE INDEX m_latest_items_deleted_gc ON m_latest_items (deleted_at_ms, item_id) WHERE deleted_at_ms IS NOT NULL;`;

const STATS_SCHEMA = `
	CREATE TABLE m_item_stats (
		routing_hash       BLOB    NOT NULL,
		lower_hk           BLOB    NOT NULL,
		lower_sk           BLOB    NOT NULL,
		item_count         INTEGER NOT NULL,
		item_bytes         INTEGER NOT NULL,
		key_bytes          INTEGER NOT NULL,
		indexed_item_count INTEGER NOT NULL,
		indexed_key_bytes  INTEGER NOT NULL,
		overflow_bytes     INTEGER NOT NULL,
		est_bytes          INTEGER NOT NULL,
		PRIMARY KEY (routing_hash, lower_hk, lower_sk)
	) WITHOUT ROWID, STRICT;
	CREATE INDEX m_item_stats_promotion ON m_item_stats (est_bytes) WHERE lower_hk = x'' AND lower_sk = x'';`;

/**
 * The overflow bytes of a row with payload `itemBytes + overhead`, on a 4 KiB page of an index B-tree: the unused
 * space of its overflow pages, plus `rowOverhead` for the part of the row that stays on the tree page.
 */
function overflowBytes(itemBytes: number, overhead: number, rowOverhead: number): number {
	const p = itemBytes + overhead;
	if (p <= 1002) {
		return 0;
	}
	let local = 489 + ((p - 489) % 4092);
	if (local > 1002) {
		local = 489;
	}
	const moved = p - local;
	return Math.ceil(moved / 4092) * 4096 - moved + rowOverhead;
}

/** The same function as SQL over the expression `itemBytes`. */
function overflowSql(itemBytes: string, overhead: number, rowOverhead: number): string {
	const p = `(${itemBytes} + ${overhead})`;
	const rem = `(489 + ((${p} - 489) % 4092))`;
	const local = `(CASE WHEN ${rem} > 1002 THEN 489 ELSE ${rem} END)`;
	const moved = `(${p} - ${local})`;
	return `(CASE WHEN ${p} <= 1002 THEN 0 ELSE ((${moved} + 4091) / 4092) * 4096 - ${moved} + ${rowOverhead} END)`;
}

const hashKey = (bytes: number) => KeyCodec.encode(new Uint8Array(bytes - 1).fill(0x61));
/** A text sort key of exactly `bytes` encoded bytes. */
function sortKey(i: number, bytes: number): Uint8Array {
	const probe = KeyCodec.encode("0");
	const tag = probe.length - 1;
	return KeyCodec.encode(String(i).padStart(bytes - tag, "0"));
}

function shuffled(n: number): number[] {
	const order = Array.from({ length: n }, (_, i) => i);
	for (let i = n - 1; i > 0; i--) {
		const j = Math.floor(Math.random() * (i + 1));
		[order[i], order[j]] = [order[j], order[i]];
	}
	return order;
}

type ItemsCase = { key: number; data: number; indexed: boolean; rows: number; random?: boolean; firstItemId?: number };

/** Bytes for each row of `m_items`: the increase of `databaseSize` divided by the row count. */
async function measureItems(c: ItemsCase): Promise<number> {
	const stub = testPartitionStub(`item-size-model.${crypto.randomUUID()}`);
	return await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
		const sql = state.storage.sql;
		sql.exec(ITEMS_SCHEMA);
		const hk = hashKey(8);
		const routingHash = routingHashFor(hk);
		const data = "d".repeat(c.data);
		const order = c.random ? shuffled(c.rows) : Array.from({ length: c.rows }, (_, i) => i);
		const before = sql.databaseSize;
		state.storage.transactionSync(() => {
			for (const i of order) {
				const sk = sortKey(i, c.key - hk.length);
				sql.exec(
					`INSERT INTO m_items (routing_hash, hk, sk, item_id, data_kind, v, last_read_ts, last_write_ts,
					                      ttl_epoch_utc_seconds, item_bytes, data)
					 VALUES (?, ?, ?, ?, 0, 1, ?, ?, ?, ?, ?)`,
					routingHash,
					hk,
					sk,
					(c.firstItemId ?? 1) + i,
					NOW_MS,
					NOW_MS,
					c.indexed ? TTL_SECONDS : null,
					c.data + hk.length + sk.length,
					data,
				);
			}
		});
		return (sql.databaseSize - before) / c.rows;
	});
}

/** Bytes for each row of `m_latest_items`. An indexed row is a tombstone. */
async function measureLatest(c: ItemsCase): Promise<number> {
	const stub = testPartitionStub(`item-size-model.${crypto.randomUUID()}`);
	return await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
		const sql = state.storage.sql;
		sql.exec(LATEST_SCHEMA);
		const hk = hashKey(8);
		const routingHash = routingHashFor(hk);
		const data = "d".repeat(c.data);
		const before = sql.databaseSize;
		state.storage.transactionSync(() => {
			for (let i = 0; i < c.rows; i++) {
				const sk = sortKey(i, c.key - hk.length);
				const keyBytes = hk.length + sk.length;
				sql.exec(
					`INSERT INTO m_latest_items (routing_hash, base_hk, base_sk, item_id, revision, deleted_at_ms,
					                             newest_seq, data_kind, ttl_at, item_bytes, data)
					 VALUES (?, ?, ?, ?, 1, ?, ?, ?, NULL, ?, ?)`,
					routingHash,
					hk,
					sk,
					i + 1,
					c.indexed ? NOW_MS : null,
					1_000_000 + i,
					c.indexed ? null : 0,
					c.indexed ? keyBytes : c.data + keyBytes,
					c.indexed ? null : data,
				);
			}
		});
		return (sql.databaseSize - before) / c.rows;
	});
}

/**
 * The multiples of 1/4 `a` and `b` that make the largest relative error of `measured = known + a * key + b`
 * smallest. A least-squares fit gives large relative errors for small rows, because their fixed bytes dominate.
 */
function fitMaxRelative(points: Array<{ key: number; known: number; measured: number }>): { a: number; b: number; err: number } {
	let best = { a: 0, b: 0, err: Infinity };
	for (let a = 0; a <= 3; a += 0.25) {
		for (let b = 0; b <= 150; b += 0.25) {
			const err = Math.max(...points.map((p) => Math.abs(p.known + a * p.key + b - p.measured) / p.measured));
			if (err < best.err) {
				best = { a, b, err };
			}
		}
	}
	return best;
}

const KEYS = [16, 58, 108, 208, 408];
const DATA = [0, 20, 50, 100, 200, 400, 700, 1_000];
/** The fit uses rows with some data. Rows of 400 bytes and more waste page space that the model does not count. */
const FIT_DATA = [20, 50, 100, 200];
const ROWS = 20_000;

/** Finds the largest data size with no overflow page: the payload is then at most 1,002 bytes. */
async function payloadOverhead(
	measure: (c: ItemsCase) => Promise<number>,
	key: number,
	indexed: boolean,
	firstItemId = 1,
): Promise<number> {
	let lo = 600;
	let hi = 1002;
	while (hi - lo > 1) {
		const mid = Math.floor((lo + hi) / 2);
		const perRow = await measure({ key, data: mid, indexed, rows: 2_000, firstItemId });
		if (perRow > 2_500) {
			hi = mid;
		} else {
			lo = mid;
		}
	}
	return 1002 - (lo + key);
}

type Coefficients = { kif: number; bo: number; ikf: number; io: number; overhead: number; rowOverhead: number };
type Table = {
	name: string;
	measure: (c: ItemsCase) => Promise<number>;
	indexedData: number[];
	/** The `row_payload_overhead` of the model: one constant within a few bytes of each measured case. */
	overhead: number;
};
const TABLES: Table[] = [
	{ name: "items", measure: measureItems, indexedData: DATA, overhead: 40 },
	// A tombstone has no data.
	{ name: "latest_items", measure: measureLatest, indexedData: [0], overhead: 29 },
];

it("measures the coefficients and row_payload_overhead", async () => {
	for (const table of TABLES) {
		const measuredOverhead = await payloadOverhead(table.measure, 24, false);
		const measuredOverheadIndexed = table.name === "items" ? await payloadOverhead(table.measure, 24, true) : measuredOverhead;
		const measured = new Map<string, number>();
		for (const key of KEYS) {
			for (const data of DATA) {
				measured.set(`${key}/${data}/0`, await table.measure({ key, data, indexed: false, rows: ROWS }));
			}
			for (const data of table.indexedData) {
				measured.set(`${key}/${data}/1`, await table.measure({ key, data, indexed: true, rows: ROWS }));
			}
		}
		const itemBytesOf = (key: number, data: number, isIndexed: boolean) => (table.name === "latest_items" && isIndexed ? 0 : data) + key;
		const fitsInline = (key: number, data: number, isIndexed: boolean) => itemBytesOf(key, data, isIndexed) + table.overhead <= 1002;
		const base = fitMaxRelative(
			KEYS.flatMap((key) =>
				FIT_DATA.filter((data) => fitsInline(key, data, false)).map((data) => ({
					key,
					known: data + key,
					measured: measured.get(`${key}/${data}/0`)!,
				})),
			),
		);
		const indexedFitData = table.indexedData.filter((data) => FIT_DATA.includes(data) || table.indexedData.length === 1);
		const indexed = fitMaxRelative(
			KEYS.flatMap((key) =>
				indexedFitData
					.filter((data) => fitsInline(key, data, true))
					.map((data) => ({
						key,
						known: itemBytesOf(key, data, true) + base.a * key + base.b,
						measured: measured.get(`${key}/${data}/1`)!,
					})),
			),
		);
		const c: Coefficients = { kif: base.a, bo: base.b, ikf: indexed.a, io: indexed.b, overhead: table.overhead, rowOverhead: 0 };
		const model = (key: number, data: number, isIndexed: boolean) => {
			const itemBytes = itemBytesOf(key, data, isIndexed);
			return itemBytes + c.kif * key + c.bo + (isIndexed ? c.ikf * key + c.io : 0) + overflowBytes(itemBytes, c.overhead, c.rowOverhead);
		};
		// The bytes that each row with overflow pages uses beyond the model: the smallest largest error.
		const overflowRows = [...measured.entries()]
			.map(([id, m]) => {
				const [key, data, ix] = id.split("/").map(Number);
				return { key, data, isIndexed: ix === 1, m };
			})
			.filter((row) => !fitsInline(row.key, row.data, row.isIndexed));
		let bestRowOverhead = { value: 0, err: Infinity };
		for (let value = 0; value <= 200; value++) {
			const err = Math.max(...overflowRows.map((row) => Math.abs(model(row.key, row.data, row.isIndexed) + value - row.m) / row.m));
			if (err < bestRowOverhead.err) {
				bestRowOverhead = { value, err };
			}
		}
		c.rowOverhead = bestRowOverhead.value;
		const lines = [
			`### ${table.name}`,
			`measured row_payload_overhead: ${measuredOverhead} (not indexed), ${measuredOverheadIndexed} (indexed), item ids up to 2,000; model uses ${table.overhead}`,
			`key_index_factor ${c.kif}, base_overhead ${c.bo}, indexed_key_factor ${c.ikf}, indexed_overhead ${c.io}, overflow_row_overhead ${c.rowOverhead}`,
			"| key | data | Measured, not indexed | Model, not indexed | Measured, indexed | Model, indexed |",
			"| --- | --- | --- | --- | --- | --- |",
		];
		const errors = new Map<string, number[]>();
		for (const key of KEYS) {
			for (const data of DATA) {
				const m0 = measured.get(`${key}/${data}/0`)!;
				const m1 = measured.get(`${key}/${data}/1`);
				const e0 = model(key, data, false);
				const e1 = m1 === undefined ? undefined : model(key, data, true);
				lines.push(`| ${key} | ${data} | ${m0.toFixed(0)} | ${e0.toFixed(0)} | ${m1?.toFixed(0) ?? "-"} | ${e1?.toFixed(0) ?? "-"} |`);
				const add = (name: string, m: number, e: number) => errors.set(name, [...(errors.get(name) ?? []), (m - e) / m]);
				add(`${data}`, m0, e0);
				if (m1 !== undefined) {
					add(`${data} indexed`, m1, e1!);
				}
			}
		}
		lines.push("", "| data | (measured - model) / measured, smallest to largest |", "| --- | --- |");
		for (const [name, list] of errors) {
			const pct = (v: number) => `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`;
			lines.push(`| ${name} | ${pct(Math.min(...list))} to ${pct(Math.max(...list))} |`);
		}
		if (table.name === "items") {
			for (const firstItemId of [40_000, 10_000_000]) {
				lines.push(
					`row_payload_overhead with item ids from ${firstItemId}: ${await payloadOverhead(measureItems, 24, false, firstItemId)}`,
				);
			}
			for (const key of [58, 208]) {
				const sorted = measured.get(`${key}/100/0`)!;
				const random = await measureItems({ key, data: 100, indexed: false, rows: ROWS, random: true });
				lines.push(`Random order, key ${key}, data 100: ${random.toFixed(0)} (sorted ${sorted.toFixed(0)})`);
			}
		}
		console.log(lines.join("\n"));
	}
});

it("measures overflow_bytes against the file growth", async () => {
	const lines = [
		"| data | item_bytes | measured | overflow_bytes, no row overhead | measured - item_bytes - overflow_bytes |",
		"| --- | --- | --- | --- | --- |",
	];
	const overhead = await payloadOverhead(measureItems, 24, false);
	for (const data of [800, 900, 950, 970, 990, 1_000, 1_100, 1_500, 2_000, 3_000, 4_000, 4_100, 4_600, 5_000, 8_000]) {
		const perRow = await measureItems({ key: 24, data, indexed: false, rows: 2_000 });
		const itemBytes = data + 24;
		const of = overflowBytes(itemBytes, overhead, 0);
		lines.push(`| ${data} | ${itemBytes} | ${perRow.toFixed(0)} | ${of} | ${(perRow - itemBytes - of).toFixed(0)} |`);
	}
	console.log(`row_payload_overhead ${overhead}\n${lines.join("\n")}`);
});

it("measures the storage of item_stats rows", async () => {
	const lines = ["| Row kind | Bytes for each row |", "| --- | --- |"];
	for (const kind of ["first bucket", "range bucket"] as const) {
		const stub = testPartitionStub(`item-size-model.${crypto.randomUUID()}`);
		const perRow = await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
			const sql = state.storage.sql;
			sql.exec(STATS_SCHEMA);
			const n = 20_000;
			const before = sql.databaseSize;
			state.storage.transactionSync(() => {
				for (let i = 0; i < n; i++) {
					const first = kind === "first bucket";
					const routingHash = first ? crypto.getRandomValues(new Uint8Array(8)) : routingHashFor(hashKey(8));
					const lowerHk = first ? new Uint8Array() : hashKey(8);
					const lowerSk = first ? new Uint8Array() : sortKey(i, 16);
					const count = first ? 1 + Math.floor(Math.random() * 100_000) : 2_048 + Math.floor(Math.random() * 2_048);
					const itemBytes = count * 150;
					sql.exec(
						`INSERT INTO m_item_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)`,
						routingHash,
						lowerHk,
						lowerSk,
						count,
						itemBytes,
						count * 24,
						Math.floor(count / 3),
						Math.floor(count / 3) * 24,
						itemBytes + 3 * count * 24 + 50 * count,
					);
				}
			});
			return (sql.databaseSize - before) / n;
		});
		lines.push(`| ${kind} | ${perRow.toFixed(1)} |`);
	}
	console.log(lines.join("\n"));
});

// ---------------------------------------------------------------------------------------------------------------
// Statement costs on one range-leaf slice.

const SLICE_ROWS = 300_000;
const K = 2_048;
const OVERHEAD = 40;
const ROW_OVERHEAD = 61;
/** The estimate weights of the cost runs, from the fit of `items`. Their values do not change the cost of a statement. */
const MODEL = { kif: 1.25, bo: 76, ikf: 1.25, io: 25, oc: 1 };
/** The `est_bytes` of a bucket over six SQL expressions for its facts: the estimate, rounded up with `ceil`. */
const estSql = (ic: string, ib: string, kb: string, iic: string, ikb: string, ob: string) =>
	`ceil((${ib}) + ${MODEL.kif} * (${kb}) + ${MODEL.bo} * (${ic}) + ${MODEL.ikf} * (${ikb}) + ${MODEL.io} * (${iic}) + ${MODEL.oc} * (${ob}))`;
const itemEstimate = (itemBytes: number, keyBytes: number, ttl: unknown) =>
	itemBytes +
	MODEL.kif * keyBytes +
	MODEL.bo +
	(ttl === null ? 0 : MODEL.ikf * keyBytes + MODEL.io) +
	MODEL.oc * overflowBytes(itemBytes, OVERHEAD, ROW_OVERHEAD);

type Slice = { stub: DurableObjectStub<FokosDBPartitionDO>; routingHash: Uint8Array; hk: Uint8Array; skBytes: number };

/** Builds `m_items` with one slice and its `m_item_stats` buckets of `K` items, as an import does. */
async function buildSlice(skBytes: number): Promise<Slice> {
	const stub = testPartitionStub(`item-size-model.${crypto.randomUUID()}`);
	const hk = hashKey(8);
	const routingHash = routingHashFor(hk);
	await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
		const sql = state.storage.sql;
		sql.exec(ITEMS_SCHEMA);
		sql.exec(STATS_SCHEMA);
		// The split runs write here, so that the buckets of the slice stay as the import built them.
		sql.exec(`CREATE TABLE m_split_stats AS SELECT * FROM m_item_stats WHERE 0`);
		const data = "d".repeat(20);
		for (let start = 0; start < SLICE_ROWS; start += 50_000) {
			state.storage.transactionSync(() => {
				for (let i = start; i < Math.min(SLICE_ROWS, start + 50_000); i++) {
					const sk = sortKey(i, skBytes);
					sql.exec(
						`INSERT INTO m_items VALUES (?, ?, ?, ?, 0, 1, ?, ?, ?, ?, ?)`,
						routingHash,
						hk,
						sk,
						i + 1,
						NOW_MS,
						NOW_MS,
						i % 3 === 0 ? TTL_SECONDS : null,
						20 + hk.length + sk.length,
						data,
					);
				}
			});
		}
		state.storage.transactionSync(() => {
			for (let b = 0; b * K < SLICE_ROWS; b++) {
				const first = b * K;
				const count = Math.min(K, SLICE_ROWS - first);
				const keyBytes = hk.length + skBytes;
				const itemBytes = 20 + keyBytes;
				const indexed = Math.floor((first + count + 2) / 3) - Math.floor((first + 2) / 3);
				sql.exec(
					`INSERT INTO m_item_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ${estSql("?4", "?5", "?6", "?7", "?8", "0")})`,
					routingHash,
					b === 0 ? new Uint8Array() : hk,
					b === 0 ? new Uint8Array() : sortKey(first, skBytes),
					count,
					count * itemBytes,
					count * keyBytes,
					indexed,
					indexed * keyBytes,
				);
			}
		});
	});
	return { stub, routingHash, hk, skBytes };
}

const median = (values: number[]) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];

/** Wall time of one call of `fn` in the slice DO, minus an empty call. The clock does not advance in sync code. */
async function timeCall<T>(slice: Slice, fn: (state: DurableObjectState) => T): Promise<{ ms: number; result: T }> {
	const empty: number[] = [];
	const full: number[] = [];
	let result!: T;
	for (let i = 0; i < 3; i++) {
		let t = Date.now();
		await runInDurableObject(slice.stub, () => 0);
		empty.push(Date.now() - t);
		t = Date.now();
		result = await runInDurableObject(slice.stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => fn(state));
		full.push(Date.now() - t);
	}
	return { ms: Math.max(0, median(full) - median(empty)), result };
}

const RANGE = `routing_hash = ?1 AND (hk, sk) >= (?2, ?3) AND (hk, sk) < (?4, ?5)`;
const OVERFLOW_OF_ITEM = overflowSql("item_bytes", OVERHEAD, ROW_OVERHEAD);
const FACTS = `COUNT(*) AS c, SUM(item_bytes) AS ib, SUM(length(hk) + length(sk)) AS kb,
	SUM(ttl_epoch_utc_seconds IS NOT NULL) AS ic, SUM(CASE WHEN ttl_epoch_utc_seconds IS NOT NULL THEN length(hk) + length(sk) ELSE 0 END) AS ikb,
	SUM(${OVERFLOW_OF_ITEM}) AS ob`;

function plans(state: DurableObjectState, statements: Array<[string, string, unknown[]]>): string[] {
	return statements.map(([name, query, args]) => {
		const rows = [...state.storage.sql.exec(`EXPLAIN QUERY PLAN ${query}`, ...args)];
		return `${name}: ${rows.map((r) => r.detail as string).join(" | ")}`;
	});
}

it("measures the statement costs on the clustered layout", async () => {
	for (const skBytes of [16, 100]) {
		const slice = await buildSlice(skBytes);
		const { routingHash, hk } = slice;
		const sk = (i: number) => sortKey(i, skBytes);
		const lines = [
			`### sort key ${skBytes} bytes, ${SLICE_ROWS} items, K = ${K}`,
			"| Operation | Cost | Rows read | Rows written |",
			"| --- | --- | --- | --- |",
		];
		const end = sortKey(SLICE_ROWS, skBytes);

		const iterate = await timeCall(slice, (state) => {
			const cursor = state.storage.sql.exec(
				`SELECT hk, sk, item_bytes, ttl_epoch_utc_seconds FROM m_items WHERE ${RANGE} ORDER BY hk, sk`,
				routingHash,
				hk,
				sk(0),
				hk,
				end,
			);
			let total = 0;
			for (const row of cursor) {
				total += itemEstimate(
					row.item_bytes as number,
					(row.hk as ArrayBuffer).byteLength + (row.sk as ArrayBuffer).byteLength,
					row.ttl_epoch_utc_seconds,
				);
			}
			return { total, read: cursor.rowsRead };
		});
		lines.push(
			`| JavaScript iteration of primary-key rows, with the estimate | ${((iterate.ms * 1000) / SLICE_ROWS).toFixed(2)} µs / row | ${iterate.result.read} | 0 |`,
		);

		const count = await timeCall(slice, (state) => {
			const cursor = state.storage.sql.exec(`SELECT COUNT(*) AS n FROM m_items WHERE ${RANGE}`, routingHash, hk, sk(0), hk, end);
			const n = cursor.one().n;
			return { n, read: cursor.rowsRead };
		});
		lines.push(
			`| SQL \`COUNT(*)\` over the primary key | ${((count.ms * 1000) / SLICE_ROWS).toFixed(3)} µs / entry | ${count.result.read} | 0 |`,
		);

		for (const k of [2_048, 4_096, 8_192, 16_384]) {
			const reps = Math.max(2, Math.floor(40 / (k / 2_048)));
			const split = await timeCall(slice, (state) => {
				const sql = state.storage.sql;
				let read = 0;
				let written = 0;
				state.storage.transactionSync(() => {
					for (let r = 0; r < reps; r++) {
						const lo = (r * 2 * k + 1) % (SLICE_ROWS - 2 * k - 2);
						const hi = lo + 2 * k + 1;
						const seek = sql.exec(
							`SELECT hk, sk FROM m_items WHERE ${RANGE} ORDER BY hk, sk LIMIT 1 OFFSET ?6`,
							routingHash,
							hk,
							sk(lo),
							hk,
							sk(hi),
							k,
						);
						const mid = seek.one();
						read += seek.rowsRead;
						const lower = sql.exec(`SELECT ${FACTS} FROM m_items WHERE ${RANGE}`, routingHash, hk, sk(lo), mid.hk, mid.sk);
						const lf = lower.one();
						read += lower.rowsRead;
						const upper = sql.exec(`SELECT ${FACTS} FROM m_items WHERE ${RANGE}`, routingHash, mid.hk, mid.sk, hk, sk(hi));
						const uf = upper.one();
						read += upper.rowsRead;
						for (const [lhk, lsk, f] of [
							[hk, sk(lo), lf],
							[mid.hk, mid.sk, uf],
						] as const) {
							const w = sql.exec(
								`INSERT OR REPLACE INTO m_split_stats VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ${estSql("?4", "?5", "?6", "?7", "?8", "?9")})`,
								routingHash,
								lhk,
								lsk,
								f.c,
								f.ib,
								f.kb,
								f.ic,
								f.ikb,
								f.ob,
							);
							read += w.rowsRead;
							written += w.rowsWritten;
						}
					}
				});
				return { read: read / reps, written: written / reps };
			});
			lines.push(
				`| Split, \`K = ${k}\` | ${(split.ms / reps).toFixed(2)} ms | ${split.result.read.toFixed(0)} | ${split.result.written.toFixed(0)} |`,
			);

			const refineReps = Math.max(2, Math.floor(20 / (k / 2_048)));
			const refine = await timeCall(slice, (state) => {
				let read = 0;
				for (let r = 0; r < refineReps; r++) {
					const lo = (r * 2 * k + 7) % (SLICE_ROWS - 2 * k - 2);
					const cursor = state.storage.sql.exec(
						`SELECT hk, sk, item_bytes, ttl_epoch_utc_seconds FROM m_items WHERE ${RANGE} ORDER BY hk, sk`,
						routingHash,
						hk,
						sk(lo),
						hk,
						sk(lo + 2 * k),
					);
					let best = Infinity;
					let acc = 0;
					for (const row of cursor) {
						acc += itemEstimate(
							row.item_bytes as number,
							(row.hk as ArrayBuffer).byteLength + (row.sk as ArrayBuffer).byteLength,
							row.ttl_epoch_utc_seconds,
						);
						best = Math.min(best, Math.abs(acc - 1e5));
					}
					read += cursor.rowsRead;
				}
				return { read: read / refineReps };
			});
			lines.push(
				`| Refine scan of \`2K\` rows, \`K = ${k}\` | ${(refine.ms / refineReps).toFixed(2)} ms | ${refine.result.read.toFixed(0)} | 0 |`,
			);
		}

		const reps = 5_000;
		const seekUpdate = await timeCall(slice, (state) => {
			const sql = state.storage.sql;
			let read = 0;
			let written = 0;
			state.storage.transactionSync(() => {
				for (let r = 0; r < reps; r++) {
					const target = sk((r * 7919) % SLICE_ROWS);
					const kb = hk.length + skBytes;
					const c = sql.exec(
						`UPDATE m_item_stats SET item_count = item_count + 1, item_bytes = item_bytes + ?4, key_bytes = key_bytes + ?5,
						        est_bytes = ${estSql("item_count + 1", "item_bytes + ?4", "key_bytes + ?5", "indexed_item_count", "indexed_key_bytes", "overflow_bytes")}
						  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = (
						        SELECT lower_hk, lower_sk FROM m_item_stats
						         WHERE routing_hash = ?1 AND (lower_hk, lower_sk) <= (?2, ?3)
						         ORDER BY lower_hk DESC, lower_sk DESC LIMIT 1)
						 RETURNING item_count, item_bytes, key_bytes, indexed_item_count, indexed_key_bytes, overflow_bytes, est_bytes`,
						routingHash,
						hk,
						target,
						20 + kb,
						kb,
					);
					c.toArray();
					read += c.rowsRead;
					written += c.rowsWritten;
				}
			});
			return { read: read / reps, written: written / reps };
		});
		const buckets = Math.ceil(SLICE_ROWS / K);
		lines.push(
			`| Bucket seek update, ${buckets} buckets | ${((seekUpdate.ms * 1000) / reps).toFixed(2)} µs | ${seekUpdate.result.read.toFixed(1)} | ${seekUpdate.result.written.toFixed(1)} |`,
		);

		const oldFacts = await timeCall(slice, (state) => {
			let read = 0;
			for (let r = 0; r < reps; r++) {
				const c = state.storage.sql.exec(
					`SELECT item_bytes, ttl_epoch_utc_seconds FROM m_items WHERE routing_hash = ? AND hk = ? AND sk = ?`,
					routingHash,
					hk,
					sk((r * 7919) % SLICE_ROWS),
				);
				c.toArray();
				read += c.rowsRead;
			}
			return { read: read / reps };
		});
		lines.push(
			`| Old-facts read (primary-key seek) | ${((oldFacts.ms * 1000) / reps).toFixed(2)} µs | ${oldFacts.result.read.toFixed(1)} | 0 |`,
		);

		const planner = await timeCall(slice, (state) => {
			const sql = state.storage.sql;
			const n = 4;
			const bucketCursor = sql.exec(
				`SELECT lower_hk, lower_sk, item_count, item_bytes, key_bytes, indexed_item_count, indexed_key_bytes, overflow_bytes
				   FROM m_item_stats WHERE routing_hash = ? ORDER BY lower_hk, lower_sk`,
				routingHash,
			);
			const rows = bucketCursor.toArray();
			let read = bucketCursor.rowsRead;
			const before: number[] = [];
			let total = 0;
			for (const b of rows) {
				before.push(total);
				total +=
					(b.item_bytes as number) +
					MODEL.kif * (b.key_bytes as number) +
					MODEL.bo * (b.item_count as number) +
					MODEL.ikf * (b.indexed_key_bytes as number) +
					MODEL.io * (b.indexed_item_count as number) +
					MODEL.oc * (b.overflow_bytes as number);
			}
			const scanned = new Set<number>();
			for (let i = 1; i < n; i++) {
				const target = (i * total) / n;
				const idx = before.findIndex((v, j) => j === before.length - 1 || before[j + 1] > target);
				if (scanned.has(idx)) {
					continue;
				}
				scanned.add(idx);
				const lower = rows[idx];
				const upper = rows[idx + 1];
				const cursor = sql.exec(
					`SELECT hk, sk, item_bytes, ttl_epoch_utc_seconds FROM m_items
					  WHERE routing_hash = ?1 AND (hk, sk) >= (?2, ?3) ${upper ? "AND (hk, sk) < (?4, ?5)" : ""} ORDER BY hk, sk`,
					...(upper
						? [routingHash, idx === 0 ? hk : lower.lower_hk, idx === 0 ? sk(0) : lower.lower_sk, upper.lower_hk, upper.lower_sk]
						: [routingHash, lower.lower_hk, lower.lower_sk]),
				);
				let acc = before[idx];
				let best = Infinity;
				for (const row of cursor) {
					acc += itemEstimate(
						row.item_bytes as number,
						(row.hk as ArrayBuffer).byteLength + (row.sk as ArrayBuffer).byteLength,
						row.ttl_epoch_utc_seconds,
					);
					best = Math.min(best, Math.abs(acc - target));
				}
				read += cursor.rowsRead;
			}
			return { read };
		});
		lines.push(`| Planner bucket path, \`N = 4\`, ${buckets} buckets | ${planner.ms.toFixed(1)} ms | ${planner.result.read} | 0 |`);

		const explain = await runInDurableObject(slice.stub, (_i: FokosDBPartitionDO, state: DurableObjectState) =>
			plans(state, [
				[
					"Range scan",
					`SELECT hk, sk, item_bytes, ttl_epoch_utc_seconds FROM m_items WHERE ${RANGE} ORDER BY hk, sk`,
					[routingHash, hk, sk(0), hk, end],
				],
				["Count", `SELECT COUNT(*) FROM m_items WHERE ${RANGE}`, [routingHash, hk, sk(0), hk, end]],
				["Split seek", `SELECT hk, sk FROM m_items WHERE ${RANGE} ORDER BY hk, sk LIMIT 1 OFFSET 10`, [routingHash, hk, sk(0), hk, end]],
				[
					"Old-facts read",
					`SELECT item_bytes, ttl_epoch_utc_seconds FROM m_items WHERE routing_hash = ? AND hk = ? AND sk = ?`,
					[routingHash, hk, sk(5)],
				],
				[
					"Last item before a bound",
					`SELECT hk, sk FROM m_items WHERE routing_hash = ?1 AND (hk, sk) < (?2, ?3) ORDER BY hk DESC, sk DESC LIMIT 1`,
					[routingHash, hk, sk(5)],
				],
				["Cleanup", `DELETE FROM m_item_stats WHERE routing_hash = ?`, [routingHash]],
			]),
		);
		console.log([...lines, "", ...explain].join("\n"));
	}
});

it("measures the promotion lookup", async () => {
	const stub = testPartitionStub(`item-size-model.${crypto.randomUUID()}`);
	const firstBuckets = 100_000;
	const routingHashes: Uint8Array[] = [];
	for (let i = 0; i < firstBuckets; i++) {
		routingHashes.push(crypto.getRandomValues(new Uint8Array(8)));
	}
	// Each routingHash has a different item count, so each first bucket has a different `est_bytes`.
	const counts = shuffled(firstBuckets).map((i) => i + 1);
	const data = "d".repeat(20);
	const seeded = await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
		const sql = state.storage.sql;
		sql.exec(ITEMS_SCHEMA);
		sql.exec(STATS_SCHEMA);
		state.storage.transactionSync(() => {
			for (let i = 0; i < firstBuckets; i++) {
				const routingHash = routingHashes[i];
				const count = counts[i];
				sql.exec(
					`INSERT INTO m_item_stats VALUES (?1, x'', x'', ?2, ?3, ?4, ?5, ?6, 0, ${estSql("?2", "?3", "?4", "?5", "?6", "0")})`,
					routingHash,
					count,
					count * 44,
					count * 24,
					Math.floor(count / 3),
					Math.floor(count / 3) * 24,
				);
				// One item for each routingHash, for the hash-key seek of the lookup.
				const hk = hashKey(8);
				const sk = sortKey(i, 16);
				sql.exec(
					`INSERT INTO m_items VALUES (?, ?, ?, ?, 0, 1, ?, ?, NULL, ?, ?)`,
					routingHash,
					hk,
					sk,
					i + 1,
					NOW_MS,
					NOW_MS,
					20 + hk.length + sk.length,
					data,
				);
			}
		});
		const types = sql.exec(`SELECT typeof(est_bytes) AS t, COUNT(*) AS n FROM m_item_stats GROUP BY 1`).toArray();
		const top = sql
			.exec(`SELECT est_bytes FROM m_item_stats WHERE lower_hk = x'' AND lower_sk = x'' ORDER BY est_bytes DESC LIMIT 6`)
			.toArray()
			.map((r) => r.est_bytes as number);
		return { types, top };
	});
	const slice: Slice = { stub, routingHash: new Uint8Array(), hk: new Uint8Array(), skBytes: 0 };
	const lookup = `SELECT routing_hash, est_bytes FROM m_item_stats
	                 WHERE lower_hk = x'' AND lower_sk = x'' AND est_bytes >= ?1
	                 ORDER BY est_bytes DESC LIMIT 5`;
	const seek = `SELECT hk FROM m_items WHERE routing_hash = ? ORDER BY hk, sk LIMIT 1`;
	const lines = [
		`est_bytes types: ${JSON.stringify(seeded.types)}`,
		"| Rows at or above `T` | Index query, time | Index query, rows read | Lookup with seeks, time | Seek rows read, each |",
		"| --- | --- | --- | --- | --- |",
	];
	// `T` above the largest value, at the third value, at the fifth value, and 0.
	for (const [label, t] of [
		["0", seeded.top[0] + 1],
		["3", seeded.top[2]],
		["5", seeded.top[4]],
		["all", 0],
	] as const) {
		const reps = 2_000;
		const index = await timeCall(slice, (state) => {
			let read = 0;
			let rows = 0;
			for (let i = 0; i < reps; i++) {
				const cursor = state.storage.sql.exec(lookup, t);
				rows += cursor.toArray().length;
				read += cursor.rowsRead;
			}
			return { read: read / reps, rows: rows / reps };
		});
		const full = await timeCall(slice, (state) => {
			let seekRead = 0;
			let seeks = 0;
			for (let i = 0; i < reps; i++) {
				for (const row of state.storage.sql.exec(lookup, t).toArray()) {
					const c = state.storage.sql.exec(seek, row.routing_hash);
					c.toArray();
					seekRead += c.rowsRead;
					seeks++;
				}
			}
			return { seekRead: seeks === 0 ? 0 : seekRead / seeks };
		});
		lines.push(
			`| ${label} (${index.result.rows} returned) | ${((index.ms * 1000) / reps).toFixed(1)} µs | ${index.result.read.toFixed(0)} | ${((full.ms * 1000) / reps).toFixed(1)} µs | ${full.result.seekRead.toFixed(1)} |`,
		);
	}
	const upsert = `INSERT INTO m_item_stats VALUES (?1, x'', x'', 1, ?2, ?3, 0, 0, 0, ${estSql("1", "?2", "?3", "0", "0", "0")})
	                  ON CONFLICT (routing_hash, lower_hk, lower_sk) DO UPDATE SET
	                    item_count = item_count + 1, item_bytes = item_bytes + ?2, key_bytes = key_bytes + ?3,
	                    est_bytes = ${estSql("item_count + 1", "item_bytes + ?2", "key_bytes + ?3", "indexed_item_count", "indexed_key_bytes", "overflow_bytes")}
	                  RETURNING item_count, item_bytes, key_bytes, indexed_item_count, indexed_key_bytes, overflow_bytes, est_bytes`;
	const reps = 5_000;
	lines.push("", "| First-bucket upsert of a hash-leaf write | Time | Rows read | Rows written |", "| --- | --- | --- | --- |");
	for (const [name, pick] of [
		["one routingHash", () => routingHashes[0]],
		["spread over all routingHashes", (i: number) => routingHashes[(i * 7919) % firstBuckets]],
	] as const) {
		const up = await timeCall(slice, (state) => {
			let read = 0;
			let written = 0;
			state.storage.transactionSync(() => {
				for (let i = 0; i < reps; i++) {
					const c = state.storage.sql.exec(upsert, pick(i), 44, 24);
					c.toArray();
					read += c.rowsRead;
					written += c.rowsWritten;
				}
			});
			return { read: read / reps, written: written / reps };
		});
		lines.push(`| ${name} | ${((up.ms * 1000) / reps).toFixed(2)} µs | ${up.result.read.toFixed(1)} | ${up.result.written.toFixed(1)} |`);
	}
	const plan = await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) =>
		plans(state, [
			["Lookup", lookup, [0]],
			["Seek", seek, [routingHashes[0]]],
			["Upsert", upsert, [routingHashes[0], 44, 24]],
		]),
	);
	console.log([`${firstBuckets} first buckets`, ...lines, ...plan].join("\n"));
});

it("counts the rows written with a promotion column that is NULL below a floor", async () => {
	const stub = testPartitionStub(`item-size-model.${crypto.randomUUID()}`);
	const lines = await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
		const sql = state.storage.sql;
		sql.exec(`
			CREATE TABLE m_promotion_stats (
				routing_hash BLOB    NOT NULL,
				lower_hk     BLOB    NOT NULL,
				lower_sk     BLOB    NOT NULL,
				item_count   INTEGER NOT NULL,
				item_bytes   INTEGER NOT NULL,
				promotion_est_bytes INTEGER,
				PRIMARY KEY (routing_hash, lower_hk, lower_sk)
			) WITHOUT ROWID, STRICT;
			CREATE INDEX m_promotion_stats_promotion ON m_promotion_stats (promotion_est_bytes) WHERE promotion_est_bytes IS NOT NULL;`);
		const est = (ic: string, ib: string) => `ceil((${ib}) + ${MODEL.bo} * (${ic}))`;
		const promotionEst = (ic: string, ib: string) => `CASE WHEN ${est(ic, ib)} >= ?9 THEN ${est(ic, ib)} ELSE NULL END`;
		const upsert = `INSERT INTO m_promotion_stats VALUES (?1, ?2, ?3, 1, ?4, ${promotionEst("1", "?4")})
		                  ON CONFLICT (routing_hash, lower_hk, lower_sk) DO UPDATE SET
		                    item_count = item_count + 1, item_bytes = item_bytes + ?4,
		                    promotion_est_bytes = ${promotionEst("item_count + 1", "item_bytes + ?4")}
		                  RETURNING item_count, item_bytes, promotion_est_bytes, ${est("item_count", "item_bytes")} AS estimate`;
		const out = ["| Write | promotion_est_bytes before | promotion_est_bytes after | Rows written |", "| --- | --- | --- | --- |"];
		const floor = 10_000;
		const r = crypto.getRandomValues(new Uint8Array(8));
		const empty = new Uint8Array();
		const promotionEstOf = (hk: Uint8Array) => {
			const rows = sql.exec(`SELECT promotion_est_bytes FROM m_promotion_stats WHERE routing_hash = ? AND lower_hk = ? AND lower_sk = x''`, r, hk).toArray();
			return rows.length === 0 ? "no row" : String(rows[0].promotion_est_bytes);
		};
		const step = (name: string, hk: Uint8Array, delta: number, floorValue: number | null) => {
			const before = promotionEstOf(hk);
			// The statement binds the parameters ?1 to ?4 and ?9; ?5 to ?8 are unused.
			const c = sql.exec(upsert, r, hk, empty, delta, 0, 0, 0, 0, floorValue);
			c.toArray();
			out.push(`| ${name} | ${before} | ${promotionEstOf(hk)} | ${c.rowsWritten} |`);
		};
		step("Insert, below the floor", empty, 100, floor);
		step("Update, below to below", empty, 100, floor);
		step("Update, below to at or above", empty, 10_000, floor);
		step("Update, above to above (value changes)", empty, 100, floor);
		step("Update, above to below (floor raised)", empty, 100, 1_000_000);
		step("Update, below to below again", empty, 100, 1_000_000);
		step("Insert, NULL floor (range bucket)", KeyCodec.encode("a"), 100, null);
		step("Update, NULL floor (range bucket)", KeyCodec.encode("a"), 100, null);
		// An update that leaves promotion_est_bytes at the same non-NULL value.
		sql.exec(`UPDATE m_promotion_stats SET promotion_est_bytes = 5 WHERE routing_hash = ? AND lower_hk = x''`, r);
		const same = sql.exec(`UPDATE m_promotion_stats SET item_count = item_count + 1, promotion_est_bytes = 5 WHERE routing_hash = ? AND lower_hk = x''`, r);
		same.toArray();
		out.push(`| Update, same non-NULL value | 5 | 5 | ${same.rowsWritten} |`);
		for (const [name, value] of [
			["Delete, NULL", KeyCodec.encode("a")],
			["Delete, non-NULL", empty],
		] as const) {
			const d = sql.exec(`DELETE FROM m_promotion_stats WHERE routing_hash = ? AND lower_hk = ?`, r, value);
			d.toArray();
			out.push(`| ${name} | | | ${d.rowsWritten} |`);
		}
		const plan = plans(state, [
			[
				"Lookup",
				`SELECT routing_hash, promotion_est_bytes FROM m_promotion_stats WHERE promotion_est_bytes >= ?1 ORDER BY promotion_est_bytes DESC LIMIT 5`,
				[floor],
			],
		]);
		return [...out, ...plan];
	});
	console.log(lines.join("\n"));
});

it("counts the rows written by an item insert", async () => {
	const stub = testPartitionStub(`item-size-model.${crypto.randomUUID()}`);
	const lines = await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
		const sql = state.storage.sql;
		sql.exec(ITEMS_SCHEMA);
		sql.exec(LATEST_SCHEMA);
		const hk = hashKey(8);
		const routingHash = routingHashFor(hk);
		const out = ["| Insert | Rows written |", "| --- | --- |"];
		for (const [name, ttl, scanIndex] of [
			["items, no TTL", null, false],
			["items, TTL", TTL_SECONDS, false],
			["items with a scan index, no TTL", null, true],
			["items with a scan index, TTL", TTL_SECONDS, true],
		] as const) {
			if (scanIndex) {
				sql.exec(`CREATE INDEX IF NOT EXISTS m_items_scan ON m_items (routing_hash, hk, sk, item_bytes)`);
			}
			const sk = KeyCodec.encode(`${name}`);
			const c = sql.exec(
				`INSERT INTO m_items VALUES (?, ?, ?, ?, 0, 1, ?, ?, ?, 40, 'd')`,
				routingHash,
				hk,
				sk,
				out.length,
				NOW_MS,
				NOW_MS,
				ttl,
			);
			c.toArray();
			out.push(`| ${name} | ${c.rowsWritten} |`);
		}
		for (const deleted of [false, true]) {
			const c = sql.exec(
				`INSERT INTO m_latest_items VALUES (?, ?, ?, ?, 1, ?, 1, 0, NULL, 40, ?)`,
				routingHash,
				hk,
				KeyCodec.encode(`latest ${deleted}`),
				100 + out.length,
				deleted ? NOW_MS : null,
				deleted ? null : "d",
			);
			c.toArray();
			out.push(`| latest_items, ${deleted ? "tombstone" : "live"} | ${c.rowsWritten} |`);
		}
		return out;
	});
	console.log(lines.join("\n"));
});

it("measures a hash-leaf write with no bucket below m items", async () => {
	const M = 16;
	const FLOOR = 1e12; // Above each estimate here, so each bucket stores NULL.
	const schema = `
		CREATE TABLE m_hash_stats (
			routing_hash        BLOB    NOT NULL,
			lower_hk            BLOB    NOT NULL,
			lower_sk            BLOB    NOT NULL,
			item_count          INTEGER NOT NULL,
			item_bytes          INTEGER NOT NULL,
			key_bytes           INTEGER NOT NULL,
			indexed_item_count  INTEGER NOT NULL,
			indexed_key_bytes   INTEGER NOT NULL,
			overflow_bytes      INTEGER NOT NULL,
			promotion_est_bytes INTEGER,
			PRIMARY KEY (routing_hash, lower_hk, lower_sk)
		) WITHOUT ROWID, STRICT;
		CREATE INDEX m_hash_stats_promotion ON m_hash_stats (promotion_est_bytes) WHERE promotion_est_bytes IS NOT NULL;`;
	const promotionEst = (...f: [string, string, string, string, string, string]) =>
		`CASE WHEN ${estSql(...f)} >= ?5 THEN ${estSql(...f)} ELSE NULL END`;
	// ?1 routingHash, ?2 item-count delta, ?3 item-bytes delta, ?4 key-bytes delta, ?5 floor.
	const upsert = `INSERT INTO m_hash_stats VALUES (?1, x'', x'', ?2, ?3, ?4, 0, 0, 0, ${promotionEst("?2", "?3", "?4", "0", "0", "0")})
	                  ON CONFLICT (routing_hash, lower_hk, lower_sk) DO UPDATE SET
	                    item_count = item_count + ?2, item_bytes = item_bytes + ?3, key_bytes = key_bytes + ?4,
	                    promotion_est_bytes = ${promotionEst("item_count + ?2", "item_bytes + ?3", "key_bytes + ?4", "indexed_item_count", "indexed_key_bytes", "overflow_bytes")}
	                  RETURNING item_count, ${estSql("item_count", "item_bytes", "key_bytes", "indexed_item_count", "indexed_key_bytes", "overflow_bytes")} AS e`;
	const update = `UPDATE m_hash_stats SET
	                    item_count = item_count + ?2, item_bytes = item_bytes + ?3, key_bytes = key_bytes + ?4,
	                    promotion_est_bytes = ${promotionEst("item_count + ?2", "item_bytes + ?3", "key_bytes + ?4", "indexed_item_count", "indexed_key_bytes", "overflow_bytes")}
	                  WHERE routing_hash = ?1 AND lower_hk = x'' AND lower_sk = x''
	                  RETURNING item_count, ${estSql("item_count", "item_bytes", "key_bytes", "indexed_item_count", "indexed_key_bytes", "overflow_bytes")} AS e`;
	// ?1 routingHash, ?2 m, ?3 the read limit, ?4 unused, ?5 floor. It inserts the first bucket from the counted
	// facts when the routingHash holds at least m items, and it reads at most ?3 items.
	const create = `INSERT INTO m_hash_stats
	                  SELECT ?1, x'', x'', c, ib, kb, ic, ikb, ob, CASE WHEN e >= ?5 THEN e ELSE NULL END
	                    FROM (SELECT c, ib, kb, ic, ikb, ob, ${estSql("c", "ib", "kb", "ic", "ikb", "ob")} AS e
	                            FROM (SELECT ${FACTS}
	                                    FROM (SELECT hk, sk, item_bytes, ttl_epoch_utc_seconds FROM m_items
	                                           WHERE routing_hash = ?1 LIMIT ?3)))
	                   WHERE c >= ?2`;
	// The count before the create statement. It reads at most ?2 items.
	const count = `SELECT COUNT(*) AS n FROM (SELECT 1 FROM m_items WHERE routing_hash = ?1 LIMIT ?2)`;
	const insertItem = `INSERT INTO m_items VALUES (?, ?, ?, ?, 0, 1, ?, ?, NULL, ?, 'dddddddddddddddddddd') RETURNING item_bytes`;
	const updateItem = `UPDATE m_items SET item_bytes = item_bytes + 1, last_write_ts = ? WHERE routing_hash = ? AND hk = ? AND sk = ? RETURNING item_bytes`;
	const deleteItem = `DELETE FROM m_items WHERE routing_hash = ? AND hk = ? AND sk = ? RETURNING item_bytes`;
	const hk = hashKey(8);
	const kb = hk.length + sortKey(0, 16).length;
	const ib = 20 + kb;
	let nextId = 1;
	type Cost = { read: number; written: number };
	const add = (a: Cost, c: { rowsRead: number; rowsWritten: number }) => {
		a.read += c.rowsRead;
		a.written += c.rowsWritten;
	};
	type Design = "always" | "from m" | "from m, count first";
	/**
	 * The bucket statements of one write. "always" keeps a bucket for each routingHash. "from m" and
	 * "from m, count first" keep a bucket only for a routingHash that held `M` items at an insert.
	 */
	const bucketWrite = (sql: SqlStorage, cost: Cost, design: Design, r: Uint8Array, dc: number, db: number, dk: number, insert: boolean) => {
		if (design === "always") {
			const c = sql.exec(dc < 0 || !insert ? update : upsert, r, dc, db, dk, FLOOR);
			c.toArray();
			add(cost, c);
			return;
		}
		const c = sql.exec(update, r, dc, db, dk, FLOOR);
		const rows = c.toArray();
		add(cost, c);
		if (rows.length === 0 && insert && design === "from m, count first") {
			const n = sql.exec(count, r, M + 1);
			const counted = n.one().n as number;
			add(cost, n);
			if (counted < M) {
				return;
			}
		}
		if (rows.length === 0 && insert) {
			const k = sql.exec(create, r, M, M + 1, 0, FLOOR);
			k.toArray();
			add(cost, k);
		}
	};
	const lines = ["| Design | Write | Rows read | Rows written | Time |", "| --- | --- | --- | --- | --- |"];
	for (const design of ["always", "from m", "from m, count first"] as const) {
		const stub = testPartitionStub(`item-size-model.${crypto.randomUUID()}`);
		await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
			state.storage.sql.exec(ITEMS_SCHEMA);
			state.storage.sql.exec(schema);
		});
		const slice: Slice = { stub, routingHash: new Uint8Array(), hk, skBytes: 16 };
		const reps = 2_000;
		// Background: 100,000 routingHashes with one item each, so the B-trees are not tiny.
		await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
			const cost = { read: 0, written: 0 };
			state.storage.transactionSync(() => {
				for (let i = 0; i < 100_000; i++) {
					const r = crypto.getRandomValues(new Uint8Array(8));
					state.storage.sql.exec(insertItem, r, hk, sortKey(0, 16), nextId++, NOW_MS, NOW_MS, ib).toArray();
					bucketWrite(state.storage.sql, cost, design, r, 1, ib, kb, true);
				}
			});
		});
		const write = (name: string, fn: (sql: SqlStorage, cost: Cost) => void, count: number) =>
			timeCall(slice, (state) => {
				const cost = { read: 0, written: 0 };
				state.storage.transactionSync(() => {
					for (let i = 0; i < count; i++) {
						fn(state.storage.sql, cost);
					}
				});
				return cost;
			}).then(({ ms, result }) => {
				lines.push(
					`| ${design} | ${name} | ${(result.read / count).toFixed(1)} | ${(result.written / count).toFixed(1)} | ${((ms * 1000) / count).toFixed(1)} µs |`,
				);
			});
		// A new routingHash for each insert.
		await write(
			"Insert, first item of a routingHash",
			(sql, cost) => {
				const r = crypto.getRandomValues(new Uint8Array(8));
				const c = sql.exec(insertItem, r, hk, sortKey(0, 16), nextId++, NOW_MS, NOW_MS, ib);
				c.toArray();
				add(cost, c);
				bucketWrite(sql, cost, design, r, 1, ib, kb, true);
			},
			reps,
		);
		// A set of routingHashes that get their k-th item, for k = 2 to M + 1.
		const group = Array.from({ length: reps }, () => crypto.getRandomValues(new Uint8Array(8)));
		await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
			const sql = state.storage.sql;
			state.storage.transactionSync(() => {
				for (const r of group) {
					sql.exec(insertItem, r, hk, sortKey(1, 16), nextId++, NOW_MS, NOW_MS, ib).toArray();
					bucketWrite(sql, { read: 0, written: 0 }, design, r, 1, ib, kb, true);
				}
			});
			for (let k = 2; k <= M + 1; k++) {
				const cost = { read: 0, written: 0 };
				state.storage.transactionSync(() => {
					for (const r of group) {
						const c = sql.exec(insertItem, r, hk, sortKey(k, 16), nextId++, NOW_MS, NOW_MS, ib);
						c.toArray();
						add(cost, c);
						bucketWrite(sql, cost, design, r, 1, ib, kb, true);
					}
				});
				if ([2, M - 1, M, M + 1].includes(k)) {
					lines.push(`| ${design} | Insert, item ${k} of a routingHash | ${(cost.read / reps).toFixed(1)} | ${(cost.written / reps).toFixed(1)} | |`);
				}
			}
		});
		// Updates and deletes of items of the routingHashes with one item. Each update adds 1 byte.
		const singles = Array.from({ length: reps }, () => crypto.getRandomValues(new Uint8Array(8)));
		await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
			state.storage.transactionSync(() => {
				for (const r of singles) {
					state.storage.sql.exec(insertItem, r, hk, sortKey(0, 16), nextId++, NOW_MS, NOW_MS, ib).toArray();
					bucketWrite(state.storage.sql, { read: 0, written: 0 }, design, r, 1, ib, kb, true);
				}
			});
		});
		let u = 0;
		await write(
			"Update, routingHash with one item",
			(sql, cost) => {
				const r = singles[u++ % reps];
				const c = sql.exec(updateItem, NOW_MS, r, hk, sortKey(0, 16));
				c.toArray();
				add(cost, c);
				bucketWrite(sql, cost, design, r, 0, 1, 0, false);
			},
			reps,
		);
		await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
			const sql = state.storage.sql;
			const cost = { read: 0, written: 0 };
			state.storage.transactionSync(() => {
				for (const r of singles) {
					const c = sql.exec(deleteItem, r, hk, sortKey(0, 16));
					c.toArray();
					add(cost, c);
					if (design === "always") {
						const d = sql.exec(`DELETE FROM m_hash_stats WHERE routing_hash = ?`, r);
						d.toArray();
						add(cost, d);
					} else {
						bucketWrite(sql, cost, design, r, -1, -ib, -kb, false);
					}
				}
			});
			lines.push(`| ${design} | Delete, last item of a routingHash | ${(cost.read / reps).toFixed(1)} | ${(cost.written / reps).toFixed(1)} | |`);
		});
		if (design === "from m, count first") {
			const plan = await runInDurableObject(stub, (_i: FokosDBPartitionDO, state: DurableObjectState) =>
				plans(state, [
					["Count", count, [group[0], M + 1]],
					["Create", create, [group[0], M, M + 1, 0, FLOOR]],
				]),
			);
			lines.push(...plan);
		}
	}
	console.log(lines.join("\n"));
});
