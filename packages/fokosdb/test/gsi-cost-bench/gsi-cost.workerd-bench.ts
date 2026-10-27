/**
 * Measures the storage and the SQLite row writes of GSI materialization in workerd.
 * `pnpm bench:workerd` runs it. The numbers are from the local runtime, not from production.
 */
import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { expect, it, vi } from "vitest";
import { partitionStubByName } from "../../src/shared/do-stubs.js";
import { gsiItemSortKey } from "../../src/shared/gsi/sort-key.js";
import { gsiTableConfig, type FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { FokosRouter } from "../../src/sharding/router.js";
import { kb, makeStub, materializationOf } from "../partition-do/helpers.js";

const ROWS = 20_000;

/** The bytes that `ROWS` rows add to the database, for each row. */
async function bytesPerRow(stub: DurableObjectStub, insert: (sql: SqlStorage, i: number) => void): Promise<number> {
	return await runInDurableObject(stub, (_i: unknown, state: DurableObjectState) => {
		const sql = state.storage.sql;
		const before = sql.databaseSize;
		state.storage.transactionSync(() => {
			for (let i = 0; i < ROWS; i++) {
				insert(sql, i);
			}
		});
		return (sql.databaseSize - before) / ROWS;
	});
}

function gsiRoot(base: FokosDBRouteContext, indexName: string, hashKey: string) {
	const table = gsiTableConfig(
		{ topology: base.topology, rangeConfig: base.rangeConfig, policy: base.policy, policyVersion: base.policyVersion },
		indexName,
	);
	const ctx = new FokosRouter(table.topology, table.rangeConfig, table.policy, table.policyVersion).rootContext(kb(hashKey));
	return { ctx, stub: partitionStubByName(env, ctx, ctx.doName) };
}

it("row sizes", async () => {
	const base = makeStub({ captureChanges: true, nsMat: "GSI_MATERIALIZATION_DO" });
	await base.rpc.apiGetItem(base.ctx, { hashKey: kb("x"), sortKey: kb("s") });
	const mat = materializationOf(base.ctx, "x");
	const hex = (i: number) => i.toString(16).padStart(64, "0");
	const sizes = {
		producer_fences: await bytesPerRow(base.stub, (sql, i) => sql.exec(`INSERT INTO producer_fences VALUES (?, ?)`, hex(i), 3)),
		change_positions: await bytesPerRow(mat, (sql, i) => sql.exec(`INSERT INTO change_positions VALUES (?, ?, ?)`, i + 1, i, i)),
		target_index_keys: await bytesPerRow(mat, (sql, i) =>
			sql.exec(
				`INSERT INTO target_index_keys (item_id, target_id, index_hk, index_sk) VALUES (?, ?, ?, ?)`,
				i,
				"byStatus",
				kb("OPEN"),
				kb(`2026-10-24T17:46:00.${String(i).padStart(6, "0")}Z`),
			),
		),
		gsi_tombstone: await bytesPerRow(base.stub, (sql, i) =>
			sql.exec(
				`INSERT INTO items (hk, sk, data_kind, v, est_row_bytes, data) VALUES (?, ?, 0, 1, 0, x'')`,
				kb("OPEN"),
				gsiItemSortKey(kb(`2026-10-24T17:46:00.${String(i).padStart(6, "0")}Z`), kb(`order-${String(i).padStart(8, "0")}`), kb("s")),
			),
		),
	};
	console.log(`bytes per row, ${ROWS} rows:\n${JSON.stringify(sizes, null, 2)}`);
});

type Counter = { cursors: { query: string; cursor: SqlStorageCursor<Record<string, SqlStorageValue>> }[] };
const counters = new Map<string, Counter>();
const statements: string[] = [];

/** Counts the rows that each later `sql.exec` of the DO writes. */
async function count(name: string, stub: DurableObjectStub): Promise<void> {
	const counter: Counter = { cursors: [] };
	counters.set(name, counter);
	await runInDurableObject(stub, (_i: unknown, state: DurableObjectState) => {
		const sql = state.storage.sql;
		const exec = sql.exec.bind(sql);
		sql.exec = ((query: string, ...bindings: unknown[]) => {
			const cursor = exec(query, ...(bindings as SqlStorageValue[]));
			counters.get(name)?.cursors.push({ query, cursor });
			return cursor;
		}) as typeof sql.exec;
	});
}

function written(): Record<string, number> {
	const out: Record<string, number> = {};
	for (const [name, c] of counters) {
		out[name] = c.cursors.reduce((n, { cursor }) => n + cursor.rowsWritten, 0);
		for (const { query, cursor } of c.cursors) {
			if (cursor.rowsWritten > 0) {
				statements.push(`${name} ${cursor.rowsWritten} ${query.replace(/\s+/g, " ").trim().slice(0, 100)}`);
			}
		}
		c.cursors = [];
	}
	return out;
}

it("rows written for each base write, with 2 GSIs", async () => {
	const t = makeStub({ captureChanges: true, nsMat: "GSI_MATERIALIZATION_DO" });
	const put = (data: object) => t.rpc.apiPutItem(t.ctx, { hashKey: kb("o1"), sortKey: kb("s"), data: JSON.stringify(data), kind: "json" });
	await t.rpc.apiGetItem(t.ctx, { hashKey: kb("o1"), sortKey: kb("s") });
	const mat = materializationOf(t.ctx, "o1");
	await runInDurableObject(mat, () => {});
	await count("base", t.stub);
	await count("mat", mat);
	const roots = {
		open: gsiRoot(t.ctx, "byStatus", "OPEN"),
		closed: gsiRoot(t.ctx, "byStatus", "CLOSED"),
		owner: gsiRoot(t.ctx, "byOwner", "u1"),
	};
	// One GSI root holds every hash key, so the OPEN and CLOSED entries share one DO.
	expect(roots.open.ctx.doName).toBe(roots.closed.ctx.doName);
	const entries = async (r: { stub: DurableObjectStub }, hk: string) =>
		await runInDurableObject(r.stub, (_i: unknown, s: DurableObjectState) => {
			try {
				return s.storage.sql
					.exec<{ d: string }>(`SELECT json(data) AS d FROM items WHERE hk = ?`, kb(hk))
					.toArray()
					.map((x) => x.d);
			} catch {
				return [];
			}
		});
	await count("byStatus", roots.open.stub);
	await count("byOwner", roots.owner.stub);
	const results: Record<string, Record<string, number>> = {};
	const step = async (label: string, run: () => Promise<unknown>, done: () => Promise<void>) => {
		written();
		const start = performance.now();
		await run();
		await vi.waitFor(done, { timeout: 10_000, interval: 5 });
		const ms = performance.now() - start;
		await new Promise((r) => setTimeout(r, 300));
		results[label] = { ...written(), visibleMs: Math.round(ms) };
		console.log(`${label}:\n  ${statements.splice(0).join("\n  ")}`);
	};
	await step(
		"create",
		() => put({ status: "OPEN", at: "t1", owner: "u1" }),
		async () => {
			expect(await entries(roots.open, "OPEN")).toHaveLength(1);
			expect(await entries(roots.owner, "u1")).toHaveLength(1);
		},
	);
	await step(
		"update, same GSI keys",
		() => put({ status: "OPEN", at: "t1", owner: "u1", n: 2 }),
		async () => {
			expect((await entries(roots.open, "OPEN"))[0]).toContain('"n":2');
		},
	);
	await step(
		"update, key move on byStatus",
		() => put({ status: "CLOSED", at: "t2", owner: "u1" }),
		async () => {
			expect(await entries(roots.open, "OPEN")).toHaveLength(0);
			expect(await entries(roots.closed, "CLOSED")).toHaveLength(1);
		},
	);
	await step(
		"delete",
		() => t.rpc.apiDeleteItem(t.ctx, { hashKey: kb("o1"), sortKey: kb("s") }),
		async () => {
			expect(await entries(roots.closed, "CLOSED")).toHaveLength(0);
			expect(await entries(roots.owner, "u1")).toHaveLength(0);
		},
	);
	console.log(`rows written per DO:\n${JSON.stringify(results, null, 2)}`);
});

it("gsiWriteEntries call duration", async () => {
	const t = makeStub();
	const lines: string[] = [];
	const epoch = 1;
	for (const n of [1, 10, 100]) {
		const samples: number[] = [];
		for (let rep = 0; rep < 30; rep++) {
			const items = Array.from({ length: n }, (_, i) => ({
				opIndex: i,
				hashKey: kb("OPEN"),
				sortKey: gsiItemSortKey(kb(`t${rep}`), kb(`o${i}`), kb("s")),
				op: "put" as const,
				data: JSON.stringify({ status: "OPEN", i, pad: "x".repeat(200) }),
				kind: "json" as const,
				baseRevision: 1,
			}));
			const start = performance.now();
			await t.stub.gsiWriteEntries(t.ctx, { producerId: "p".repeat(64), epoch, ancestorFences: [], items });
			samples.push(performance.now() - start);
		}
		samples.sort((a, b) => a - b);
		lines.push(`${n} entries: median ${samples[15].toFixed(2)} ms, max ${samples[29].toFixed(2)} ms`);
	}
	console.log(`gsiWriteEntries, local workerd:\n${lines.join("\n")}`);
});
