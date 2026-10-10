/**
 * Measures the expression engine inside a `PartitionDO`: the compile, the partition path, and the SQLite
 * statement over the `items` table of that partition. Run it with
 * `pnpm --filter fokosdb bench:workerd expression`.
 *
 * The columns:
 * - Tree B: the JSON bytes of the expression tree.
 * - SQL B: the characters of the statement that SQLite gets.
 * - Binds: the binding descriptors of the plan.
 * - Compile: the compiler alone.
 * - Path: the partition path of `prepareCase`, which is all the JavaScript work before `sql.exec`.
 * - SQLite first: one `sql.exec` of a statement text that SQLite did not see before, so it includes the
 *   prepare. Each call adds a different comment to the statement.
 * - SQLite again: one `sql.exec` of the same statement text.
 *
 * A condition, an update probe, and a projection read one item. A query scans BENCH_ITEM_COUNT items.
 * Timers in workerd advance in whole milliseconds, so each sample times a batch of at least MIN_BATCH_MS.
 * workerd gives no heap statistics: `expression-alloc.mjs` counts the heap bytes of the same cases.
 */
import { runInDurableObject } from "cloudflare:test";
import { expect, it } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { makeStub } from "../partition-do/helpers.js";
import {
	BENCH_HASH_KEY,
	BENCH_ITEM_COUNT,
	CASES,
	benchDocument,
	benchSortKey,
	compileCase,
	prepareCase,
	treeBytes,
} from "./expression-cases.js";

const MIN_BATCH_MS = 100;
const SAMPLES = 5;
// Each result goes into this ring, so the optimizer cannot remove the work.
const sink: unknown[] = Array.from<unknown>({ length: 1024 });

/** The median µs per call of `fn`. */
function measure(fn: () => unknown): number {
	const run = (rounds: number) => {
		for (let r = 0; r < rounds; r++) {
			sink[r & 1023] = fn();
		}
	};
	// Warm up, and find the number of rounds that takes at least MIN_BATCH_MS.
	let rounds = 1;
	for (;;) {
		const start = performance.now();
		run(rounds);
		if (performance.now() - start >= MIN_BATCH_MS) {
			break;
		}
		rounds *= 2;
	}
	const samples: number[] = [];
	for (let s = 0; s < SAMPLES; s++) {
		const start = performance.now();
		run(rounds);
		samples.push(((performance.now() - start) * 1e3) / rounds);
	}
	samples.sort((a, b) => a - b);
	return samples[Math.floor(SAMPLES / 2)];
}

it("expression compile and SQLite statements in a PartitionDO", async () => {
	const { ctx, stub } = makeStub();
	const rows = await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
		for (let i = 0; i < BENCH_ITEM_COUNT; i++) {
			const put = await instance.apiPutItem(ctx, {
				hashKey: BENCH_HASH_KEY,
				sortKey: benchSortKey(i),
				data: JSON.stringify(benchDocument(i)),
				kind: "json",
			});
			expect(put.value.outcome).toBe("ok");
		}
		const sql = state.storage.sql;
		return CASES.map((c) => {
			const { sql: statement, params, bindingCount } = prepareCase(c);
			const resultRows = sql.exec(statement, ...params).toArray();
			// A statement that reads no item measures nothing.
			expect(resultRows.length, c.name).toBe(c.kind === "query" ? BENCH_ITEM_COUNT : 1);
			let unique = 0;
			const first = measure(() => sql.exec(`${statement}\n-- ${unique++}`, ...params).toArray());
			const again = measure(() => sql.exec(statement, ...params).toArray());
			const compile = measure(() => compileCase(c));
			const path = measure(() => prepareCase(c));
			return `| ${c.name} | ${treeBytes(c)} | ${statement.length} | ${bindingCount} | ${compile.toFixed(1)} | ${path.toFixed(1)} | ${first.toFixed(1)} | ${again.toFixed(1)} |`;
		});
	});
	expect(sink.some((v) => v !== undefined)).toBe(true);
	console.log(
		[
			`workerd, PartitionDO with ${BENCH_ITEM_COUNT} items, µs per call, median of ${SAMPLES} batches of at least ${MIN_BATCH_MS} ms`,
			"| Case | Tree B | SQL B | Binds | Compile | Path | SQLite first | SQLite again |",
			"| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
			...rows,
		].join("\n"),
	);
});
