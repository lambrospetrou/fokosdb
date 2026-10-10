/**
 * Counts the heap bytes of each case of `expression-cases.ts` in Node. workerd gives no heap statistics,
 * and both use V8. Run it with `pnpm --filter fokosdb bench:alloc:expression`.
 *
 * Each column is all the bytes that one call puts on the heap, the temporary objects included. This is the
 * work that the garbage collector gets for each expression. The young generation is large, and the script
 * accepts only a count that no collection disturbed.
 *
 * - Validate, Identity, Compile: the three functions of the expression engine, each alone. The compile
 *   includes one validation, and no identity.
 * - Path: the partition path of `prepareCase`. The script reads the statement to its end, as SQLite does,
 *   so that the count includes the flat copy of the statement text.
 * - Kept: the bytes that the result of the partition path keeps alive, until the request drops it.
 */
import { build } from "esbuild";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { GCProfiler, getHeapStatistics } from "node:v8";

if (typeof globalThis.gc !== "function") {
	throw new Error("run with node --expose-gc");
}

const MAX_CALLS = 2_000;
const WARMUP = 3_000;
const MIN_CALLS = 8;

const { outputFiles } = await build({
	entryPoints: [resolve(import.meta.dirname, "expression-cases.ts")],
	bundle: true,
	format: "esm",
	platform: "node",
	write: false,
});
const bundle = join(mkdtempSync(join(tmpdir(), "fokos-bench-")), "cases.mjs");
writeFileSync(bundle, outputFiles[0].text);
const { CASES, validateCase, identityCase, compileCase, prepareCase } = await import(pathToFileURL(bundle).href);

const used = () => {
	const heap = getHeapStatistics();
	return heap.used_heap_size + heap.external_memory;
};
const sink = Array.from({ length: 1024 });

/**
 * Collects, and waits until the sweep of the buffers outside the heap is complete. A sweep that ends in
 * the middle of a count makes the count too low.
 */
async function settle() {
	for (let i = 0; i < 3; i++) {
		gc();
		await new Promise((done) => setTimeout(done, 10));
	}
}

async function allocated(fn) {
	for (let i = 0; i < WARMUP; i++) {
		sink[i & 1023] = fn();
	}
	// A count with fewer calls follows each count that a collection disturbed. A call that makes a large
	// buffer outside the heap starts a collection long before the young generation is full.
	for (let calls = MAX_CALLS; calls >= MIN_CALLS; calls = Math.floor(calls / 2)) {
		sink.fill(undefined);
		await settle();
		const profiler = new GCProfiler();
		profiler.start();
		const before = used();
		for (let i = 0; i < calls; i++) {
			sink[i & 1023] = fn();
		}
		const after = used();
		if (profiler.stop().statistics.length === 0) {
			return (after - before) / calls;
		}
	}
	throw new Error(`a garbage collection ran in each count, also with ${MIN_CALLS} calls; make --max-semi-space-size larger`);
}

async function kept(fn) {
	const results = Array.from({ length: 200 });
	await settle();
	const before = used();
	for (let i = 0; i < results.length; i++) {
		results[i] = fn();
	}
	await settle();
	return (used() - before) / results.length;
}

// A statement that the compiler builds from parts is a tree of string parts until a reader makes it flat.
const path = (c) => {
	const prepared = prepareCase(c);
	// The result is kept, so that the optimizing compiler does not remove the read.
	prepared.end = prepared.sql.indexOf("\u0001");
	return prepared;
};
const kib = (bytes) => (bytes / 1024).toFixed(1);

const rows = [];
for (const c of CASES) {
	const counts = [];
	for (const fn of [() => validateCase(c), () => identityCase(c), () => compileCase(c), () => path(c)]) {
		counts.push(kib(await allocated(fn)));
	}
	rows.push(`| ${c.name} | ${counts.join(" | ")} | ${kib(await kept(() => path(c)))} |`);
}
console.log(
	[
		`Node ${process.version}, heap KiB per call, mean of ${MIN_CALLS} to ${MAX_CALLS} calls after ${WARMUP} warm-up calls`,
		"| Case | Validate | Identity | Compile | Path | Kept |",
		"| --- | ---: | ---: | ---: | ---: | ---: |",
		...rows,
	].join("\n"),
);
