/**
 * Counts the heap bytes of each case of `client-creation-cases.ts` in Node. workerd gives no heap
 * statistics, and both use V8. Run it with `pnpm --filter fokosdb bench:alloc`.
 *
 * - Allocated: all the bytes that one call puts on the heap, the temporary objects included. This is
 *   the work that the garbage collector gets for each request. The young generation is large, and the
 *   script checks that no collection ran while it counted.
 * - Retained: the bytes that the result of one call keeps alive, until the request drops it.
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

const N = 20_000;
const WARMUP = 100_000;

// Node has no `cloudflare:workers`. The cases never send a request, so an empty `env` is enough.
const { outputFiles } = await build({
	entryPoints: [resolve(import.meta.dirname, "client-creation-cases.ts")],
	bundle: true,
	format: "esm",
	platform: "node",
	write: false,
	plugins: [
		{
			name: "cloudflare-workers",
			setup(b) {
				b.onResolve({ filter: /^cloudflare:workers$/ }, () => ({ path: "env", namespace: "cf" }));
				b.onLoad({ filter: /.*/, namespace: "cf" }, () => ({ contents: "export const env = {};" }));
			},
		},
	],
});
const bundle = join(mkdtempSync(join(tmpdir(), "fokos-bench-")), "cases.mjs");
writeFileSync(bundle, outputFiles[0].text);
const { CASES } = await import(pathToFileURL(bundle).href);

const used = () => getHeapStatistics().used_heap_size;
const sink = Array.from({ length: 1024 });
const kept = Array.from({ length: N });

function allocated(fn) {
	gc();
	const profiler = new GCProfiler();
	profiler.start();
	const before = used();
	for (let i = 0; i < N; i++) {
		sink[i & 1023] = fn();
	}
	const after = used();
	const gcs = profiler.stop().statistics.length;
	if (gcs > 0) {
		throw new Error(`${gcs} garbage collections ran while the script counted; make --max-semi-space-size larger`);
	}
	return (after - before) / N;
}

function retained(fn) {
	gc();
	const before = used();
	for (let i = 0; i < N; i++) {
		kept[i] = fn();
	}
	gc();
	const after = used();
	kept.fill(undefined);
	return (after - before) / N;
}

const rows = [];
for (const [name, fn] of CASES) {
	for (let i = 0; i < WARMUP; i++) {
		sink[i & 1023] = fn();
	}
	rows.push(`| ${name} | ${allocated(fn).toFixed(0)} | ${retained(fn).toFixed(0)} |`);
}
console.log(
	[
		`Node ${process.version}, heap bytes per call, mean of ${N} calls after ${WARMUP} warm-up calls`,
		"| Case | Allocated | Retained |",
		"| --- | ---: | ---: |",
		...rows,
	].join("\n"),
);
