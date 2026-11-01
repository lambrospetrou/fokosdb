#!/usr/bin/env node
// Prints a per-module size breakdown for each entry bundle of packages/fokosdb.
//
// tsdown reports only the size of each emitted file, not the modules inside it. This script runs the
// same tsdown build as `pnpm build` with a probe plugin that reads `chunk.modules` of the rolldown
// output: `renderedLength` is the number of bytes a module contributes to its chunk. The probe walks
// the import graph of each entry the same way the `check-client-bundle` plugin of tsdown.config.ts
// does, so the numbers match what a consumer ships.
//
// The build here replaces the config plugins with the probe, so the size budget of the client can
// never make this report fail. The dts build is off, because it holds no JavaScript.
//
// Usage:
//   node tools/bundle-breakdown.js [--entry <filter>] [--all]
//     --entry <filter>  Report only the entries whose name contains <filter>.
//     --all             List every module, not only the largest 25.

import { createRequire } from "node:module";
import { join, relative, dirname, sep } from "node:path";
import { gzipSync } from "node:zlib";

const REPO_ROOT = join(import.meta.dirname, "..");
const PKG_DIR = join(REPO_ROOT, "packages/fokosdb");
const pkgRequire = createRequire(join(PKG_DIR, "package.json"));

const { build } = await import(pkgRequire.resolve("tsdown"));
const { transformSync } = pkgRequire("esbuild");

let topN = 25;
let entryFilter = null;
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
	if (argv[i] === "--all") topN = Number.POSITIVE_INFINITY;
	else if (argv[i] === "--entry") entryFilter = argv[++i];
	else if (argv[i]?.startsWith("--entry=")) entryFilter = argv[i].slice("--entry=".length);
	else {
		console.error(`Unknown argument: ${argv[i]}\nUsage: node tools/bundle-breakdown.js [--entry <filter>] [--all]`);
		process.exit(1);
	}
}

const graphs = [];
await build({
	cwd: PKG_DIR,
	dts: false,
	report: false,
	logLevel: "silent",
	plugins: [
		{
			name: "bundle-breakdown",
			generateBundle(_options, bundle) {
				for (const entry of Object.values(bundle)) {
					if (entry.type !== "chunk" || !entry.isEntry || !entry.fileName.endsWith(".js")) continue;
					const chunks = new Set([entry]);
					const externals = new Set();
					const pending = [entry];
					while (pending.length > 0) {
						const chunk = pending.pop();
						for (const imported of [...chunk.imports, ...chunk.dynamicImports]) {
							const target = bundle[imported];
							if (target?.type !== "chunk") externals.add(imported);
							else if (!chunks.has(target)) {
								chunks.add(target);
								pending.push(target);
							}
						}
					}
					graphs.push({ name: entry.name, chunks: [...chunks], externals: [...externals].sort() });
				}
			},
		},
	],
});

if (graphs.length === 0) {
	console.error("The build produced no JavaScript entries.");
	process.exit(1);
}

/** Shortens a module id to a display path: `src/...` for sources, `name/rest` for packages. */
function displayPath(id) {
	if (id.startsWith(PKG_DIR + sep)) return relative(PKG_DIR, id);
	const pnpm = id.match(/node_modules\/\.pnpm\/[^/]+\/node_modules\/(.+)$/);
	if (pnpm) return pnpm[1];
	const nm = id.match(/node_modules\/(.+)$/);
	if (nm) return nm[1];
	return relative(REPO_ROOT, id);
}

/** Groups `src/` modules by their directory and packages by their name. */
function groupOf(path) {
	if (path.startsWith("src" + sep)) return dirname(path);
	const idx = path.indexOf(sep);
	return idx === -1 ? path : path.slice(0, idx);
}

const kB = (bytes) => `${(bytes / 1024).toFixed(1)} kB`.padStart(9);

/** Prints a `BYTES PCT BAR LABEL` table sorted by size. */
function printTable(rows, total) {
	const maxBytes = rows[0]?.[0] ?? 0;
	for (const [bytes, label] of rows) {
		const pct = (bytes / total) * 100;
		const bar = "#".repeat(Math.max(1, Math.round((bytes / maxBytes) * 20)));
		console.log(`  ${kB(bytes)}  ${pct.toFixed(0).padStart(3)}%  ${bar.padEnd(20)} ${label}`);
	}
}

const order = ["client/index", "server/index", "sharding/client/index", "sharding/server/index"];
graphs.sort((a, b) => {
	const [ia, ib] = [order.indexOf(a.name), order.indexOf(b.name)];
	return (ia === -1 ? order.length : ia) - (ib === -1 ? order.length : ib) || a.name.localeCompare(b.name);
});

const selected = graphs.filter((graph) => !entryFilter || graph.name.includes(entryFilter));
if (selected.length === 0) {
	console.error(`No entry contains "${entryFilter}". Entries: ${graphs.map((graph) => graph.name).join(", ")}`);
	process.exit(1);
}

console.log("Each entry ships as its chunks merged: a chunk that two entries import counts toward both.\n");

for (const graph of selected) {
	// renderedLength counts UTF-16 code units; emitted code is ASCII, so it equals rendered bytes.
	// The byte totals below come from the chunk code itself, not from renderedLength.
	const modules = new Map();
	for (const chunk of graph.chunks) {
		for (const [id, mod] of Object.entries(chunk.modules)) {
			if (mod.renderedLength === 0) continue;
			modules.set(id, (modules.get(id) ?? 0) + mod.renderedLength);
		}
	}
	const rawBytes = graph.chunks.reduce((total, chunk) => total + Buffer.byteLength(chunk.code), 0);
	const attributed = [...modules.values()].reduce((a, b) => a + b, 0);
	const minified = graph.chunks.map((chunk) => transformSync(chunk.code, { minify: true, target: "esnext" }).code);
	const minBytes = minified.reduce((total, code) => total + Buffer.byteLength(code), 0);
	const gzipBytes = gzipSync(minified.join("\n")).byteLength;

	const groups = new Map();
	for (const [id, bytes] of modules) {
		const key = groupOf(displayPath(id));
		groups.set(key, (groups.get(key) ?? 0) + bytes);
	}

	console.log(`${graph.name}  (${graph.chunks.length} chunks, ${modules.size} modules)`);
	console.log(`  ${kB(rawBytes)} raw ${kB(minBytes)} min ${kB(gzipBytes)} min+gzip`);
	console.log(`  chunks: ${graph.chunks.map((chunk) => chunk.fileName).join(", ")}`);
	if (graph.externals.length > 0) console.log(`  externals: ${graph.externals.join(", ")}`);
	console.log();
	printTable(
		[...groups.entries()].sort((a, b) => b[1] - a[1]).map(([label, bytes]) => [bytes, label]),
		attributed,
	);
	console.log();
	const top = [...modules.entries()].sort((a, b) => b[1] - a[1]).slice(0, topN);
	printTable(
		top.map(([id, bytes]) => [bytes, displayPath(id)]),
		attributed,
	);
	if (modules.size > top.length) console.log(`  ... and ${modules.size - top.length} more modules (--all to list them)`);
	const glue = rawBytes - attributed;
	if (glue > 512) console.log(`  chunk glue (imports, wrappers): ${(glue / 1024).toFixed(1)} kB`);
	console.log();
}
