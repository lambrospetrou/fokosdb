import { gzipSync } from "node:zlib";
import { transformSync } from "esbuild";
import { defineConfig } from "tsdown";

/** Source directory that the client entry must never reach. */
const CLIENT_FORBIDDEN_SRC = "src/server/";

/** Bare specifiers that the client is allowed to import at runtime. */
const CLIENT_ALLOWED_EXTERNALS = [/^cloudflare:workers$/, /^xxhash-wasm$/, /^durable-utils\//];

/**
 * Upper bound for the client entry and every chunk that it imports, in MINIFIED bytes.
 *
 * Minified, because that is the size a consumer ships: their bundler merges the entry with its chunks
 * and minifies the result. Gating the unminified size instead makes a doc comment cost the same as
 * code, so writing down why something works competes with the budget meant to catch a Durable Object
 * class reaching the client — and that mistake is tens of kB minified, which this still catches.
 */
const CLIENT_MAX_BYTES = 72 * 1024;

export default defineConfig({
	entry: {
		"client/index": "src/client/index.ts",
		"server/index": "src/server/index.ts",
	},
	format: ["esm"],
	dts: true,
	sourcemap: true,
	// Resolving these here (in Node) instead of in the consumer's wrangler/esbuild build silently
	// ships broken code, e.g. xxhash-wasm's ArrayBuffer-based Node loader instead of its
	// workerd-safe variant. Keep them external.
	external: ["cloudflare:workers", "cloudflare:test", "xxhash-wasm", /^durable-utils\//],
	plugins: [
		{
			// Fails the build if the client bundle grows past its budget or contains server code.
			//
			// The client is made for Workers that have no Durable Object bindings, so its bundle must never
			// contain a Durable Object implementation. Nothing else catches that: the client and the server
			// entries share chunks, and one accidental value import (instead of `import type`) silently moves
			// a whole DO class into the client graph. Rolldown reports the real module graph, so the check
			// walks the chunks that each entry imports and inspects the modules behind them.
			name: "check-client-bundle",
			generateBundle(_options, bundle) {
				const graphs = Object.values(bundle).flatMap((entry) => {
					// The declaration build emits its own bundle, which holds no JavaScript entry.
					if (entry.type !== "chunk" || !entry.isEntry || !entry.fileName.endsWith(".js")) return [];

					const chunks = new Set([entry]);
					const externals = new Set<string>();
					const pending = [entry];
					while (pending.length > 0) {
						const chunk = pending.pop()!;
						for (const imported of [...chunk.imports, ...chunk.dynamicImports]) {
							const target = bundle[imported];
							if (target?.type !== "chunk") {
								externals.add(imported);
							} else if (!chunks.has(target)) {
								chunks.add(target);
								pending.push(target);
							}
						}
					}
					return [{ entry, chunks: [...chunks], externals: [...externals] }];
				});
				if (graphs.length === 0) return;

				for (const { entry, chunks, externals } of graphs) {
					if (entry.name !== "client/index") continue;

					const serverModules = chunks
						.flatMap((chunk) => chunk.moduleIds)
						.filter((id) => id.includes(CLIENT_FORBIDDEN_SRC))
						.sort();
					if (serverModules.length > 0) {
						this.error(
							`The client bundle contains server modules. Import them with \`import type\` only, or move the shared part to src/shared/:\n  ${serverModules.join("\n  ")}`,
						);
					}

					const unexpected = externals.filter((id) => !CLIENT_ALLOWED_EXTERNALS.some((allowed) => allowed.test(id))).sort();
					if (unexpected.length > 0) {
						this.error(
							`The client bundle imports packages that are not on the allow list. Add them to tsdown.config.ts if they belong there:\n  ${unexpected.join("\n  ")}`,
						);
					}

					const bytes = minifiedBytes(chunks);
					if (bytes > CLIENT_MAX_BYTES) {
						this.error(`The client bundle is ${(bytes / 1024).toFixed(1)} kB minified, over the ${CLIENT_MAX_BYTES / 1024} kB budget.`);
					}
				}

				console.info(bundleSizeReport(graphs));
			},
		},
	],
});

/**
 * Minifies each chunk on its own, because the chunks share symbol names and joining them first gives
 * invalid code. The budget check and the report both measure these.
 */
function minifyChunks(chunks: Array<{ code: string }>): string[] {
	return chunks.map((chunk) => transformSync(chunk.code, { minify: true, target: "esnext" }).code);
}

function minifiedBytes(chunks: Array<{ code: string }>): number {
	return minifyChunks(chunks).reduce((total, code) => total + Buffer.byteLength(code), 0);
}

/**
 * Formats the size of each entry together with the chunks that it imports.
 *
 * tsdown reports each emitted file on its own, unminified. This reports what a consumer really
 * ships, because the consumer's bundler merges an entry with its chunks and then minifies them.
 *
 * The report is informational. Comment out its one call in the plugin below to switch it off, or
 * delete this function, that call and the two imports at the top of this file to remove it.
 */
function bundleSizeReport(graphs: Array<{ entry: { fileName: string }; chunks: Array<{ code: string }> }>): string {
	const kB = (bytes: number) => `${(bytes / 1024).toFixed(1)} kB`.padStart(9);
	const lines = graphs.map(({ entry, chunks }) => {
		// The chunks are compressed together, because the consumer's bundler merges them into the one
		// file that the network then compresses.
		const minified = minifyChunks(chunks);
		const rawBytes = chunks.reduce((total, chunk) => total + Buffer.byteLength(chunk.code), 0);
		const minBytes = minified.reduce((total, code) => total + Buffer.byteLength(code), 0);
		const gzipBytes = gzipSync(minified.join("\n")).byteLength;
		return `  ${entry.fileName.padEnd(16)}${kB(rawBytes)} raw${kB(minBytes)} min${kB(gzipBytes)} min+gzip`;
	});
	return `Bundle sizes (entry and imported chunks):\n${lines.join("\n")}`;
}
