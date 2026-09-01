import { defineConfig } from "tsdown";

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
});
