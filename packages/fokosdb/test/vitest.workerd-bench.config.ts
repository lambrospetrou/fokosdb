import { resolve } from "node:path";
import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// Benchmarks that must run inside the Workers runtime. `vitest bench` does not run in the Workers pool, so each
// benchmark is an ordinary test that times its own batches and prints a table. `pnpm test` does not run them.
export default defineConfig({
	// The package directory, so that the paths below do not depend on the directory that starts vitest.
	root: resolve(import.meta.dirname, ".."),
	test: {
		include: ["test/*-bench/*.workerd-bench.ts"],
		testTimeout: 300_000,
	},
	plugins: [
		cloudflareTest({
			wrangler: {
				configPath: "./wrangler.jsonc",
			},
		}),
	],
});
