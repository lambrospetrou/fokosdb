import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
	define: {
		// The suites run inside the Workers runtime and cannot read the shell environment. This
		// substitutes the run count of the property-based suites into the test modules at build time;
		// `propertyRuns` in test/property-based/harness.ts reads it and falls back to its default.
		__FOKOS_PROPERTY_RUNS__: JSON.stringify(process.env.FOKOS_PROPERTY_RUNS ?? ""),
	},
	test: {
		// Several suites spy on Date.now, console.error and PartitionStore.prototype. Restoring
		// globally keeps a spy from leaking out of the test that installed it.
		restoreMocks: true,
	},
	plugins: [
		cloudflareTest({
			wrangler: {
				configPath: "./wrangler.jsonc",
			},
			miniflare: {
				bindings: {
					FOKOS_SHOULD_FETCH_COLO_INFO: false,
				},
			},
		}),
	],
});
