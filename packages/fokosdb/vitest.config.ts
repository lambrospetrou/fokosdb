import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
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
