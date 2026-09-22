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
		// The Durable Object suites drive real splits, migrations, alarms and 2PC fan-out. Their
		// duration follows the scheduling and the parallel load, not the code under test: one case has
		// run 1.5 s on its own and 5.9 s inside the full suite. A wedged split is caught by the
		// `vi.waitFor` deadlines in test/partition-do/partition-harness.ts, which report the state the
		// partition stopped in; this bound only stops a run that is slow and healthy. It must therefore
		// stay above the longest of those deadlines (30 s, the migration hold) plus the setup a test
		// runs before it, or the bare timeout arrives first and takes the state report with it — and the
		// `finally` that releases the hold never runs. A test that needs longer still sets its own.
		testTimeout: 45_000,
		// The same bound for the hooks. The suites build their shared split trees in `beforeAll`, so a
		// hook runs the same waits a test does and needs the same room: the default 10 s sits under the
		// 15 s harness deadlines, which kills the setup of a whole file before it can report a state.
		hookTimeout: 45_000,
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
