import { cloudflareTest } from "@cloudflare/vitest-plugin";
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
		// The Durable Object suites do real splits, migrations, alarms and 2PC fan-out. Their duration
		// comes from the schedule and the parallel load, and not from the code under test. One test
		// operated in 1.5 s alone, and in 5.9 s in the full suite. The `vi.waitFor` deadlines in
		// test/partition-do/partition-harness.ts find a split that stopped, and they report the state of
		// the partition. This limit stops only a run that is slow and correct.
		// Thus this limit must be more than the longest deadline (30 s, the migration hold) plus the
		// setup time of a test. If it is not more, the timeout occurs first. The report of the state is
		// then lost, and the `finally` that releases the hold does not operate. A test that needs more
		// time sets its own limit.
		testTimeout: 45_000,
		// The hooks get the same limit. The suites build their shared split trees in `beforeAll`. Thus a
		// hook does the same waits as a test, and it needs the same time. The default limit is 10 s,
		// and a split under load can need more. That limit stops the setup of a full file before the
		// file can report a state.
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
