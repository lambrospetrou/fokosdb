/**
 * Measures the creation of the clients inside workerd. Run it with `pnpm --filter fokosdb bench:workerd`.
 *
 * Timers in workerd advance in whole milliseconds, so each sample times a batch of at least MIN_BATCH_MS.
 */
import { expect, it } from "vitest";
import { CASES } from "./client-creation-cases.js";

const MIN_BATCH_MS = 200;
const SAMPLES = 7;
// Each result goes into this ring, so the optimizer cannot remove the work.
const sink: unknown[] = Array.from<unknown>({ length: 1024 });

/** The median ns per call of `fn`. */
function measure(fn: () => unknown): number {
	const run = (rounds: number) => {
		for (let r = 0; r < rounds; r++) {
			sink[r & 1023] = fn();
		}
	};
	// Warm up, and find the number of rounds that takes at least MIN_BATCH_MS.
	let rounds = 1_000;
	for (;;) {
		const start = performance.now();
		run(rounds);
		if (performance.now() - start >= MIN_BATCH_MS) {
			break;
		}
		rounds *= 2;
	}
	const samples: number[] = [];
	for (let s = 0; s < SAMPLES; s++) {
		const start = performance.now();
		run(rounds);
		samples.push(((performance.now() - start) * 1e6) / rounds);
	}
	samples.sort((a, b) => a - b);
	return samples[Math.floor(SAMPLES / 2)];
}

it("client creation", () => {
	const rows = CASES.map(([name, fn]) => `| ${name} | ${measure(fn).toFixed(0)} |`);
	expect(sink.some((v) => v !== undefined)).toBe(true);
	console.log(
		[`workerd, ns per call, median of ${SAMPLES} batches of at least ${MIN_BATCH_MS} ms`, "| Case | ns |", "| --- | ---: |", ...rows].join(
			"\n",
		),
	);
});
