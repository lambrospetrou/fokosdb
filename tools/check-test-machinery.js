#!/usr/bin/env node
// Keeps the rules of the Durable Object test machinery. Each entry of CHECKS is one rule: a function
// that gets every test file and returns its errors. To add a rule, add a function to CHECKS.

import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const PACKAGE_DIR = join(import.meta.dirname, "../packages/fokosdb");

// Files that still have a prototype spy. Each entry is work to do: move the spy to a seam on a
// test-only subclass, then remove the entry. The check fails when an entry has no spy.
const PROTOTYPE_SPY_EXCEPTIONS = {
	"test/partition-do/promotion.test.ts": "PartialRangeTopology is private to the runtime, thus no subclass can reach it.",
};

const TIMER_MARKER = "guard: allow-timer";

/** Returns `name:line: text` for each line of the file that matches `pattern`. */
function matches({ name, lines }, pattern) {
	return lines.flatMap((line, i) => (pattern.test(line) ? [`${name}:${i + 1}: ${line.trim()}`] : []));
}

const CHECKS = [
	// A prototype spy reaches every instance in the isolate, and `vi.restoreAllMocks()` of a different
	// test can remove it. Put the seam on `ControlledPartitionDO` (test/controlled-partition-do.ts).
	function noPrototypeSpy(files) {
		const pattern = /vi\.spyOn\([^,]*prototype/;
		const errors = [];
		for (const file of files) {
			const hits = matches(file, pattern);
			if (!(file.name in PROTOTYPE_SPY_EXCEPTIONS)) errors.push(...hits.map((hit) => `vi.spyOn on a prototype: ${hit}`));
			else if (hits.length === 0) errors.push(`${file.name} has no prototype spy. Remove it from PROTOTYPE_SPY_EXCEPTIONS.`);
		}
		for (const name of Object.keys(PROTOTYPE_SPY_EXCEPTIONS)) {
			if (!files.some((file) => file.name === name)) errors.push(`${name} does not exist. Remove it from PROTOTYPE_SPY_EXCEPTIONS.`);
		}
		return errors;
	},

	// A file that uses a migration hold helper stays sequential.
	function noConcurrentHoldUser(files) {
		return files
			.filter(({ lines }) => lines.some((line) => /withMigrationHeld|withMigrationBatchCap/.test(line)))
			.flatMap((file) => matches(file, /describe\.concurrent/))
			.map((hit) => `describe.concurrent in a file that uses a migration hold helper: ${hit}`);
	},

	// A partition test drives the scheduler, and does not wait for the clock. A timer that is not a
	// wait is permitted when its line, or the line above it, has the marker and a reason.
	function noBareTimerInPartitionTests(files) {
		const pattern = /(^|[^\w.])(setTimeout|sleep)\(/;
		return files
			.filter(({ name }) => name.startsWith("test/partition-do/"))
			.flatMap(({ name, lines }) =>
				lines.flatMap((line, i) => {
					const marked = line.includes(TIMER_MARKER) || (i > 0 && lines[i - 1].includes(TIMER_MARKER));
					return pattern.test(line) && !marked
						? [`bare timer (drive the scheduler, or mark a non-wait with ${TIMER_MARKER}): ${name}:${i + 1}: ${line.trim()}`]
						: [];
				}),
			);
	},
];

const files = readdirSync(join(PACKAGE_DIR, "test"), { recursive: true })
	.filter((path) => path.endsWith(".ts"))
	.map((path) => {
		const name = `test/${path}`;
		return { name, lines: readFileSync(join(PACKAGE_DIR, name), "utf8").split("\n") };
	});

const errors = CHECKS.flatMap((check) => check(files).map((error) => `${check.name}: ${error}`));
if (errors.length > 0) {
	for (const error of errors) console.error(`ERROR: ${error}`);
	process.exit(1);
}
console.log(`Test machinery checks passed (${CHECKS.length} checks).`);
