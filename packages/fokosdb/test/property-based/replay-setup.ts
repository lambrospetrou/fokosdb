// Every test file loads this module before it runs. It does two things for the property-based suites:
//
//   1. It applies FOKOS_PROPERTY_SEED and FOKOS_PROPERTY_PATH to every `fc.assert`, so a shell command
//      replays a failure and no suite changes.
//   2. It adds the replay commands to the report of a failure.
//
// README.md in this directory says how to replay a failure and when a replay does not reproduce it.
import fc from "fast-check";
import { expect } from "vitest";

// `vitest.config.ts` substitutes these constants into this module when it builds it.
declare const __FOKOS_PROPERTY_SEED__: string;
declare const __FOKOS_PROPERTY_PATH__: string;

if (__FOKOS_PROPERTY_SEED__ !== "") {
	const seed = Number(__FOKOS_PROPERTY_SEED__);
	if (!Number.isSafeInteger(seed)) {
		throw new Error(`FOKOS_PROPERTY_SEED must be an integer, got ${JSON.stringify(__FOKOS_PROPERTY_SEED__)}`);
	}
	// `endOnFailure` stops the shrink, so the replay runs the reported counterexample and nothing smaller.
	const path = __FOKOS_PROPERTY_PATH__ === "" ? {} : { path: __FOKOS_PROPERTY_PATH__, endOnFailure: true };
	fc.configureGlobal({ ...fc.readConfigureGlobal(), seed, ...path });
} else if (__FOKOS_PROPERTY_PATH__ !== "") {
	throw new Error("FOKOS_PROPERTY_PATH needs FOKOS_PROPERTY_SEED, because a path is a position in the runs of one seed");
}

// A string that a POSIX shell reads as one literal word.
const shellQuote = (s: string): string => `'${s.replaceAll("'", `'\\''`)}'`;

// A pattern for `vitest -t` that matches the name literally.
const literalPattern = (s: string): string => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** The commands that run the failed property again, from the package root. */
function replayCommands(details: fc.RunDetails<unknown>, message: string): string {
	const { testPath, currentTestName } = expect.getState();
	// `currentTestName` holds the describe blocks and the test, separated by " > ". `-t` matches a
	// substring of the full name, so the name of the test alone selects it.
	const testName = currentTestName?.split(" > ").at(-1) ?? "";
	const file = testPath === undefined ? "<test file>" : testPath.slice(testPath.lastIndexOf("/test/") + 1);
	const target = `pnpm vitest run ${file} -t ${shellQuote(literalPattern(testName))}`;

	const lines = ["", "Replay this failure from packages/fokosdb. README.md in test/property-based says more.", ""];
	if (details.counterexamplePath !== null) {
		// fc.commands prints the commands that ran as `/*replayPath="..."*/`. The shrunk sequence of
		// commands needs it, because a shrink of commands depends on which commands ran.
		const replayPath = /\/\*replayPath="([^"]*)"\*\//.exec(message)?.[1];
		const replayPathVar = replayPath === undefined ? "" : `FOKOS_PROPERTY_REPLAY_PATH=${shellQuote(replayPath)} `;
		const firstRun = Number(details.counterexamplePath.split(":")[0]);
		lines.push(
			"The counterexample alone:",
			`  FOKOS_PROPERTY_SEED=${details.seed} FOKOS_PROPERTY_PATH=${shellQuote(details.counterexamplePath)} ${replayPathVar}${target}`,
			`Every run of the seed up to the failure (run ${firstRun + 1}), and the shrink again:`,
			`  FOKOS_PROPERTY_SEED=${details.seed} FOKOS_PROPERTY_RUNS=${firstRun + 1} ${target}`,
		);
	} else {
		lines.push("Every run of the seed:", `  FOKOS_PROPERTY_SEED=${details.seed} ${target}`);
	}
	lines.push(
		"",
		"The seed sets the input and not the timing. A failure that needs a race, such as a split in",
		"progress at the same time as a transaction, can pass on a replay. The suites also share one",
		"table, and the counterexample alone skips the runs that changed it. When the first command",
		"passes, use the second one, and run it more than one time.",
	);
	return lines.join("\n");
}

fc.installGlobalPlugin(() => ({
	onAllRunsComplete: (details) => {
		if (!details.failed) {
			return;
		}
		// A throw here replaces the report of `fc.assert`. The error keeps that report and its cause.
		const message = fc.defaultReportMessage(details) ?? "Property failed";
		throw new Error(`${message}\n${replayCommands(details, message)}`, { cause: details.errorInstance });
	},
}));
