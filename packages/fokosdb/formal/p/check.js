#!/usr/bin/env node
// Compiles the P model of FokosDB and runs its test cases in Docker. `check.js --help` gives the usage.
//
// A test case named tcBug<Defect> must report a violation of the monitor that EXPECTED_MONITOR gives.
// Every other test case must report no bug. The script exits non-zero on any other result.

import { spawn } from "node:child_process";
import { closeSync, mkdirSync, openSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { parseArgs } from "node:util";

const USAGE = `Compiles the P model of FokosDB and runs its test cases in Docker.

Usage:
  check.js [--pex] [--jobs <n>] [--schedules <n>] [test case...] [-- <p check option>...]
  check.js --both-strategies [--jobs <n>] [--schedules <n>] [test case...]
  check.js --replay <test case> <schedule file>
  check.js --help

Without a test case, the script runs every test case in PTst/. A test case named tcBug<Defect> must
report a violation of the monitor of its seeded defect. Every other test case must report no bug.

Options:
  --pex           Use the exhaustive checker PEx, not the random checker. A test case passes only when
                  PEx explores every state, and a tcBug<Defect> test case only when PEx finds the
                  violation.
  -j, --jobs <n>  Run <n> test cases at the same time. The default is JOBS, else 2. Each run is slower
                  when more runs share the CPU.
  --schedules <n> The number of schedules of the random checker. The default is SCHEDULES, else
                  1000. A test case in TEST_SCHEDULES runs at least its own number. The random
                  checker stops at the first bug.
  --both-strategies
                  Run each test case that must report no bug twice: with the random strategy and with
                  fair PCT (--sch-fairpct 10). Each tcBug<Defect> test case runs once, with its own
                  strategy, because the random strategy does not find some seeded defects in 10000
                  schedules.
  --replay        Replay a schedule of the random checker, for example
                  PCheckerOutput/<test case>/BugFinding/FokosDB_0_0.schedule.
  -h, --help      Show this help.

The script gives each option after "--" to every \`p check\` call without a change, for example
\`-- --sch-pct 3 --seed 42\`. The script refuses an option that it sets itself. Without them, a test
case in TEST_STRATEGY uses its own strategy.

Environment:
  SCHEDULES       The default of --schedules.
  PEX_TIMEOUT     The time limit of PEx for each test case, in seconds (default 60).
  JOBS            The default of --jobs.

Output:
  One line for each run of a test case, in the order of the test cases: the name, the schedules and
  timelines that the checker explored (or the time limit of PEx), the time in seconds, and the
  result. The output of a run goes to PCheckerOutput/<test case>/, and to
  PCheckerOutput/pex/<test case>/ for PEx. With --both-strategies, the second run of a test case
  goes to PCheckerOutput/random/<test case>/ or PCheckerOutput/fairpct/<test case>/. The random
  checker writes the schedule of a bug to BugFinding/FokosDB_0_0.schedule there. The Maven
  repository of PEx stays in .p-cache/ between runs.

Exit status: 0 when every test case gives the expected result, else 1.`;

const P_VERSION = "3.1.0";
const IMAGE = `fokosdb-p:${P_VERSION}`;

const EXPECTED_MONITOR = {
	tcBugNewRowVersionFromOne: "VersionIncreases",
	tcBugCommitOnOneAccept: "Atomicity",
	tcBugPutIgnoresLock: "LockExclusion",
	tcBugCommittedBeforeApply: "ReadAfterCommit",
	tcBugTokenRowIgnored: "SingleApply",
	tcBugStaleCancelsOnDriving: "Atomicity",
	tcBugCancelInAnyState: "Atomicity",
	tcBugNoPreparingHold: "LocksResolve",
	tcBugPrepareSkipsWatermark: "WriteSerializable",
	tcBugSingleShotIgnoresLock: "LockExclusion",
	tcBugNewRowTsFromOwnStamp: "WriteSerializable",
	tcBugAbsentCheckKeepsNoStamp: "WriteSerializable",
	tcBugQuarantineStampedChecks: "CommittedNeverQuarantined",
	tcBugReadSkipsPhaseTwo: "ReadSerializable",
	tcBugReadIgnoresPendingWrite: "ReadSerializable",
	tcBugReadIgnoresMaxDeletedV: "ReadSerializable",
	tcBugReusedVersionRead: "ReadSerializable",
};

// The two search strategies of the random checker. RANDOM is the default of `p check`.
const RANDOM = ["--sch-random"];
const FAIR_PCT = ["--sch-fairpct", "10"];

// The search strategy for a test case whose orders need many unlikely steps in a row. Options after
// "--" replace it.
const TEST_STRATEGY = {
	tcStale: FAIR_PCT,
	tcRepair: FAIR_PCT,
	tcBugPutIgnoresLock: FAIR_PCT,
	tcBugCancelInAnyState: FAIR_PCT,
	tcBugSingleShotIgnoresLock: FAIR_PCT,
	tcBugNewRowTsFromOwnStamp: FAIR_PCT,
	tcAbsentCheck: FAIR_PCT,
	tcLateCheck: FAIR_PCT,
	tcBugQuarantineStampedChecks: FAIR_PCT,
	tcBugAbsentCheckKeepsNoStamp: FAIR_PCT,
	tcReadHappy: FAIR_PCT,
	tcReadVsWrites: FAIR_PCT,
	tcReadFaults: FAIR_PCT,
	tcSnapshot: FAIR_PCT,
	tcBugReadSkipsPhaseTwo: FAIR_PCT,
	tcBugReadIgnoresPendingWrite: FAIR_PCT,
	tcBugReadIgnoresMaxDeletedV: FAIR_PCT,
	tcBugReusedVersionRead: FAIR_PCT,
};

// The least number of schedules of the random checker for a test case whose defect needs more
// schedules than the default. The checker stops at the first bug, so a found defect costs no more.
const TEST_SCHEDULES = {
	tcBugReadIgnoresMaxDeletedV: 5000,
};

// The `p check` options that the script sets itself.
const OWN_OPTIONS = ["-tc", "--testcase", "-o", "--outdir", "-md", "--mode", "-r", "--replay", "-s", "--schedules", "-t", "--timeout"];

function fail(message) {
	console.error(message);
	process.exit(1);
}

function positiveInt(name, value) {
	if (!/^[1-9][0-9]*$/.test(value)) {
		fail(`${name} must be a positive integer, not "${value}".`);
	}
	return value;
}

/** Runs a command in this directory and resolves to its exit code and, for piped streams, its output. */
function run(command, args, stdio) {
	const child = spawn(command, args, { stdio });
	let output = "";
	child.stdout?.on("data", (data) => (output += data));
	child.stderr?.on("data", (data) => (output += data));
	return new Promise((resolve, reject) => {
		child.on("error", reject);
		child.on("close", (code) => resolve({ code: code ?? 1, output }));
	});
}

async function p(args, stdio) {
	return await run(
		"docker",
		[
			"run",
			"--rm",
			"--user",
			`${process.getuid()}:${process.getgid()}`,
			"-e",
			"HOME=/tmp",
			"-e",
			"MAVEN_OPTS=-Dmaven.repo.local=/workspace/.p-cache/m2",
			"-v",
			`${process.cwd()}:/workspace`,
			IMAGE,
			"p",
			...args,
		],
		stdio,
	);
}

/** The first line of `text` that matches `pattern`, or "". */
function firstLine(text, pattern) {
	return text.split("\n").find((line) => pattern.test(line)) ?? "";
}

// The monitor that an error line names. A liveness error names the monitor. An assertion gives its
// source location, and the monitor is the spec block that holds that line.
function violatedMonitor(line) {
	const named = line.match(/([A-Za-z0-9_]+) detected liveness bug/) ?? line.match(/Monitor .([A-Za-z0-9_]+)/);
	if (named) {
		return named[1];
	}
	const location = line.match(/(PSpec\/[A-Za-z0-9_]+\.p):([0-9]+)/);
	if (!location) {
		return "(no monitor)";
	}
	const lines = readFileSync(location[1], "utf8").split("\n").slice(0, Number(location[2]));
	return lines.map((source) => source.trim().split(/\s+/)).findLast((fields) => fields[0] === "spec")?.[1] ?? "";
}

/**
 * Runs one test case and resolves to its output line and whether it gave the expected result.
 * `options` go to `p check`, and the output line shows `shown`.
 */
async function runTestCase({ tc, outdir, options, shown }, { mode, pexTimeout, ...settings }) {
	const log = `${outdir}/check.log`;
	rmSync(outdir, { recursive: true, force: true });
	mkdirSync(outdir, { recursive: true });
	const pex = mode === "pex";
	const schedules = String(Math.max(Number(settings.schedules), TEST_SCHEDULES[tc] ?? 0));
	const args = pex
		? ["check", "--mode", "pex", "-tc", tc, "-t", pexTimeout, "-o", outdir, ...options]
		: ["check", "-tc", tc, "-s", schedules, "-o", outdir, ...options];
	const start = Date.now();
	const fd = openSync(log, "w");
	try {
		await p(args, ["ignore", fd, fd]);
	} finally {
		closeSync(fd);
	}
	const secs = Math.floor((Date.now() - start) / 1000);
	const text = readFileSync(log, "utf8");
	let bound, cleanPattern, bugPattern, errorLine;
	if (pex) {
		bound = `time limit ${pexTimeout}s`;
		cleanPattern = /Result: correct for any depth/;
		bugPattern = /Result: found cex/;
		errorLine = () => firstLine(text, /Property violated/);
	} else {
		const explored = (word) => text.match(new RegExp(`Explored ([0-9]+) ${word}`))?.[1] ?? "0";
		const strategy = shown.length > 0 ? `, ${shown.join(" ")}` : "";
		bound = `${explored("schedule")} of ${schedules} schedules, ${explored("timeline")} timelines${strategy}`;
		cleanPattern = /Found 0 bugs\./;
		bugPattern = /Checker found a bug\./;
		errorLine = () => {
			const files = readdirSync(`${outdir}/BugFinding`).filter((name) => name.endsWith("_0_0.txt"));
			return firstLine(files.map((name) => readFileSync(`${outdir}/BugFinding/${name}`, "utf8")).join("\n"), /^<ErrorLog>/);
		};
	}
	const ran = text.match(/^\.\. Test case :: /gm)?.length ?? 0;
	const expected = EXPECTED_MONITOR[tc];
	const error = text.match(/^Error: ?(.*)$/m);
	let result;
	if (error) {
		result = `ERROR: ${error[1]}, see ${log}`;
	} else if (ran !== 1) {
		result = `ERROR: the name ran ${ran} test cases, see ${log}`;
	} else if (cleanPattern.test(text)) {
		result = expected ? `FAIL: expected a violation of ${expected}` : "ok";
	} else if (bugPattern.test(text)) {
		const got = violatedMonitor(errorLine());
		if (!tc.startsWith("tcBug")) {
			result = `FAIL: violation of ${got}, see ${outdir}`;
		} else if (got === expected) {
			result = `ok (violation of ${got})`;
		} else {
			result = `FAIL: expected a violation of ${expected ?? "a monitor in EXPECTED_MONITOR"}, got ${got}, see ${outdir}`;
		}
	} else if (pex && /Result:/.test(text)) {
		result = `INCOMPLETE:${text.match(/Result:([^[\n]*)/)[1]}`;
	} else {
		result = `ERROR: see ${log}`;
	}
	return {
		line: `${tc.padEnd(28)} ${bound.padEnd(54)} ${String(secs).padStart(5)}s  ${result}`,
		ok: result.startsWith("ok"),
	};
}

async function main() {
	let argv = process.argv.slice(2);
	let checkOptions = [];
	const separator = argv.indexOf("--");
	if (separator >= 0) {
		checkOptions = argv.slice(separator + 1);
		argv = argv.slice(0, separator);
	}
	let parsed;
	try {
		parsed = parseArgs({
			args: argv,
			allowPositionals: true,
			options: {
				help: { type: "boolean", short: "h" },
				pex: { type: "boolean" },
				jobs: { type: "string", short: "j" },
				schedules: { type: "string" },
				"both-strategies": { type: "boolean" },
				replay: { type: "boolean" },
			},
		});
	} catch (error) {
		// The advice of parseArgs about "--" does not apply: here "--" starts the `p check` options.
		fail(`${error.message.replace(/\. To specify a positional argument.*/s, ".")}\nRun check.js --help for the usage.`);
	}
	const { values: flags, positionals } = parsed;
	if (flags.help) {
		console.log(USAGE);
		process.exit(0);
	}
	const bothStrategies = flags["both-strategies"] === true;
	if (
		flags.replay &&
		(flags.pex || bothStrategies || flags.jobs !== undefined || flags.schedules !== undefined || positionals.length !== 2 || separator >= 0)
	) {
		fail("Usage: check.js --replay <test case> <schedule file>");
	}
	if (bothStrategies && (flags.pex || separator >= 0)) {
		fail("--both-strategies sets the strategy of each run, thus it takes no --pex and no options after --.");
	}
	for (const option of checkOptions) {
		if (OWN_OPTIONS.includes(option)) {
			fail(`The script sets ${option}. Use the test case arguments, --schedules, or PEX_TIMEOUT.`);
		}
	}

	const settings = {
		mode: flags.pex ? "pex" : "bugfinding",
		schedules: positiveInt("--schedules", flags.schedules ?? process.env.SCHEDULES ?? "1000"),
		pexTimeout: positiveInt("PEX_TIMEOUT", process.env.PEX_TIMEOUT ?? "60"),
	};
	const jobs = Number(positiveInt("--jobs", flags.jobs ?? process.env.JOBS ?? "2"));

	process.chdir(import.meta.dirname);

	if ((await run("docker", ["image", "inspect", IMAGE], "ignore")).code !== 0) {
		const build = await run("docker", ["build", "--build-arg", `P_VERSION=${P_VERSION}`, "-t", IMAGE, "."], "inherit");
		if (build.code !== 0) {
			process.exit(build.code);
		}
	}

	const compile = await p(["compile", "--mode", settings.mode], ["ignore", "pipe", "pipe"]);
	if (compile.code !== 0) {
		fail(compile.output.trimEnd());
	}

	if (flags.replay) {
		const [tc, schedule] = positionals;
		process.exit((await p(["check", "-tc", tc, "--replay", schedule, "-o", `PCheckerOutput/${tc}/replay`], "inherit")).code);
	}

	let tests = [...new Set(positionals)];
	if (tests.length === 0) {
		tests = readdirSync("PTst")
			.filter((name) => name.endsWith(".p"))
			.sort()
			.flatMap((name) => [...readFileSync(`PTst/${name}`, "utf8").matchAll(/^test ([A-Za-z0-9_]+)/gm)].map((match) => match[1]));
	}
	if (tests.length === 0) {
		fail("No test case to run.");
	}
	if (checkOptions.length > 0) {
		console.log(`p check options: ${checkOptions.join(" ")}`);
	}

	const runs = tests.flatMap((tc) => {
		if (settings.mode === "pex") {
			return [{ tc, outdir: `PCheckerOutput/pex/${tc}`, options: checkOptions, shown: [] }];
		}
		const own = checkOptions.length > 0 ? checkOptions : (TEST_STRATEGY[tc] ?? (bothStrategies ? RANDOM : []));
		const first = { tc, outdir: `PCheckerOutput/${tc}`, options: own, shown: checkOptions.length > 0 ? [] : own };
		if (!bothStrategies || tc.startsWith("tcBug")) {
			return [first];
		}
		const [other, name] = own === FAIR_PCT ? [RANDOM, "random"] : [FAIR_PCT, "fairpct"];
		return [first, { tc, outdir: `PCheckerOutput/${name}/${tc}`, options: other, shown: other }];
	});

	// Each worker takes the next run. A line prints when every run before it has its line, thus the
	// output keeps the order of the runs.
	const results = [];
	let next = 0;
	let printed = 0;
	async function worker() {
		while (next < runs.length) {
			const i = next++;
			results[i] = await runTestCase(runs[i], settings);
			for (; printed < runs.length && results[printed]; printed++) {
				console.log(results[printed].line);
			}
		}
	}
	await Promise.all(Array.from({ length: Math.min(jobs, runs.length) }, worker));

	const failures = results.filter((r) => !r.ok).length;
	if (failures > 0) {
		console.log(`${failures} run(s) did not give the expected result.`);
		process.exit(1);
	}
}

await main();
