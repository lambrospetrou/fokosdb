const origin = process.argv[2];
if (!origin) {
	console.error("Usage: node check.mjs <deployed-worker-url>");
	process.exit(2);
}

try {
	const url = new URL("/run", origin);
	const response = await fetch(url, { method: "POST", signal: AbortSignal.timeout(90_000) });
	const result = await response.json();
	console.log(JSON.stringify(result, null, 2));

	if (response.status === 200 && result.status === "read_conflict") {
		console.log("PASS: the read rejected the impossible pair");
	} else if (response.status === 409 && result.status === "counterexample") {
		console.error("FAIL: the read returned items that never coexisted");
		process.exitCode = 1;
	} else {
		console.error(`ERROR: the test could not check the result (HTTP ${response.status})`);
		process.exitCode = 2;
	}
} catch (error) {
	console.error("ERROR: the test could not check the result", error);
	process.exitCode = 2;
}
