// Capture CPU and heap profiles of a local `wrangler dev` worker through the
// Chrome DevTools Protocol (CDP). No dependencies: Node >= 22 for global WebSocket.
//
//   node tools/cdp-profile.mjs --type cpu --duration 50000 --out /tmp/x.cpuprofile
//   node tools/cdp-profile.mjs --type heap --duration 50000 --out /tmp/x.heapprofile
//   node tools/cdp-profile.mjs --type snapshot --out /tmp/x.heapsnapshot
//
// Options:
//   --port 9229        inspector port (wrangler dev default, or --inspector-port value)
//   --target <substr>  pick the inspector target whose title/url contains this
//   --interval <us>    CPU sampling interval in microseconds (default 250)
//   --heap-interval <bytes>  heap sampling interval (default 16384)
//
// CPU writes a V8 .cpuprofile, heap a sampling .heapprofile, snapshot a full
// .heapsnapshot. Open all three in Chrome DevTools (Performance / Memory) or
// speedscope.app — they are not pprof files.
//
// A capture samples only what runs during --duration. Start the load first
// (for example `k6 run tools/k6_profile.js`), then start the capture.

import { writeFileSync } from "node:fs";

const args = Object.fromEntries(
	process.argv
		.slice(2)
		.map((a, i, arr) => (a.startsWith("--") ? [a.slice(2), arr[i + 1]] : []))
		.filter(Boolean),
);
const TYPE = args.type || "cpu";
const DURATION = Number(args.duration || 30000);
const PORT = Number(args.port || 9229);
const OUT = args.out || `/tmp/${TYPE}-${Date.now()}`;
const TARGET = args.target || null;
const CPU_INTERVAL = Number(args.interval || 250);
const HEAP_INTERVAL = Number(args["heap-interval"] || 16384);

if (!["cpu", "heap", "snapshot"].includes(TYPE)) {
	console.error("--type must be cpu, heap or snapshot");
	process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function listTargets() {
	for (let i = 0; i < 40; i++) {
		try {
			const res = await fetch(`http://127.0.0.1:${PORT}/json/list`);
			const list = await res.json();
			if (Array.isArray(list) && list.length) return list;
		} catch {}
		await sleep(500);
	}
	throw new Error(`No CDP targets on 127.0.0.1:${PORT}. Is \`wrangler dev\` running?`);
}

const targets = (await listTargets()).filter((t) => t.webSocketDebuggerUrl);
if (!targets.length) throw new Error(`Inspector on port ${PORT} lists no debuggable targets`);
const target = TARGET
	? targets.find((t) => `${t.title} ${t.url}`.includes(TARGET))
	: targets.find((t) => `${t.title} ${t.url}`.includes("workerd")) || targets[0];
if (!target) {
	console.error("No target matches --target. Targets:");
	for (const t of targets) console.error(`  ${t.id}  ${t.title}  ${t.url}`);
	process.exit(1);
}
console.log(`target: ${target.title || target.id}  ${target.url}`);

// workerd's inspector rejects handshakes without an Origin header.
const ws = new WebSocket(target.webSocketDebuggerUrl, { headers: { Origin: "http://localhost" } });
await new Promise((res, rej) => {
	ws.onopen = res;
	ws.onerror = () => rej(new Error("WebSocket connect failed"));
});

let seq = 0;
const pending = new Map();
const snapshotChunks = [];
ws.onmessage = (ev) => {
	const msg = JSON.parse(ev.data);
	if (msg.id && pending.has(msg.id)) {
		const { res, rej } = pending.get(msg.id);
		pending.delete(msg.id);
		if (msg.error) rej(new Error(msg.error.message));
		else res(msg.result);
	} else if (msg.method === "HeapProfiler.addHeapSnapshotChunk") {
		snapshotChunks.push(msg.params.chunk);
	}
};
const send = (method, params = {}) =>
	new Promise((res, rej) => {
		const id = ++seq;
		pending.set(id, { res, rej });
		ws.send(JSON.stringify({ id, method, params }));
	});

let out;
if (TYPE === "cpu") {
	await send("Profiler.enable");
	await send("Profiler.setSamplingInterval", { interval: CPU_INTERVAL });
	await send("Profiler.start");
	console.log(`cpu: sampling for ${DURATION} ms`);
	await sleep(DURATION);
	const { profile } = await send("Profiler.stop");
	out = JSON.stringify(profile);
} else if (TYPE === "heap") {
	await send("HeapProfiler.enable");
	await send("HeapProfiler.startSampling", { samplingInterval: HEAP_INTERVAL });
	console.log(`heap: sampling for ${DURATION} ms`);
	await sleep(DURATION);
	const { profile } = await send("HeapProfiler.stopSampling");
	out = JSON.stringify(profile);
} else {
	await send("HeapProfiler.enable");
	await send("HeapProfiler.takeHeapSnapshot", { reportProgress: false });
	out = snapshotChunks.join("");
}

writeFileSync(OUT, out);
console.log(`wrote ${OUT} (${(out.length / 1024).toFixed(0)} kB)`);
ws.close();
process.exit(0);
