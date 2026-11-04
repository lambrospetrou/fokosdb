/**
 * Profiling load for a deployed fokosdb http-api worker.
 *
 * Sends 70% getItem / 30% putItem to one table. ROOTS=1 (the default) gives the
 * table a single root partition, so every request lands on one Durable Object
 * and a profile of that isolate is dense.
 *
 * Use a fresh TABLE per topology: rootTreesN is fixed at first write.
 *
 * Examples:
 *   k6 run -e BASE_URL=https://<worker>.<sub>.workers.dev -e FOKOS_API_TOKEN=<token> tools/k6_profile.js
 *   k6 run -e BASE_URL=... -e FOKOS_API_TOKEN=... -e TABLE=prof-x -e RPS=300 -e DURATION=300s tools/k6_profile.js
 *
 * While the test runs, capture profiles with the Cloudflare API. The script logs
 * the serving partition actor ID on its first response; that ID plus the
 * namespace ID target a Durable Object capture.
 */
import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8787";
const FOKOS_API_TOKEN = __ENV.FOKOS_API_TOKEN || "dev-token-1";
const TABLE = __ENV.TABLE || "profile-load";
const ROOTS = Number(__ENV.ROOTS || 1);
const RPS = Number(__ENV.RPS || 200);
const DURATION = __ENV.DURATION || "300s";
const DATA_LEN = Number(__ENV.DATA_LEN || 512);
const HKS = Number(__ENV.HKS || 200);
const SKS = Number(__ENV.SKS || 20);

export const options = {
	scenarios: {
		profile_load: {
			executor: "ramping-arrival-rate",
			startRate: 0,
			timeUnit: "1s",
			preAllocatedVUs: 50,
			maxVUs: 500,
			stages: [
				// Warm-up: wake the isolate so the profiler finds it.
				{ target: Math.max(10, RPS / 10), duration: "20s" },
				{ target: RPS, duration: "10s" },
				{ target: RPS, duration: DURATION },
			],
		},
	},
};

const DATA_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
function randData() {
	let s = "";
	for (let i = 0; i < DATA_LEN; i++) s += DATA_CHARS[Math.floor(Math.random() * DATA_CHARS.length)];
	return s;
}

const params = {
	headers: {
		"Content-Type": "application/json",
		"x-fokos-secret-token": FOKOS_API_TOKEN,
	},
};

let loggedActor = false;

function rpc(action, body) {
	const res = http.post(
		`${BASE_URL}/api/rpc/${TABLE}/${action}`,
		JSON.stringify({ ...body, partitionOptions: { rootTreesN: ROOTS } }),
		params,
	);
	check(res, { [`${action} 200`]: (r) => r.status === 200 });
	if (!loggedActor && __VU === 1 && res.status === 200) {
		try {
			const p = JSON.parse(res.body)?.meta?.partitions?.[0];
			if (p?.servedByActorId) {
				loggedActor = true;
				console.log(`serving actor: ${p.servedByActorId} (${p.servedByActorName})`);
			}
		} catch {}
	}
	return res;
}

export default function () {
	const key = { hashKey: `hk-${Math.floor(Math.random() * HKS)}`, sortKey: `sk-${Math.floor(Math.random() * SKS)}` };
	if (Math.random() < 0.7) {
		rpc("getItem", key);
	} else {
		rpc("putItem", { ...key, data: randData() });
	}
}
