/**
 * Examples:
 * k6 run -e BASE_URL=http://localhost:8787 tools/k6_basic.js
 * k6 run -e BASE_URL=http://localhost:8787 -e DB_NAME=mydb tools/k6_basic.js
 *
 */
import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8787";
const DB_NAME = __ENV.DB_NAME || new Date().toDateString().replaceAll(" ", "_").toLowerCase();
const FOKOS_API_TOKEN = __ENV.FOKOS_API_TOKEN || "dev-token-1";

export const options = {
	scenarios: {
		ramp_and_sustain: {
			executor: "ramping-arrival-rate",
			startRate: 0,
			timeUnit: "1s",
			preAllocatedVUs: 300,
			maxVUs: 1000,
			stages: [
				// Initial warmup to wake up the DOs.
				{ target: 10, duration: "5s" },
				// Ramp up to 500 RPS within 5s.
				{ target: 500, duration: "5s" },
				// Sustain 500 RPS for 5 minutes.
				{ target: 500, duration: "300s" },
			],
		},
	},
};

const TXT_1KB = "x".repeat(1024);
const TXT_10KB = "x".repeat(10 * 1024);

export default function () {
	const dbConfig = {
		partitionOptions: {
			rootTreesN: 10,
			hashSplitN: 4,
			rangeSplitN: 4,
			hashSplitConditions: {
				maxSizeMb: 100,
			},
		},
	};

	const hk = `stress#${crypto.randomUUID()}`;
	const sk = crypto.randomUUID();
	const data = `payload-${crypto.randomUUID()}-${TXT_10KB}`;

	const payload = JSON.stringify({
		...dbConfig,
		hashKey: hk,
		sortKey: sk,
		data: data,
	});

	const params = {
		headers: {
			"Content-Type": "application/json",
			"x-fokos-secret-token": FOKOS_API_TOKEN,
		},
	};

	const putRes = http.post(`${BASE_URL}/api/rpc/k6_${DB_NAME}/putItem`, payload, params);
	check(putRes, {
		"putItem status 200": (r) => r.status === 200,
	});

	const getPayload = JSON.stringify({
		...dbConfig,
		hashKey: hk,
		sortKey: sk,
	});

	const getRes = http.post(`${BASE_URL}/api/rpc/k6_${DB_NAME}/getItem`, getPayload, params);
	check(getRes, {
		"getItem status 200": (r) => r.status === 200,
		"getItem data matches": (r) => {
			try {
				return JSON.parse(r.body).item.data === data;
			} catch {
				return false;
			}
		},
	});
}
