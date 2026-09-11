import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";

// These requests go through the worker's default export, so they exercise the built `dist/` output
// of `fokosdb/client` and `fokosdb/server` under the same workerd resolution wrangler uses in
// production — including the `workerd` export condition that picks xxhash-wasm's safe loader.

const TOKEN = "test-token";
const headers = { "x-fokos-secret-token": TOKEN, "content-type": "application/json" };

async function rpc(table: string, action: string, body: unknown): Promise<Response> {
	return await SELF.fetch(`https://example.com/api/rpc/${table}/${action}`, {
		method: "POST",
		headers,
		body: JSON.stringify(body),
	});
}

describe("http-api example worker", () => {
	it("rejects a request with no token", async () => {
		const res = await SELF.fetch("https://example.com/api/hello/world");
		expect(res.status).toBe(401);
	});

	it("serves an authenticated route", async () => {
		const res = await SELF.fetch("https://example.com/api/hello/world", { headers });
		expect(res.status).toBe(200);
		expect(await res.json()).toEqual({ message: "Hello, world!" });
	});

	it("round-trips an item through the partition Durable Objects", async () => {
		const table = `t-${crypto.randomUUID()}`;

		const put = await rpc(table, "putItem", { hashKey: "user#1", sortKey: "profile", data: "hello fokos" });
		expect(put.status).toBe(200);

		const get = await rpc(table, "getItem", { hashKey: "user#1", sortKey: "profile" });
		expect(get.status).toBe(200);
		expect(await get.json()).toMatchObject({
			found: true,
			item: { data: "hello fokos", dataEncoding: "utf8" },
		});
	});

	it("reports a validation failure as 400", async () => {
		const res = await rpc(`t-${crypto.randomUUID()}`, "putItem", { hashKey: 42 });
		expect(res.status).toBe(400);
	});

	it("answers a FokosError with its status hint, its category, its code and its error_id", async () => {
		const res = await rpc(`t-${crypto.randomUUID()}`, "putItem", { hashKey: "", data: "v" });
		expect(res.status).toBe(400);
		expect(await res.json()).toMatchObject({
			error: "FokosValidationError",
			code: "hash_key_empty",
			error_id: expect.stringMatching(/^e_2fzzq9_[0-9a-f]{32}$/),
		});
	});

	it("reports a failed condition as 409, not as a server error", async () => {
		const condition = { op: "exists", args: [{ ref: "hashKey" }] };
		const res = await rpc(`t-${crypto.randomUUID()}`, "putItem", { hashKey: "absent", data: "v", condition });
		expect(res.status).toBe(409);
		expect(await res.json()).toMatchObject({ error: "FokosConditionCheckError", code: "condition_failed" });
	});

	it("reports a cancelled transaction as 409, with the result of each operation", async () => {
		const table = `t-${crypto.randomUUID()}`;
		const res = await rpc(table, "transactWriteItems", {
			items: [
				{ operation: "put", hashKey: "tx-1", data: "v" },
				{ operation: "check", hashKey: "tx-absent", condition: { op: "exists", args: [{ ref: "hashKey" }] } },
			],
		});
		expect(res.status).toBe(409);
		expect(await res.json()).toMatchObject({
			error: "FokosTransactionCancelledError",
			code: "transaction_cancelled",
			error_id: expect.stringMatching(/^e_zd7rzd_/),
			results: [{ outcome: "passed" }, { outcome: "rejected", reason: { code: "condition_failed", hashKey: "tx-absent" } }],
		});
		// A cancelled transaction applied nothing.
		expect(await (await rpc(table, "getItem", { hashKey: "tx-1" })).json()).toMatchObject({ found: false });
	});
});
