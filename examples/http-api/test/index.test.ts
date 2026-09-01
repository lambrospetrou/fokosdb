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
});
