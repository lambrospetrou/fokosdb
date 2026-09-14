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

	it("queryItems returns the items of a projection page", async () => {
		const table = `t-${crypto.randomUUID()}`;
		for (const sk of ["s1", "s2", "s3"]) {
			const put = await rpc(table, "putItem", { hashKey: "qi", sortKey: sk, data: `v-${sk}` });
			expect(put.status).toBe(200);
		}

		const res = await rpc(table, "queryItems", { queries: [{ hashKey: "qi" }], select: "projection" });
		expect(res.status).toBe(200);
		const body = (await res.json()) as { items: Array<Record<string, unknown>>; count: number; scannedCount: number };
		expect(body.items).toHaveLength(3);
		expect(body.count).toBe(3);
		expect(body.scannedCount).toBe(3);
		expect(body.items[0]).toMatchObject({ sortKey: "s1", data: "v-s1", dataEncoding: "utf8" });
	});

	it("queryItems returns projected records and serializes a bytes cell", async () => {
		const table = `t-${crypto.randomUUID()}`;
		for (const sk of ["s1", "s2", "s3"]) {
			const put = await rpc(table, "putItem", { hashKey: "qi", sortKey: sk, data: `v-${sk}` });
			expect(put.status).toBe(200);
		}

		const res = await rpc(table, "queryItems", {
			queries: [{ hashKey: "qi" }],
			projection: [{ expr: { ref: "sortKey" }, as: "id" }, { expr: { ref: "data" } }, { expr: { b64: "AQID" }, as: "bin" }],
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as { items: Array<Record<string, unknown>>; count: number };
		expect(body.count).toBe(3);
		expect(body.items).toHaveLength(3);
		expect(body.items[0]).toEqual({ id: "s1", data: "v-s1", bin: { b64: "AQID" } });
	});

	it("queryItems accepts a filter, in projection and in count mode", async () => {
		const table = `t-${crypto.randomUUID()}`;
		for (const sk of ["s1", "s2", "s3"]) {
			const put = await rpc(table, "putItem", { hashKey: "qi", sortKey: sk, data: `v-${sk}` });
			expect(put.status).toBe(200);
		}
		const filter = { op: "gte", args: [{ ref: "sortKey" }, { val: "s2" }] };

		const res = await rpc(table, "queryItems", { queries: [{ hashKey: "qi" }], filter });
		expect(res.status).toBe(200);
		const body = (await res.json()) as { items: Array<{ sortKey: string }>; count: number; scannedCount: number };
		expect(body.items.map((i) => i.sortKey)).toEqual(["s2", "s3"]);
		expect(body.count).toBe(2);
		expect(body.scannedCount).toBe(3);

		const count = await rpc(table, "queryItems", { queries: [{ hashKey: "qi" }], select: "count", filter });
		expect(count.status).toBe(200);
		const countBody = (await count.json()) as { items: unknown[]; count: number; scannedCount: number };
		expect(countBody.items).toEqual([]);
		expect(countBody.count).toBe(2);
		expect(countBody.scannedCount).toBe(3);
	});

	it("queryItems combines a filter with a projection", async () => {
		const table = `t-${crypto.randomUUID()}`;
		for (const sk of ["s1", "s2", "s3"]) {
			const put = await rpc(table, "putItem", { hashKey: "qi", sortKey: sk, data: `v-${sk}` });
			expect(put.status).toBe(200);
		}

		const res = await rpc(table, "queryItems", {
			queries: [{ hashKey: "qi" }],
			filter: { op: "gte", args: [{ ref: "sortKey" }, { val: "s2" }] },
			projection: [{ expr: { ref: "sortKey" }, as: "id" }],
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as { items: Array<Record<string, unknown>>; count: number; scannedCount: number };
		expect(body.items).toEqual([{ id: "s2" }, { id: "s3" }]);
		expect(body.count).toBe(2);
		expect(body.scannedCount).toBe(3);
	});

	it("rejects a projection on a count query as 400", async () => {
		const res = await rpc(`t-${crypto.randomUUID()}`, "queryItems", {
			queries: [{ hashKey: "qi" }],
			select: "count",
			projection: [{ expr: { ref: "sortKey" } }],
		});
		expect(res.status).toBe(400);
		expect(await res.json()).toMatchObject({ error: "FokosValidationError", code: "query_projection_with_count" });
	});

	it("queryItems returns a count page with no items", async () => {
		const table = `t-${crypto.randomUUID()}`;
		for (const sk of ["s1", "s2", "s3"]) {
			const put = await rpc(table, "putItem", { hashKey: "qi", sortKey: sk, data: `v-${sk}` });
			expect(put.status).toBe(200);
		}

		const res = await rpc(table, "queryItems", { queries: [{ hashKey: "qi" }], select: "count" });
		expect(res.status).toBe(200);
		const body = (await res.json()) as {
			items: unknown[];
			count: number;
			scannedCount: number;
			cursor?: string;
			meta: { rowsReturned: number };
		};
		expect(body.items).toEqual([]);
		expect(body.count).toBe(3);
		expect(body.scannedCount).toBe(3);
		expect(body.cursor).toBeUndefined();
		expect(body.meta.rowsReturned).toBe(3);
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
