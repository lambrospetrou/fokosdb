import { env, exports } from "cloudflare:workers";
import { beforeEach, describe, expect, it } from "vitest";
import { MAX_BATCH_FORWARDED_SUB_BATCH_BYTES, MAX_BATCH_GET_ITEMS, MAX_BATCH_WRITE_ITEMS } from "../src/lib/transaction-limits.js";

const TOKEN = "test-token";
const PARTITION_OPTIONS = {
	rootTreesN: 1,
	hashSplitN: 2,
	rangeSplitN: 2,
	hashSplitConditions: { maxSizeMb: 10 },
	rangeSplitConditions: { maxSizeMb: 10 },
};

type BatchGetHttpResult = {
	items: Array<{
		inputIndex: number;
		found: boolean;
		item: {
			hashKey: string;
			sortKey?: string;
			data?: string;
			dataEncoding?: "utf8" | "base64";
			version?: number;
		};
	}>;
	unprocessedKeys: unknown[];
	meta: {
		requestedCount: number;
		processedCount: number;
		unprocessedCount: number;
		rowsRead: number;
		rowsWritten: number;
		forwardCount: number;
		partitionsVisited: number;
	};
	partitionMetas: Array<{ rowsRead: number; rowsWritten: number; forwardCount: number; servedByActorName: string }>;
};

type BatchWriteHttpResult = {
	processedItems: Array<{ inputIndex: number; operation: "put" | "delete"; item: { hashKey: string; sortKey?: string } }>;
	unprocessedItems: unknown[];
	meta: {
		requestedCount: number;
		processedCount: number;
		unprocessedCount: number;
		rowsRead: number;
		rowsWritten: number;
		forwardCount: number;
		partitionsVisited: number;
	};
	partitionMetas: Array<{ rowsRead: number; rowsWritten: number; forwardCount: number; servedByActorName: string }>;
};

type GetItemHttpResult =
	| { found: true; item: { hashKey: string; sortKey?: string; data: string; dataEncoding: "utf8" | "base64"; version: number } }
	| { found: false; item: { hashKey: string; sortKey?: string } };

beforeEach(() => {
	(env as unknown as { FOKOS_API_TOKENS: string }).FOKOS_API_TOKENS = TOKEN;
});

function tableName(): string {
	return `httpbatch.${crypto.randomUUID().replaceAll("-", "")}`;
}

async function rpc(table: string, rpcAction: string, body: unknown): Promise<Response> {
	return exports.default.fetch(`https://example.com/api/rpc/${table}/${rpcAction}`, {
		method: "POST",
		headers: { "Content-Type": "application/json", "x-fokos-secret-token": TOKEN },
		body: JSON.stringify(body),
	});
}

async function expectOkJson<T>(response: Response): Promise<T> {
	const text = await response.text();
	expect(response.status, text).toBe(200);
	return JSON.parse(text) as T;
}

async function expectValidationFailure(table: string, rpcAction: string, body: unknown, expected: RegExp): Promise<void> {
	const response = await rpc(table, rpcAction, body);
	const text = await response.text();
	expect(response.status, text).toBe(400);
	expect(text).toMatch(expected);
}

describe("HTTP batch item RPCs", () => {
	it("batchGetItems returns found and missing items in request order with serialized data and meta", async () => {
		const table = tableName();
		await expectOkJson(
			await rpc(table, "putItem", {
				hashKey: "http-get-a",
				sortKey: "sk",
				data: "alpha",
				partitionOptions: PARTITION_OPTIONS,
			}),
		);
		await expectOkJson(
			await rpc(table, "putItem", {
				hashKey: "http-get-b",
				data: "bravo",
				partitionOptions: PARTITION_OPTIONS,
			}),
		);

		const result = await expectOkJson<BatchGetHttpResult>(
			await rpc(table, "batchGetItems", {
				items: [{ hashKey: "http-get-a", sortKey: "sk" }, { hashKey: "http-missing" }, { hashKey: "http-get-b" }],
				partitionOptions: PARTITION_OPTIONS,
			}),
		);

		expect(result.items).toEqual([
			{
				inputIndex: 0,
				found: true,
				item: { hashKey: "http-get-a", sortKey: "sk", data: "alpha", dataEncoding: "utf8", version: 1 },
			},
			{ inputIndex: 1, found: false, item: { hashKey: "http-missing" } },
			{ inputIndex: 2, found: true, item: { hashKey: "http-get-b", data: "bravo", dataEncoding: "utf8", version: 1 } },
		]);
		expect(result.unprocessedKeys).toEqual([]);
		expect(result.meta).toEqual({
			requestedCount: 3,
			processedCount: 3,
			unprocessedCount: 0,
			rowsRead: 2,
			rowsWritten: 0,
			forwardCount: 0,
			partitionsVisited: 1,
		});
		expect(result.partitionMetas).toHaveLength(1);
		expect(result.partitionMetas[0]).toMatchObject({ rowsRead: 2, rowsWritten: 0, forwardCount: 0 });
		expect(result.partitionMetas[0].servedByActorName).toContain(table);
	});

	it("batchWriteItems applies put/delete operations and returns processed shape without versions", async () => {
		const table = tableName();
		await expectOkJson(
			await rpc(table, "putItem", {
				hashKey: "http-existing",
				data: "old",
				partitionOptions: PARTITION_OPTIONS,
			}),
		);

		const result = await expectOkJson<BatchWriteHttpResult>(
			await rpc(table, "batchWriteItems", {
				operations: [
					{ operation: "put", hashKey: "http-created", data: "new" },
					{ operation: "delete", hashKey: "http-existing" },
				],
				partitionOptions: PARTITION_OPTIONS,
			}),
		);

		expect(result.processedItems).toEqual([
			{ inputIndex: 0, operation: "put", item: { hashKey: "http-created" } },
			{ inputIndex: 1, operation: "delete", item: { hashKey: "http-existing" } },
		]);
		expect("version" in result.processedItems[0]).toBe(false);
		expect(result.unprocessedItems).toEqual([]);
		expect(result.meta).toMatchObject({
			requestedCount: 2,
			processedCount: 2,
			unprocessedCount: 0,
			forwardCount: 0,
			partitionsVisited: 1,
		});
		expect(result.meta.rowsWritten).toBeGreaterThan(0);
		expect(result.partitionMetas).toHaveLength(1);
		expect(result.partitionMetas[0].rowsWritten).toBeGreaterThan(0);

		const created = await expectOkJson<GetItemHttpResult>(
			await rpc(table, "getItem", { hashKey: "http-created", partitionOptions: PARTITION_OPTIONS }),
		);
		expect(created).toMatchObject({ found: true, item: { data: "new", dataEncoding: "utf8", version: 1 } });

		const deleted = await expectOkJson<GetItemHttpResult>(
			await rpc(table, "getItem", { hashKey: "http-existing", partitionOptions: PARTITION_OPTIONS }),
		);
		expect(deleted).toMatchObject({ found: false, item: { hashKey: "http-existing" } });
	});

	it("rejects invalid batch request shapes during HTTP validation", async () => {
		const table = tableName();
		await expectValidationFailure(
			table,
			"batchGetItems",
			{ items: [], partitionOptions: PARTITION_OPTIONS },
			/batchGetItems requires at least 1 item/,
		);
		await expectValidationFailure(
			table,
			"batchGetItems",
			{
				items: Array.from({ length: MAX_BATCH_GET_ITEMS + 1 }, (_, index) => ({ hashKey: `too-many-get-${index}` })),
				partitionOptions: PARTITION_OPTIONS,
			},
			/batchGetItems supports at most 100 items/,
		);
		await expectValidationFailure(
			table,
			"batchGetItems",
			{ items: [{ hashKey: "duplicate-get" }, { hashKey: "duplicate-get" }], partitionOptions: PARTITION_OPTIONS },
			/batchGetItems duplicate key/,
		);
		await expectValidationFailure(
			table,
			"batchGetItems",
			{ items: [{ hashKey: "" }], partitionOptions: PARTITION_OPTIONS },
			/hashKey must not be empty/,
		);
		await expectValidationFailure(
			table,
			"batchWriteItems",
			{ operations: [], partitionOptions: PARTITION_OPTIONS },
			/batchWriteItems requires at least 1 item/,
		);
		await expectValidationFailure(
			table,
			"batchWriteItems",
			{
				operations: Array.from({ length: MAX_BATCH_WRITE_ITEMS + 1 }, (_, index) => ({
					operation: "put",
					hashKey: `too-many-write-${index}`,
					data: "x",
				})),
				partitionOptions: PARTITION_OPTIONS,
			},
			/batchWriteItems supports at most 25 items/,
		);
		await expectValidationFailure(
			table,
			"batchWriteItems",
			{
				operations: [
					{ operation: "put", hashKey: "duplicate-write", data: "x" },
					{ operation: "delete", hashKey: "duplicate-write" },
				],
				partitionOptions: PARTITION_OPTIONS,
			},
			/batchWriteItems duplicate key/,
		);
		await expectValidationFailure(
			table,
			"batchWriteItems",
			{
				operations: [
					{
						operation: "put",
						hashKey: "too-large-forwarded-op",
						data: "x".repeat(MAX_BATCH_FORWARDED_SUB_BATCH_BYTES + 1),
					},
				],
				partitionOptions: PARTITION_OPTIONS,
			},
			/batchWriteItems operation payload exceeds 1 MB forwarded sub-batch limit/,
		);
		await expectValidationFailure(
			table,
			"batchWriteItems",
			{
				operations: [{ operation: "put", hashKey: "conditional", data: "x", conditions: [{ type: "item_exists" }] }],
				partitionOptions: PARTITION_OPTIONS,
			},
			/conditions/,
		);
		await expectValidationFailure(
			table,
			"batchWriteItems",
			{ operations: [{ operation: "check", hashKey: "checked" }], partitionOptions: PARTITION_OPTIONS },
			/check|operation/,
		);
		await expectValidationFailure(
			table,
			"batchWriteItems",
			{
				operations: [{ operation: "put", hashKey: "with-token", data: "x" }],
				clientRequestToken: "not-supported-for-batch-write",
				partitionOptions: PARTITION_OPTIONS,
			},
			/clientRequestToken/,
		);
	});
});
