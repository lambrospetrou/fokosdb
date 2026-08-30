import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../do-partition.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { validateReadJsonPath } from "./path.js";

const validPaths = ["$", "$.profile.email", "$.items[0]", "$.items[#-1]", '$."special.label"', '$."items[0]"', '$.""'];

const invalidPaths = [
	"",
	"profile.email",
	"$$.value",
	"$.",
	"$..value",
	"$.items[-1]",
	"$.items[#+1]",
	"$.items[#-0]",
	"$.items[#]",
	"$.items[1",
	"$.items[1x]",
	'$.["value"]',
	'$."unterminated',
	'$."bad\\q"',
	'$."value\\u0000hidden"',
	"$.value trailing",
	"$.value\0hidden",
];

describe("SQLite read JSON paths", () => {
	it.each(validPaths)("accepts %s", (path) => {
		expect(() => validateReadJsonPath(path)).not.toThrow();
	});

	it.each(invalidPaths)("rejects %s", (path) => {
		expect(() => validateReadJsonPath(path)).toThrow();
	});

	it("accepts the dereference limit and rejects one selector above it", () => {
		expect(() => validateReadJsonPath(`$${".a".repeat(EXPRESSION_LIMITS.jsonPathDereferences)}`)).not.toThrow();
		expect(() => validateReadJsonPath(`$${".a".repeat(EXPRESSION_LIMITS.jsonPathDereferences + 1)}`)).toThrow(/dereference limit/);
	});

	it("measures the path limit in UTF-8 bytes", () => {
		const atLimit = `$.${"a".repeat(EXPRESSION_LIMITS.jsonPathBytes - 2)}`;
		const aboveLimit = `${atLimit}a`;
		expect(new TextEncoder().encode(atLimit).byteLength).toBe(EXPRESSION_LIMITS.jsonPathBytes);
		expect(() => validateReadJsonPath(atLimit)).not.toThrow();
		expect(() => validateReadJsonPath(aboveLimit)).toThrow(/path limit/);
		expect(() => validateReadJsonPath(`$.${"日".repeat(1364)}`)).not.toThrow();
		expect(() => validateReadJsonPath(`$.${"日".repeat(1365)}`)).toThrow(/path limit/);
	});

	it("supports quoted special labels in Workers SQLite", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `expression-path.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const document = { "a.b[0]": 1, 'quote"key': 2 };
			for (const [path, value] of [
				['$."a.b[0]"', 1],
				['$."quote\\"key"', 2],
			] as const) {
				validateReadJsonPath(path);
				const row = state.storage.sql.exec<{ value: number }>("SELECT json_extract(?, ?) AS value", JSON.stringify(document), path).one();
				expect(row.value).toBe(value);
			}
		});
	});

	it("keeps hostile path text in a SQL binding", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `expression-path.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const label = "x'); DROP TABLE items; --";
			const path = `$."${label}"`;
			validateReadJsonPath(path);
			const row = state.storage.sql
				.exec<{ value: number }>("SELECT json_extract(?, ?) AS value", JSON.stringify({ [label]: 7 }), path)
				.one();
			expect(row.value).toBe(7);
			expect(state.storage.sql.exec<{ count: number }>("SELECT COUNT(*) AS count FROM items").one().count).toBe(0);
		});
	});
});
