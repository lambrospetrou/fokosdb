import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { beforeAll, describe, expect, it } from "vitest";
import { PartitionDO } from "../do-partition.js";
import type { JsonValue } from "../json-types.js";
import { KeyCodec } from "../partition-topology/key-codec.js";
import { PartitionStore } from "../partition/partition-store.js";
import type { DataKind } from "../types.js";
import { compileConditionExpression } from "./compiler.js";
import { evaluateConditionPlan } from "./runtime.js";
import type { ConditionExpression } from "./types.js";

/**
 * The showcase suite for condition expressions. Every case here is a happy-path example of how a
 * user writes a condition, evaluated against real items inside a real PartitionDO. The suite has no
 * error cases on purpose: it is the single place to read to learn what the feature can express.
 */

type ShowcaseItem = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
	data: string | Uint8Array | JsonValue;
	kind: DataKind;
	ttl?: number;
};

/** Base64 for a byte literal, the wire form of a `b64` value node. */
const bytes = (...values: number[]) => new Uint8Array(values).toBase64();

// A shipped order under a text hash key and a text sort key, with a TTL and deeply nested data.
const shippedOrder: ShowcaseItem = {
	hashKey: "customer#acme",
	sortKey: "order#2024-05-01",
	kind: "json",
	ttl: 2_000_000_000,
	data: {
		status: "shipped",
		quantity: 3,
		total: 149.5,
		currency: "EUR",
		balanceDelta: -12.5,
		discount: null,
		tags: ["priority", "gift"],
		contact: { email: "  Ops@ACME.example  ", country: "de" },
		shipping: { address: { city: "Berlin", zip: "10115" }, weightKg: 2.5 },
		history: [
			{ code: "created", at: 1 },
			{ code: "shipped", at: 2 },
		],
	},
};

// A second order under the same hash key, without a TTL. It shows how the same condition selects
// one order and rejects the other.
const pendingOrder: ShowcaseItem = {
	hashKey: "customer#acme",
	sortKey: "order#2024-06-15",
	kind: "json",
	data: {
		status: "pending",
		quantity: 1,
		total: 20,
		currency: "USD",
		tags: ["backorder"],
		contact: { email: "sales@globex.example", country: "us" },
		shipping: { address: { city: "Austin", zip: "73301" }, weightKg: 0.4 },
		history: [{ code: "created", at: 5 }],
	},
};

// Plain text data under a text hash key, with no sort key.
const note: ShowcaseItem = { hashKey: "customer#globex", kind: "text", data: "free-form note" };

// Binary hash key, binary sort key, and binary data — the same operators work on all three.
const binaryItem: ShowcaseItem = {
	hashKey: new Uint8Array([0x01, 0x0a]),
	sortKey: new Uint8Array([0x61, 0x62]),
	kind: "bytes",
	data: new Uint8Array([0xde, 0xad, 0xbe, 0xef]),
};

const partition = PartitionDO.getByName(env.PARTITION_DO, `expression-showcase.${crypto.randomUUID()}`);

beforeAll(async () => {
	await runInDurableObject(partition, (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		for (const item of [shippedOrder, pendingOrder, note, binaryItem]) {
			store.upsertItem({
				hk: KeyCodec.encode(item.hashKey),
				sk: KeyCodec.encodeOptional(item.sortKey),
				data: item.kind === "json" ? JSON.stringify(item.data) : (item.data as string | Uint8Array),
				kind: item.kind,
				ttlEpochUtcSeconds: item.ttl ?? null,
				lastTransactionTs: 1,
			});
		}
	});
});

/** Compiles the condition and evaluates it against one seeded item, exactly as a write path does. */
async function evaluate(item: ShowcaseItem, condition: ConditionExpression): Promise<boolean> {
	return await runInDurableObject(partition, (_instance: PartitionDO, state: DurableObjectState) => {
		const result = evaluateConditionPlan(
			state.storage,
			compileConditionExpression(condition),
			KeyCodec.encode(item.hashKey),
			KeyCodec.encodeOptional(item.sortKey),
		);
		return result.conditionOk;
	});
}

type ShowcaseCase = {
	name: string;
	item: ShowcaseItem;
	condition: ConditionExpression;
	/** Defaults to true; only the contrast cases set it to false. */
	expected?: boolean;
};

function showcase(cases: readonly ShowcaseCase[]): void {
	it.each(cases)("$name", async ({ item, condition, expected = true }) => {
		expect(await evaluate(item, condition)).toBe(expected);
	});
}

describe("expression showcase: comparison operators", () => {
	showcase([
		{ name: "eq on the hash key", item: shippedOrder, condition: { op: "eq", args: [{ ref: "hashKey" }, { val: "customer#acme" }] } },
		{ name: "eq on the sort key", item: shippedOrder, condition: { op: "eq", args: [{ ref: "sortKey" }, { val: "order#2024-05-01" }] } },
		{
			name: "eq on a nested field",
			item: shippedOrder,
			condition: { op: "eq", args: [{ ref: "data", path: "$.shipping.address.city" }, { val: "Berlin" }] },
		},
		{
			name: "ne on a nested field",
			item: shippedOrder,
			condition: { op: "ne", args: [{ ref: "data", path: "$.status" }, { val: "cancelled" }] },
		},
		{
			name: "lt on a nested number",
			item: shippedOrder,
			condition: { op: "lt", args: [{ ref: "data", path: "$.shipping.weightKg" }, { val: 5 }] },
		},
		{
			name: "lte on a nested number",
			item: shippedOrder,
			condition: { op: "lte", args: [{ ref: "data", path: "$.quantity" }, { val: 3 }] },
		},
		{ name: "gt on a nested number", item: shippedOrder, condition: { op: "gt", args: [{ ref: "data", path: "$.total" }, { val: 100 }] } },
		{ name: "gte on the item version", item: shippedOrder, condition: { op: "gte", args: [{ ref: "v" }, { val: 1 }] } },
		{
			name: "between on a nested number",
			item: shippedOrder,
			condition: { op: "between", args: [{ ref: "data", path: "$.total" }, { val: 100 }, { val: 200 }] },
		},
		{
			name: "in on a nested field",
			item: shippedOrder,
			condition: { op: "in", args: [{ ref: "data", path: "$.currency" }, { val: "EUR" }, { val: "USD" }] },
		},
		{
			name: "in on the sort key",
			item: pendingOrder,
			condition: { op: "in", args: [{ ref: "sortKey" }, { val: "order#2024-05-01" }, { val: "order#2024-06-15" }] },
		},
		{
			name: "eq rejects the other order",
			item: pendingOrder,
			condition: { op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "shipped" }] },
			expected: false,
		},
	]);
});

describe("expression showcase: existence and logical operators", () => {
	showcase([
		{ name: "exists on the TTL", item: shippedOrder, condition: { op: "exists", args: [{ ref: "ttl" }] } },
		{ name: "not_exists on an absent TTL", item: pendingOrder, condition: { op: "not_exists", args: [{ ref: "ttl" }] } },
		{ name: "not_exists on an absent sort key", item: note, condition: { op: "not_exists", args: [{ ref: "sortKey" }] } },
		{
			name: "exists on a nested field",
			item: shippedOrder,
			condition: { op: "exists", args: [{ ref: "data", path: "$.shipping.address.city" }] },
		},
		{
			name: "not_exists on an absent nested field",
			item: shippedOrder,
			condition: { op: "not_exists", args: [{ ref: "data", path: "$.coupon" }] },
		},
		{
			name: "and over two nested fields",
			item: shippedOrder,
			condition: {
				op: "and",
				args: [
					{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "shipped" }] },
					{ op: "gt", args: [{ ref: "data", path: "$.total" }, { val: 100 }] },
				],
			},
		},
		{
			name: "or over two states",
			item: pendingOrder,
			condition: {
				op: "or",
				args: [
					{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "pending" }] },
					{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "shipped" }] },
				],
			},
		},
		{
			name: "not around a comparison",
			item: shippedOrder,
			condition: { op: "not", args: [{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "cancelled" }] }] },
		},
	]);
});

describe("expression showcase: prefix, containment, size, and type", () => {
	showcase([
		{
			name: "begins_with on the sort key",
			item: shippedOrder,
			condition: { op: "begins_with", args: [{ ref: "sortKey" }, { val: "order#2024-05" }] },
		},
		{
			name: "begins_with on a nested field",
			item: shippedOrder,
			condition: { op: "begins_with", args: [{ ref: "data", path: "$.status" }, { val: "ship" }] },
		},
		{ name: "begins_with on text data", item: note, condition: { op: "begins_with", args: [{ ref: "data" }, { val: "free-form" }] } },
		{
			name: "contains a substring of a nested field",
			item: shippedOrder,
			condition: { op: "contains", args: [{ ref: "data", path: "$.contact.email" }, { val: "ACME" }] },
		},
		{
			name: "contains an element of a nested array",
			item: shippedOrder,
			condition: { op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: "priority" }] },
		},
		{ name: "contains a substring of text data", item: note, condition: { op: "contains", args: [{ ref: "data" }, { val: "note" }] } },
		{
			name: "size counts array elements",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "size", args: [{ ref: "data", path: "$.tags" }] }, { val: 2 }] },
		},
		{
			name: "size counts object members",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "size", args: [{ ref: "data", path: "$.shipping.address" }] }, { val: 2 }] },
		},
		{
			name: "size counts text bytes",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "size", args: [{ ref: "data", path: "$.status" }] }, { val: 7 }] },
		},
		{
			name: "size counts text data bytes",
			item: note,
			condition: { op: "eq", args: [{ fn: "size", args: [{ ref: "data" }] }, { val: 14 }] },
		},
		{
			name: "attribute_type of a number",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "data", path: "$.total" }] }, { val: "number" }] },
		},
		{
			name: "attribute_type of an array",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "data", path: "$.tags" }] }, { val: "array" }] },
		},
		{
			name: "attribute_type of JSON null",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "data", path: "$.discount" }] }, { val: "null" }] },
		},
		{
			name: "attribute_type of an absent field",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "data", path: "$.coupon" }] }, { val: "missing" }] },
		},
		{
			name: "attribute_type of text data",
			item: note,
			condition: { op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "data" }] }, { val: "text" }] },
		},
	]);
});

describe("expression showcase: text keys and byte keys", () => {
	showcase([
		{
			name: "a text hash key reports the text type",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "hashKey" }] }, { val: "text" }] },
		},
		{
			name: "a byte hash key reports the bytes type",
			item: binaryItem,
			condition: { op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "hashKey" }] }, { val: "bytes" }] },
		},
		{ name: "eq on a byte hash key", item: binaryItem, condition: { op: "eq", args: [{ ref: "hashKey" }, { b64: bytes(0x01, 0x0a) }] } },
		{ name: "eq on a byte sort key", item: binaryItem, condition: { op: "eq", args: [{ ref: "sortKey" }, { b64: bytes(0x61, 0x62) }] } },
		{
			name: "begins_with on a byte sort key",
			item: binaryItem,
			condition: { op: "begins_with", args: [{ ref: "sortKey" }, { b64: bytes(0x61) }] },
		},
		{
			name: "contains on a byte sort key",
			item: binaryItem,
			condition: { op: "contains", args: [{ ref: "sortKey" }, { b64: bytes(0x62) }] },
		},
		{
			name: "between on a byte sort key",
			item: binaryItem,
			condition: { op: "between", args: [{ ref: "sortKey" }, { b64: bytes(0x61) }, { b64: bytes(0x7a) }] },
		},
		{ name: "eq on byte data", item: binaryItem, condition: { op: "eq", args: [{ ref: "data" }, { b64: bytes(0xde, 0xad, 0xbe, 0xef) }] } },
		{
			name: "size counts data bytes",
			item: binaryItem,
			condition: { op: "eq", args: [{ fn: "size", args: [{ ref: "data" }] }, { val: 4 }] },
		},
		{
			name: "a byte literal never matches a text key",
			item: shippedOrder,
			condition: { op: "eq", args: [{ ref: "sortKey" }, { b64: bytes(0x61, 0x62) }] },
			expected: false,
		},
	]);
});

describe("expression showcase: SQLite scalar functions", () => {
	showcase([
		{
			name: "lower and trim normalize a nested email",
			item: shippedOrder,
			condition: {
				op: "eq",
				args: [
					{ fn: "sqlite.lower", args: [{ fn: "sqlite.trim", args: [{ ref: "data", path: "$.contact.email" }] }] },
					{ val: "ops@acme.example" },
				],
			},
		},
		{
			name: "upper on a nested field",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "sqlite.upper", args: [{ ref: "data", path: "$.contact.country" }] }, { val: "DE" }] },
		},
		{
			name: "upper on text data",
			item: note,
			condition: { op: "eq", args: [{ fn: "sqlite.upper", args: [{ ref: "data" }] }, { val: "FREE-FORM NOTE" }] },
		},
		{
			name: "length of a nested field",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "sqlite.length", args: [{ ref: "data", path: "$.status" }] }, { val: 7 }] },
		},
		{
			name: "substr of a nested field",
			item: shippedOrder,
			condition: {
				op: "eq",
				args: [{ fn: "sqlite.substr", args: [{ ref: "data", path: "$.status" }, { val: 1 }, { val: 4 }] }, { val: "ship" }],
			},
		},
		{
			name: "instr locates a substring",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "sqlite.instr", args: [{ ref: "data", path: "$.status" }, { val: "ship" }] }, { val: 1 }] },
		},
		{
			name: "replace rewrites a nested field",
			item: shippedOrder,
			condition: {
				op: "eq",
				args: [
					{ fn: "sqlite.replace", args: [{ ref: "data", path: "$.shipping.address.city" }, { val: "Berlin" }, { val: "BER" }] },
					{ val: "BER" },
				],
			},
		},
		{
			name: "concat joins two nested fields",
			item: shippedOrder,
			condition: {
				op: "eq",
				args: [
					{ fn: "sqlite.concat", args: [{ ref: "data", path: "$.contact.country" }, { val: "-" }, { ref: "data", path: "$.currency" }] },
					{ val: "de-EUR" },
				],
			},
		},
		{
			name: "abs of a negative nested number",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "sqlite.abs", args: [{ ref: "data", path: "$.balanceDelta" }] }, { val: 12.5 }] },
		},
		{
			name: "round of a nested number",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "sqlite.round", args: [{ ref: "data", path: "$.total" }] }, { val: 150 }] },
		},
		{
			name: "ceil of a nested number",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "sqlite.ceil", args: [{ ref: "data", path: "$.shipping.weightKg" }] }, { val: 3 }] },
		},
		{
			name: "mod of a nested number",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "sqlite.mod", args: [{ ref: "data", path: "$.quantity" }, { val: 2 }] }, { val: 1 }] },
		},
		{
			name: "coalesce supplies a default for an absent field",
			item: shippedOrder,
			condition: {
				op: "eq",
				args: [{ fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.coupon" }, { val: "none" }] }, { val: "none" }],
			},
		},
		{
			name: "ifnull supplies a default for JSON null",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "sqlite.ifnull", args: [{ ref: "data", path: "$.discount" }, { val: 0 }] }, { val: 0 }] },
		},
		{
			name: "like matches a nested field against a pattern",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "sqlite.like", args: [{ val: "ship%" }, { ref: "data", path: "$.status" }] }, { val: 1 }] },
		},
		{
			name: "glob matches a nested field against a pattern",
			item: shippedOrder,
			condition: {
				op: "eq",
				args: [{ fn: "sqlite.glob", args: [{ val: "*115" }, { ref: "data", path: "$.shipping.address.zip" }] }, { val: 1 }],
			},
		},
		{
			name: "iif picks a branch from a like result",
			item: shippedOrder,
			condition: {
				op: "eq",
				args: [
					{
						fn: "sqlite.iif",
						args: [{ fn: "sqlite.like", args: [{ val: "ship%" }, { ref: "data", path: "$.status" }] }, { val: "fast" }, { val: "slow" }],
					},
					{ val: "fast" },
				],
			},
		},
		{
			name: "typeof reports the SQLite storage type",
			item: shippedOrder,
			condition: { op: "eq", args: [{ fn: "sqlite.typeof", args: [{ ref: "data", path: "$.status" }] }, { val: "text" }] },
		},
		{
			name: "hex renders byte data",
			item: binaryItem,
			condition: { op: "eq", args: [{ fn: "sqlite.hex", args: [{ ref: "data" }] }, { val: "DEADBEEF" }] },
		},
		{
			name: "hex renders a byte sort key",
			item: binaryItem,
			condition: { op: "eq", args: [{ fn: "sqlite.hex", args: [{ ref: "sortKey" }] }, { val: "6162" }] },
		},
		{
			name: "unhex builds a byte literal to match a byte key",
			item: binaryItem,
			condition: { op: "eq", args: [{ ref: "sortKey" }, { fn: "sqlite.unhex", args: [{ val: "6162" }] }] },
		},
	]);
});

describe("expression showcase: complex expressions", () => {
	showcase([
		{
			name: "normalized nested email, total band, and tag membership",
			item: shippedOrder,
			condition: {
				op: "and",
				args: [
					{
						op: "eq",
						args: [
							{ fn: "sqlite.lower", args: [{ fn: "sqlite.trim", args: [{ ref: "data", path: "$.contact.email" }] }] },
							{ val: "ops@acme.example" },
						],
					},
					{ op: "between", args: [{ ref: "data", path: "$.total" }, { val: 100 }, { val: 200 }] },
					{ op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: "priority" }] },
				],
			},
		},
		{
			name: "normalized nested email, total band, and tag membership - rejected",
			item: pendingOrder,
			condition: {
				op: "and",
				args: [
					{
						op: "eq",
						args: [
							{ fn: "sqlite.lower", args: [{ fn: "sqlite.trim", args: [{ ref: "data", path: "$.contact.email" }] }] },
							{ val: "ops@acme.example" },
						],
					},
					{ op: "between", args: [{ ref: "data", path: "$.total" }, { val: 100 }, { val: 200 }] },
					{ op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: "priority" }] },
				],
			},
			expected: false,
		},
		{
			name: "the last array element is reached and reformatted",
			item: shippedOrder,
			condition: {
				op: "eq",
				args: [{ fn: "sqlite.upper", args: [{ ref: "data", path: "$.history[#-1].code" }] }, { val: "SHIPPED" }],
			},
		},
		{
			name: "key structure and nested data in one expression",
			item: shippedOrder,
			condition: {
				op: "and",
				args: [
					{ op: "begins_with", args: [{ ref: "hashKey" }, { val: "customer#" }] },
					{ op: "eq", args: [{ fn: "sqlite.substr", args: [{ ref: "sortKey" }, { val: 7 }, { val: 4 }] }, { val: "2024" }] },
					{ op: "eq", args: [{ ref: "data", path: "$.shipping.address.city" }, { val: "Berlin" }] },
				],
			},
		},
		{
			name: "math functions over two nested numbers",
			item: shippedOrder,
			condition: {
				op: "and",
				args: [
					{ op: "lte", args: [{ fn: "sqlite.ceil", args: [{ ref: "data", path: "$.shipping.weightKg" }] }, { val: 3 }] },
					{ op: "eq", args: [{ fn: "sqlite.mod", args: [{ ref: "data", path: "$.quantity" }, { val: 2 }] }, { val: 1 }] },
				],
			},
		},
		{
			name: "absence guard, type guard, and a state allowlist",
			item: shippedOrder,
			condition: {
				op: "and",
				args: [
					{ op: "not_exists", args: [{ ref: "data", path: "$.cancelledAt" }] },
					{ op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "data", path: "$.tags" }] }, { val: "array" }] },
					{ op: "in", args: [{ ref: "data", path: "$.status" }, { val: "shipped" }, { val: "delivered" }] },
					{ op: "gte", args: [{ ref: "v" }, { val: 1 }] },
				],
			},
		},
		{
			name: "an or branch nested under an and with a negated term",
			item: shippedOrder,
			condition: {
				op: "and",
				args: [
					{
						op: "or",
						args: [
							{ op: "eq", args: [{ ref: "data", path: "$.currency" }, { val: "EUR" }] },
							{ op: "eq", args: [{ ref: "data", path: "$.currency" }, { val: "USD" }] },
						],
					},
					{ op: "not", args: [{ op: "eq", args: [{ ref: "data", path: "$.tags[0]" }, { val: "backorder" }] }] },
				],
			},
		},
		{
			name: "an optimistic-lock guard on version and TTL",
			item: shippedOrder,
			condition: {
				op: "and",
				args: [
					{ op: "eq", args: [{ ref: "v" }, { val: 1 }] },
					{ op: "gt", args: [{ ref: "ttl" }, { val: 1_900_000_000 }] },
				],
			},
		},
		{
			name: "byte key, byte data, and a rendered hex digest together",
			item: binaryItem,
			condition: {
				op: "and",
				args: [
					{ op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "hashKey" }] }, { val: "bytes" }] },
					{ op: "begins_with", args: [{ ref: "sortKey" }, { b64: bytes(0x61) }] },
					{ op: "eq", args: [{ fn: "sqlite.hex", args: [{ ref: "data" }] }, { val: "DEADBEEF" }] },
					{ op: "eq", args: [{ fn: "size", args: [{ ref: "data" }] }, { val: 4 }] },
				],
			},
		},
	]);
});
