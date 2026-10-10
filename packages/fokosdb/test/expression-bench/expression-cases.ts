/**
 * The cases of the expression benchmarks, and the work that a partition does for one expression before
 * SQLite runs it. `expression.workerd-bench.ts` times the cases inside a Durable Object, and
 * `expression-alloc.mjs` counts their heap bytes in Node.
 *
 * A case name is a stable identifier: the tables in `docs/agent-plans/` refer to it. Add a case at the end
 * of its group, and do not change the tree of an existing case.
 */
import { materializedPlanBindings } from "../../src/shared/expression/bindings.js";
import {
	compileConditionExpression,
	compileProjectionExpression,
	compileQueryExpression,
	compileUpdateExpression,
} from "../../src/shared/expression/compiler.js";
import { canonicalConditionIdentity, canonicalProjectionIdentity, canonicalUpdateIdentity } from "../../src/shared/expression/identity.js";
import { composeConditionStatement, composeProjectionStatement, composeQueryStatement } from "../../src/shared/expression/plan.js";
import { composeUpdateProbeStatement } from "../../src/shared/expression/runtime.js";
import {
	validateConditionExpression,
	validateProjectionExpression,
	validateUpdateExpression,
} from "../../src/shared/expression/semantic.js";
import type {
	ConditionExpression,
	ExpressionReference,
	ExpressionValue,
	ProjectionExpression,
	UpdateExpression,
} from "../../src/shared/expression/types.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";

type QueryTree = { filter?: ConditionExpression; projection?: readonly ProjectionExpression[] };

export type ExpressionCase =
	| { name: string; kind: "condition"; tree: ConditionExpression }
	| { name: string; kind: "update"; tree: UpdateExpression }
	| { name: string; kind: "projection"; tree: readonly ProjectionExpression[] }
	| { name: string; kind: "query"; tree: QueryTree };

const path = (p: string): ExpressionReference => ({ ref: "data", path: p });
const val = (value: string | number | boolean | null): ExpressionValue => ({ val: value });
const many = <T>(length: number, item: (i: number) => T): T[] => Array.from({ length }, (_, i) => item(i));

const FOUR_TERMS: ConditionExpression = {
	op: "and",
	args: [
		{ op: "eq", args: [path("$.status"), val("pending")] },
		{ op: "in", args: [path("$.region"), val("us-east-1"), val("eu-west-1")] },
		{ op: "gte", args: [path("$.total"), val(100)] },
		{ op: "not_exists", args: [path("$.cancelledAt")] },
	],
};
const FORTY_PATHS: ConditionExpression = {
	op: "and",
	args: many(40, (i) => ({ op: "eq", args: [path(`$.field${i}`), val(`value-${i}`)] })),
};
const FORTY_EIGHT_PROJECTIONS: ProjectionExpression[] = many(48, (i) => ({ expr: path(`$.field${i}.nested`) }));

export const CASES: readonly ExpressionCase[] = [
	{ name: "cond: not_exists(hashKey)", kind: "condition", tree: { op: "not_exists", args: [{ ref: "hashKey" }] } },
	{
		name: "cond: optimistic lock",
		kind: "condition",
		tree: {
			op: "and",
			args: [
				{ op: "exists", args: [{ ref: "hashKey" }] },
				{ op: "eq", args: [{ ref: "v" }, val(1)] },
			],
		},
	},
	{ name: "cond: one path eq", kind: "condition", tree: { op: "eq", args: [path("$.status"), val("pending")] } },
	{ name: "cond: contains on an array path", kind: "condition", tree: { op: "contains", args: [path("$.permissions"), val("write")] } },
	{ name: "cond: four terms", kind: "condition", tree: FOUR_TERMS },
	{
		name: "cond: nested access policy",
		kind: "condition",
		tree: {
			op: "and",
			args: [
				{ op: "eq", args: [path("$.tenantId"), val("tenant-123")] },
				{
					op: "or",
					args: [
						{ op: "eq", args: [path("$.role"), val("admin")] },
						{ op: "contains", args: [path("$.permissions"), val("write")] },
					],
				},
				{
					op: "or",
					args: [
						{ op: "not_exists", args: [{ ref: "ttlAt" }] },
						{ op: "gt", args: [{ ref: "ttlAt" }, val(1_788_000_000)] },
					],
				},
			],
		},
	},
	{ name: "cond: 40 distinct path eq", kind: "condition", tree: FORTY_PATHS },
	{
		// Workers SQLite refuses an expression deeper than 100, and SQLite parses a chain of `OR` terms as
		// one level for each term. 80 terms, with the depth of one term, stay below that depth.
		name: "cond: 80 eq on one path",
		kind: "condition",
		tree: { op: "or", args: many(80, (i) => ({ op: "eq", args: [path("$.status"), val(`value-${i}`)] })) },
	},
	{ name: "upd: set 1 literal", kind: "update", tree: [{ action: "set", target: { ref: "data", path: "$.status" }, value: val("done") }] },
	{ name: "upd: remove 1 path", kind: "update", tree: [{ action: "remove", target: { ref: "data", path: "$.cancelledAt" } }] },
	{
		name: "upd: counter and timestamp",
		kind: "update",
		tree: [
			{
				action: "set",
				target: { ref: "data", path: "$.count" },
				value: { fn: "+", args: [{ fn: "if_not_exists", args: [path("$.count"), val(0)] }, val(1)] },
			},
			{ action: "set", target: { ref: "data", path: "$.updatedAt" }, value: val(1_788_000_000) },
		],
	},
	{
		name: "upd: 20 actions with arithmetic",
		kind: "update",
		tree: many(20, (i) => ({
			action: "set" as const,
			target: { ref: "data" as const, path: `$.c${i}` },
			value: { fn: "+", args: [{ fn: "if_not_exists", args: [path(`$.c${i}`), val(0)] }, val(i + 1)] },
		})),
	},
	{
		name: "upd: 32 literal sets",
		kind: "update",
		tree: many(32, (i) => ({ action: "set" as const, target: { ref: "data" as const, path: `$.f${i}` }, value: val(`value-${i}`) })),
	},
	{ name: "proj: 1 path", kind: "projection", tree: [{ expr: path("$.status") }] },
	{
		name: "proj: 3 paths and v",
		kind: "projection",
		tree: [{ expr: path("$.status") }, { expr: path("$.region") }, { expr: path("$.total") }, { expr: { ref: "v" } }],
	},
	{ name: "proj: 48 paths", kind: "projection", tree: FORTY_EIGHT_PROJECTIONS },
	{ name: "query: one path eq filter", kind: "query", tree: { filter: { op: "eq", args: [path("$.status"), val("pending")] } } },
	{
		name: "query: four-term filter and 5 projections",
		kind: "query",
		tree: {
			filter: FOUR_TERMS,
			projection: [
				{ expr: path("$.status") },
				{ expr: path("$.region") },
				{ expr: path("$.total") },
				{ expr: path("$.owner") },
				{ expr: { ref: "v" } },
			],
		},
	},
	{ name: "query: 40-term filter and 48 projections", kind: "query", tree: { filter: FORTY_PATHS, projection: FORTY_EIGHT_PROJECTIONS } },
];

/** The hash key of every benchmark item. */
export const BENCH_HASH_KEY = KeyCodec.encode("bench");
/** The number of items that a query case scans. */
export const BENCH_ITEM_COUNT = 200;

export const benchSortKey = (i: number) => KeyCodec.encode(`item-${String(i).padStart(5, "0")}`);

/**
 * The JSON document of benchmark item `i`. The first item passes every condition case except the two
 * that no item passes: `not_exists(hashKey)` and the 80 `eq` terms. One item in three has the status
 * `pending`, so a filter case keeps a part of the page.
 */
export function benchDocument(i: number): Record<string, unknown> {
	const doc: Record<string, unknown> = {
		status: i % 3 === 0 ? "pending" : "done",
		region: ["us-east-1", "eu-west-1", "ap-south-1"][i % 3],
		total: 100 + i,
		owner: `user-${i}`,
		tenantId: "tenant-123",
		role: i % 2 === 0 ? "admin" : "member",
		permissions: ["read", "write"],
		count: i,
		description: "x".repeat(300),
	};
	for (let f = 0; f < 48; f++) {
		// A field that the 40 `eq` terms read as text and the 48 projections read through `.nested`.
		doc[`field${f}`] = f < 40 ? `value-${f}` : { nested: f };
	}
	return doc;
}

export function treeBytes(c: ExpressionCase): number {
	return JSON.stringify(c.tree).length;
}

export function validateCase(c: ExpressionCase): unknown {
	switch (c.kind) {
		case "condition":
			return validateConditionExpression(c.tree);
		case "update":
			return validateUpdateExpression(c.tree);
		case "projection":
			return validateProjectionExpression(c.tree);
		case "query":
			return [
				c.tree.filter && validateConditionExpression(c.tree.filter, "filter"),
				c.tree.projection && validateProjectionExpression(c.tree.projection),
			];
	}
}

export function identityCase(c: ExpressionCase): unknown {
	switch (c.kind) {
		case "condition":
			return canonicalConditionIdentity(c.tree);
		case "update":
			return canonicalUpdateIdentity(c.tree);
		case "projection":
			return canonicalProjectionIdentity(c.tree);
		case "query":
			return [
				c.tree.filter && canonicalConditionIdentity(c.tree.filter),
				c.tree.projection && canonicalProjectionIdentity(c.tree.projection),
			];
	}
}

export function compileCase(c: ExpressionCase): unknown {
	switch (c.kind) {
		case "condition":
			return compileConditionExpression(c.tree);
		case "update":
			return compileUpdateExpression(c.tree);
		case "projection":
			return compileProjectionExpression(c.tree);
		case "query":
			return compileQueryExpression(c.tree);
	}
}

export type PreparedStatement = { sql: string; params: unknown[]; bindingCount: number };

/**
 * The partition path of one expression: compile the tree, compose the statement, and make the bound
 * values. The result is what `PartitionStore` gives to `sql.exec`. An update case
 * composes its probe statement, which reads the item and writes nothing.
 */
export function prepareCase(c: ExpressionCase, sortKey: Uint8Array = benchSortKey(0)): PreparedStatement {
	switch (c.kind) {
		case "condition": {
			const plan = compileConditionExpression(c.tree);
			const sql = composeConditionStatement(plan.sql);
			return { sql, params: [BENCH_HASH_KEY, sortKey, ...materializedPlanBindings(plan)], bindingCount: plan.bindingCount };
		}
		case "update": {
			const plan = compileUpdateExpression(c.tree);
			const sql = composeUpdateProbeStatement(plan);
			return { sql, params: [BENCH_HASH_KEY, sortKey, ...materializedPlanBindings(plan)], bindingCount: plan.bindingCount };
		}
		case "projection": {
			const plan = compileProjectionExpression(c.tree);
			const sql = composeProjectionStatement(plan);
			return { sql, params: [...materializedPlanBindings(plan, "pool"), BENCH_HASH_KEY, sortKey], bindingCount: plan.bindingCount };
		}
		case "query": {
			const plan = compileQueryExpression(c.tree);
			const sql = composeQueryStatement(plan, { select: "projection", direction: "asc", scanConditions: ["hk = ?"] });
			return {
				sql,
				params: [...materializedPlanBindings(plan, "pool"), BENCH_HASH_KEY, BENCH_ITEM_COUNT],
				bindingCount: plan.bindingCount,
			};
		}
	}
}
