import { describe, expect, it } from "vitest";
import { queryScanStatement } from "../partition/partition-store.js";
import { KeyCodec } from "../../sharding/key-codec.js";
import { materializeExpressionBindings } from "./bindings.js";
import { compileProjectionExpression, compileQueryExpression } from "./compiler.js";
import { ExpressionError, type ExpressionErrorCode } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import {
	POOL_PARAM,
	PROJECTION_FIXED_BINDING_COUNT,
	PROJECTION_PLAN_VERSION,
	QUERY_MAX_TRAILING_BINDING_COUNT,
	QUERY_PLAN_VERSION,
	QUERY_WIDEST_SCAN_CONDITIONS,
	type CompiledProjectionPlan,
	type CompiledQueryPlan,
} from "./plan.js";
import { validateProjectionPlan, validateQueryPlan } from "./runtime.js";
import { validateProjectionExpression } from "./semantic.js";
import type { ConditionExpression, ProjectionExpression } from "./types.js";

function expectExpressionError(fn: () => unknown, code: ExpressionErrorCode, message?: RegExp): void {
	let caught: unknown;
	try {
		fn();
	} catch (error) {
		caught = error;
	}
	expect(caught).toBeInstanceOf(ExpressionError);
	expect((caught as ExpressionError).code).toBe(code);
	if (message !== undefined) expect((caught as Error).message).toMatch(message);
}

function assertRoundTrips(plan: CompiledProjectionPlan | CompiledQueryPlan): void {
	expect(JSON.parse(JSON.stringify(plan))).toEqual(plan);
}

const entry = (expr: ProjectionExpression["expr"], as?: string): ProjectionExpression => (as === undefined ? { expr } : { expr, as });

describe("projection validation", () => {
	it("resolves the default name of every reference kind and the exact path text", () => {
		const analysis = validateProjectionExpression([
			{ expr: { ref: "hashKey" } },
			{ expr: { ref: "sortKey" } },
			{ expr: { ref: "v" } },
			{ expr: { ref: "ttlAt" } },
			{ expr: { ref: "data" } },
			{ expr: { ref: "data", path: "$.a.b" } },
		]);
		expect(analysis.names).toEqual(["hashKey", "sortKey", "v", "ttlAt", "data", "$.a.b"]);
	});

	it("prefers the alias over the reference name", () => {
		expect(validateProjectionExpression([entry({ ref: "hashKey" }, "id")]).names).toEqual(["id"]);
	});

	it("requires an alias for a function, a scalar literal, and a byte literal", () => {
		for (const expr of [{ fn: "size", args: [{ ref: "data" }] }, { val: 1 }, { b64: "AQI=" }] as const) {
			expectExpressionError(() => validateProjectionExpression([entry(expr)]), "invalid_ast", /alias is required/);
		}
	});

	it("rejects duplicate resolved names, including an alias that collides with a default name", () => {
		expectExpressionError(
			() => validateProjectionExpression([{ expr: { ref: "hashKey" } }, { expr: { ref: "hashKey" } }]),
			"invalid_ast",
			/duplicate/,
		);
		expectExpressionError(
			() => validateProjectionExpression([entry({ ref: "sortKey" }, "hashKey"), { expr: { ref: "hashKey" } }]),
			"invalid_ast",
			/duplicate/,
		);
	});

	it("rejects a non-string and an empty alias", () => {
		expectExpressionError(
			() => validateProjectionExpression([{ expr: { ref: "hashKey" }, as: 1 as unknown as string }]),
			"invalid_ast",
			/must be a string/,
		);
		expectExpressionError(() => validateProjectionExpression([entry({ ref: "hashKey" }, "")]), "invalid_ast", /must not be empty/);
	});

	it("accepts a 256-byte alias and rejects 257 bytes, measured in UTF-8 bytes", () => {
		// "é" is two UTF-8 bytes, so the limit check sees bytes, not characters.
		expect(validateProjectionExpression([entry({ ref: "hashKey" }, "é".repeat(128))]).names).toEqual(["é".repeat(128)]);
		expectExpressionError(
			() => validateProjectionExpression([entry({ ref: "hashKey" }, `${"é".repeat(128)}x`)]),
			"complexity_limit",
			/alias limit/,
		);
	});

	it("accepts entries up to the limit and rejects one above", () => {
		const entries = (count: number) => Array.from({ length: count }, (_, i) => entry({ ref: "hashKey" }, `k${i}`));
		expect(validateProjectionExpression(entries(EXPRESSION_LIMITS.projectionEntries)).names).toHaveLength(
			EXPRESSION_LIMITS.projectionEntries,
		);
		expectExpressionError(
			() => validateProjectionExpression(entries(EXPRESSION_LIMITS.projectionEntries + 1)),
			"complexity_limit",
			/entry limit/,
		);
	});

	it("rejects an empty list, a non-array, and an entry with an extra field", () => {
		expectExpressionError(() => validateProjectionExpression([]), "invalid_ast", /at least one entry/);
		expectExpressionError(() => validateProjectionExpression("x"), "invalid_ast", /at least one entry/);
		expectExpressionError(() => validateProjectionExpression([{ expr: { ref: "hashKey" }, extra: true }]), "invalid_ast", /fields/);
	});

	it("rejects the append marker in a path and accepts a reverse index", () => {
		expectExpressionError(() => validateProjectionExpression([{ expr: { ref: "data", path: "$.items[#]" } }]), "invalid_path");
		expect(validateProjectionExpression([{ expr: { ref: "data", path: "$.items[#-1]" } }]).names).toEqual(["$.items[#-1]"]);
	});

	it("rejects update-only functions in a projection and in a filter", () => {
		const updateFns = [
			{ fn: "if_not_exists", args: [{ ref: "data", path: "$.x" }, { val: 0 }] },
			{ fn: "+", args: [{ val: 1 }, { val: 1 }] },
		] as const;
		for (const fn of updateFns) {
			expectExpressionError(() => validateProjectionExpression([entry(fn, "x")]), "invalid_function");
			expectExpressionError(() => compileQueryExpression({ filter: { op: "eq", args: [fn, { val: 0 }] } }), "invalid_function");
		}
	});

	it("rejects the complete data as a direct argument of a SQLite function in a projection", () => {
		expectExpressionError(
			() => validateProjectionExpression([{ expr: { fn: "sqlite.coalesce", args: [{ ref: "data" }, { val: 0 }] }, as: "x" }]),
			"invalid_type",
			/complete data/,
		);
		expectExpressionError(
			() =>
				validateProjectionExpression([{ expr: { fn: "sqlite.upper", args: [{ fn: "sqlite.hex", args: [{ ref: "data" }] }] }, as: "x" }]),
			"invalid_type",
			/complete data/,
		);
		// Fokos operations read the logical value and keep accepting the complete data.
		expect(validateProjectionExpression([entry({ fn: "size", args: [{ ref: "data" }] }, "n")]).names).toEqual(["n"]);
		expect(validateProjectionExpression([entry({ fn: "attribute_type", args: [{ ref: "data" }] }, "t")]).names).toEqual(["t"]);
		// A filter never returns a function result to a caller, so the same call compiles there.
		const plan = compileQueryExpression({
			filter: { op: "eq", args: [{ fn: "sqlite.coalesce", args: [{ ref: "data" }, { val: 0 }] }, { val: 0 }] },
		});
		expect(plan.filterSql).not.toBeNull();
	});

	it("treats an undefined alias as absent", () => {
		expect(validateProjectionExpression([{ expr: { ref: "hashKey" }, as: undefined }]).names).toEqual(["hashKey"]);
	});
});

describe("projection compiler", () => {
	it("creates a versioned JSON-safe projection plan", () => {
		const projection: readonly ProjectionExpression[] = [
			{ expr: { ref: "hashKey" }, as: "id" },
			{ expr: { ref: "data", path: "$.total" } },
		];
		const plan = compileProjectionExpression(projection);
		expect(plan.version).toBe(PROJECTION_PLAN_VERSION);
		expect(plan.kind).toBe("projection");
		expect(plan.bindingLayout).toBe("pool");
		expect(plan.names).toEqual(["id", "$.total"]);
		expect(plan.valueSql).toHaveLength(2);
		expect(plan.typeSql).toHaveLength(2);
		expect(plan.bindings).toEqual([{ kind: "path", value: "$.total" }]);
		expect(plan.completeBindingCount).toBe(POOL_PARAM + PROJECTION_FIXED_BINDING_COUNT);
		expect(plan.requiredColumns).toEqual(["hk", "data_kind", "data"]);
		expect(plan.dataDependencies).toEqual({ completeData: false, paths: ["$.total"] });
		assertRoundTrips(plan);
		expect(validateProjectionPlan(plan)).toContain("AS p0");
	});

	it("numbers pool elements densely across value and type fragments", () => {
		const plan = compileProjectionExpression([entry({ fn: "size", args: [{ val: "abc" }] }, "n")]);
		const indexes = new Set(
			[...plan.valueSql, ...plan.typeSql].flatMap((sql) => [...sql.matchAll(/\?\d+, '\$\[(\d+)\]'/g)].map((m) => Number(m[1]))),
		);
		expect([...indexes].sort((a, b) => a - b)).toEqual(Array.from({ length: plan.bindingCount }, (_, i) => i));
		expect(plan.bindings.length).toBe(plan.bindingCount);
		assertRoundTrips(plan);
	});

	it("a projection with no descriptor binds the empty pool", () => {
		const plan = compileProjectionExpression([{ expr: { ref: "hashKey" } }]);
		expect(plan.bindingCount).toBe(0);
		expect(plan.completeBindingCount).toBe(POOL_PARAM + PROJECTION_FIXED_BINDING_COUNT);
		expect(materializeExpressionBindings(plan.bindings, "pool")).toEqual(["[]"]);
		assertRoundTrips(plan);
	});
});

describe("query compiler", () => {
	it("requires a filter or a projection", () => {
		expectExpressionError(() => compileQueryExpression({}), "invalid_ast", /filter or a projection/);
	});

	it("deduplicates a path shared by the filter and the projection", () => {
		const plan = compileQueryExpression({
			filter: { op: "eq", args: [{ ref: "data", path: "$.a" }, { val: 1 }] },
			projection: [{ expr: { ref: "data", path: "$.a" } }],
		});
		expect(plan.bindings.filter((binding) => binding.kind === "path" && binding.value === "$.a")).toHaveLength(1);
		expect(plan.completeBindingCount).toBe(5);
		expect(plan.filterIdentity).not.toBeNull();
		expect(plan.projectionIdentity).not.toBeNull();
		expect(plan.requiredColumns).toEqual(["data_kind", "data"]);
		expect(plan.dataDependencies).toEqual({ completeData: false, paths: ["$.a"] });
		assertRoundTrips(plan);
		expect(() => validateQueryPlan(plan)).not.toThrow();
	});

	it("sets filterSql and projection to null for the absent half", () => {
		const filterOnly = compileQueryExpression({ filter: { op: "exists", args: [{ ref: "hashKey" }] } });
		expect(filterOnly.filterSql).not.toBeNull();
		expect(filterOnly.projection).toBeNull();
		expect(filterOnly.projectionIdentity).toBeNull();
		assertRoundTrips(filterOnly);

		const projectionOnly = compileQueryExpression({ projection: [{ expr: { ref: "hashKey" } }] });
		expect(projectionOnly.filterSql).toBeNull();
		expect(projectionOnly.filterIdentity).toBeNull();
		expect(projectionOnly.projection).not.toBeNull();
		assertRoundTrips(projectionOnly);
	});

	it("compiles a 100-choice filter with a maximum-size projection in one plan", () => {
		const plan = compileQueryExpression({
			filter: {
				op: "in",
				args: [{ ref: "v" }, ...Array.from({ length: EXPRESSION_LIMITS.inChoices }, (_, i) => ({ val: i }))],
			} as unknown as ConditionExpression,
			projection: Array.from({ length: EXPRESSION_LIMITS.projectionEntries }, (_, i) => entry({ val: i }, `k${i}`)),
		});
		expect(plan.completeBindingCount).toBe(5);
		assertRoundTrips(plan);
		expect(() => validateQueryPlan(plan)).not.toThrow();
	});

	it("numbers pool elements densely in filterSql after folding", () => {
		for (const filter of [
			{ op: "eq", args: [{ ref: "data", path: "$.value" }, { val: null }] },
			{ op: "begins_with", args: [{ ref: "data", path: "$.value" }, { val: "prefix" }] },
			{ op: "contains", args: [{ ref: "data", path: "$.values" }, { val: true }] },
			{ op: "eq", args: [{ fn: "size", args: [{ ref: "data", path: "$.values" }] }, { val: 3 }] },
			// The scalar branch of contains folds away for a number search.
			{ op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: 3 }] },
			// The array branch of contains folds away for a text-literal container.
			{ op: "contains", args: [{ val: "abc" }, { val: "b" }] },
		] as const satisfies readonly ConditionExpression[]) {
			const plan = compileQueryExpression({ filter });
			const indexes = new Set([...plan.filterSql!.matchAll(/\?\d+, '\$\[(\d+)\]'/g)].map((match) => Number(match[1])));
			expect(
				[...indexes].sort((a, b) => a - b),
				JSON.stringify(filter),
			).toEqual(Array.from({ length: plan.bindingCount }, (_, i) => i));
			expect(plan.bindings.length, JSON.stringify(filter)).toBe(plan.bindingCount);
			assertRoundTrips(plan);
		}
	});

	it("rejects a pooled binding payload one byte above the limit", () => {
		const heavy = new Uint8Array(200_000).toBase64();
		const filter = (s: string): ConditionExpression => ({
			op: "and",
			args: [
				{ op: "eq", args: [{ ref: "data" }, { b64: heavy }] },
				{ op: "eq", args: [{ ref: "data", path: "$.s" }, { val: s }] },
			],
		});
		// The pool text is all ASCII, so its string length equals its byte length.
		const base = (materializeExpressionBindings(compileQueryExpression({ filter: filter("") }).bindings, "pool")[0] as string).length;
		const atLimit = "a".repeat(EXPRESSION_LIMITS.canonicalPayloadBytes - base);
		const poolText = materializeExpressionBindings(compileQueryExpression({ filter: filter(atLimit) }).bindings, "pool")[0] as string;
		expect(poolText.length).toBe(EXPRESSION_LIMITS.canonicalPayloadBytes);
		expectExpressionError(() => compileQueryExpression({ filter: filter(`${atLimit}x`) }), "sql_limit", /payload limit/);
	});

	it("keeps the widest scan tail in step with the store's scan shape", () => {
		// The widest scan tail in the plan module must follow the store's scan shape.
		const { sql } = queryScanStatement({
			hk: KeyCodec.encode("h"),
			lower: KeyCodec.encode("a"),
			lowerInclusive: true,
			upper: KeyCodec.encode("z"),
			upperInclusive: true,
			cursor: { hk: KeyCodec.encode("h"), sk: KeyCodec.encode("m"), inclusive: true },
			direction: "desc",
			select: "count",
			limit: 1,
			plan: null,
		});
		const parameterCount = sql.match(/\?/g)?.length;
		expect(parameterCount).toBe(QUERY_MAX_TRAILING_BINDING_COUNT);
		expect(parameterCount).toBe(QUERY_WIDEST_SCAN_CONDITIONS.length + 1);
	});
});

describe("pool binding materialization", () => {
	it("packs every descriptor kind into one JSON array text", () => {
		const bindings = materializeExpressionBindings(
			[
				{ kind: "val", value: true },
				{ kind: "val", value: null },
				{ kind: "val", value: "x" },
				{ kind: "path", value: "$.a" },
				{ kind: "keyText", value: "ab" },
				{ kind: "b64", value: "AQI=" },
			],
			"pool",
		);
		expect(bindings).toEqual([`[true,null,"x","$.a","${KeyCodec.encode("ab").toHex()}","0102"]`]);
		expect(KeyCodec.encode("ab").toHex()).toBe("6162");
	});

	it("returns an empty array text for no descriptors", () => {
		expect(materializeExpressionBindings([], "pool")).toEqual(["[]"]);
	});
});

describe("plan validators", () => {
	const projectionPlan = compileProjectionExpression([entry({ ref: "data", path: "$.a" })]);
	const queryPlan = compileQueryExpression({
		filter: { op: "eq", args: [{ ref: "v" }, { val: 1 }] },
		projection: [{ expr: { ref: "hashKey" } }],
	});

	it("accepts compiled plans", () => {
		expect(validateProjectionPlan(projectionPlan)).toContain("FROM items AS i");
		expect(() => validateQueryPlan(queryPlan)).not.toThrow();
	});

	it("rejects a wrong version, kind, or binding layout", () => {
		expectExpressionError(
			() => validateProjectionPlan({ ...projectionPlan, version: 2 } as unknown as CompiledProjectionPlan),
			"runtime_capability",
		);
		expectExpressionError(
			() => validateProjectionPlan({ ...projectionPlan, kind: "query" } as unknown as CompiledProjectionPlan),
			"runtime_capability",
		);
		expectExpressionError(
			() => validateProjectionPlan({ ...projectionPlan, bindingLayout: "direct" } as unknown as CompiledProjectionPlan),
			"runtime_capability",
			/binding layout/,
		);
		expectExpressionError(() => validateQueryPlan({ ...queryPlan, version: 2 } as unknown as CompiledQueryPlan), "runtime_capability");
		expectExpressionError(
			() => validateQueryPlan({ ...queryPlan, kind: "projection" } as unknown as CompiledQueryPlan),
			"runtime_capability",
		);
		expectExpressionError(
			() => validateQueryPlan({ ...queryPlan, bindingLayout: "direct" } as unknown as CompiledQueryPlan),
			"runtime_capability",
			/binding layout/,
		);
	});

	it("rejects an inconsistent binding count and a wrong complete count", () => {
		expectExpressionError(
			() => validateProjectionPlan({ ...projectionPlan, bindingCount: projectionPlan.bindingCount + 1 }),
			"sql_limit",
			/binding count/,
		);
		expectExpressionError(
			() => validateProjectionPlan({ ...projectionPlan, completeBindingCount: projectionPlan.completeBindingCount + 1 }),
			"sql_limit",
			/binding count/,
		);
		expectExpressionError(
			() => validateQueryPlan({ ...queryPlan, bindingCount: queryPlan.bindingCount + 1 }),
			"sql_limit",
			/binding count/,
		);
		expectExpressionError(() => validateQueryPlan({ ...queryPlan, completeBindingCount: 4 }), "sql_limit", /binding count/);
	});

	it("rejects a projection shape mismatch and a query plan with neither half", () => {
		expectExpressionError(() => validateProjectionPlan({ ...projectionPlan, names: [] }), "runtime_capability", /shape/);
		expectExpressionError(
			() => validateQueryPlan({ ...queryPlan, filterSql: null, projection: null }),
			"runtime_capability",
			/neither a filter nor a projection/,
		);
		expectExpressionError(
			() => validateQueryPlan({ ...queryPlan, projection: { ...queryPlan.projection!, names: [] } }),
			"runtime_capability",
			/shape/,
		);
	});
});
