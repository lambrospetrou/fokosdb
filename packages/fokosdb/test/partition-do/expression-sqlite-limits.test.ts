import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { compileConditionExpression, compileProjectionExpression, compileUpdateExpression } from "../../src/shared/expression/compiler.js";
import { EXPRESSION_LIMITS } from "../../src/shared/expression/limits.js";
import { OPERATION_REGISTRY } from "../../src/shared/expression/operation-registry.js";
import { CONDITION_FIXED_BINDING_COUNT } from "../../src/shared/expression/plan.js";
import type { ConditionExpression, ExpressionValue, UpdateExpression } from "../../src/shared/expression/types.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { kb, makeStub } from "./helpers.js";

/**
 * SQLite refuses a statement whose expression is deeper than 100 levels, and the error does not name
 * the cause. Each expression here is at a limit of the expression engine, where its SQL is deepest.
 * The compiler accepts it, so it must run.
 */
describe("PartitionDO — an expression at the limits of the engine runs in SQLite", () => {
	const key = { hashKey: kb("hk"), sortKey: kb("sk") };
	const ok = { outcome: "ok" };
	const rejected = { outcome: "rejected", reason: { code: "condition_failed" } };
	const vIs = (value: number): ConditionExpression => ({ op: "eq", args: [{ ref: "v" }, { val: value }] });

	/** A put of version 1, then a put with the condition. The condition reads `v` = 1. */
	async function passes(condition: ConditionExpression, n = "t95"): Promise<unknown> {
		const { ctx, rpc } = makeStub();
		await rpc.apiPutItem(ctx, { ...key, data: JSON.stringify({ n }), kind: "json" });
		return await rpc.apiPutItem(ctx, { ...key, data: "{}", kind: "json", condition });
	}

	// One operator for the chain and one for each term: the largest count of terms.
	const TERMS = EXPRESSION_LIMITS.operatorsAndFunctions - 1;

	it("an `and` with the largest count of terms", async () => {
		const terms = Array.from({ length: TERMS }, () => vIs(1));
		expect(await passes({ op: "and", args: terms })).toMatchObject(ok);
		expect(await passes({ op: "and", args: [...terms.slice(1), vIs(2)] })).toMatchObject(rejected);
	});

	it("an `or` with the largest count of terms", async () => {
		// The values 2 to 91 keep the distinct literals below the binding limit.
		const terms = Array.from({ length: TERMS }, (_, i) => vIs(2 + (i % 90)));
		expect(await passes({ op: "or", args: terms })).toMatchObject(rejected);
		expect(await passes({ op: "or", args: [...terms.slice(1), vIs(1)] })).toMatchObject(ok);
	});

	it("an `in` with the largest count of choices of more than one type", async () => {
		// The path takes one binding, and each distinct choice takes one.
		const distinct = EXPRESSION_LIMITS.completeStatementBindings - CONDITION_FIXED_BINDING_COUNT - 1;
		const choices = Array.from({ length: EXPRESSION_LIMITS.inChoices }, (_, i) => {
			const n = i % distinct;
			return { val: n % 2 === 0 ? n : `t${n}` };
		});
		const target = { ref: "data", path: "$.n" } as const;
		expect(compileConditionExpression({ op: "in", args: [target, ...choices] }).bindingCount).toBe(distinct + 1);
		expect(await passes({ op: "in", args: [target, ...choices] }, "t95")).toMatchObject(ok);
		expect(await passes({ op: "in", args: [target, ...choices] }, "t94")).toMatchObject(rejected);
	});

	it.each(["not", "and", "or"] as const)("a `%s` nested to the depth limit", async (op) => {
		let condition = vIs(1);
		for (let depth = 2; depth < EXPRESSION_LIMITS.astDepth; depth++) {
			condition = op === "not" ? { op, args: [condition] } : { op, args: [condition, vIs(1)] };
		}
		// An even count of `not` is around the comparison.
		expect(await passes(condition)).toMatchObject(ok);
		expect(() => compileConditionExpression({ op: "not", args: [{ op: "not", args: [condition] }] })).toThrow(/AST depth/);
	});

	it("each operation that can hold itself, nested to the depth limit, in a condition, a projection, and an update", async () => {
		const { ctx, stub, rpc } = makeStub();
		await rpc.apiPutItem(ctx, { ...key, data: JSON.stringify({ n: 1, s: "a", out: 0 }), kind: "json" });

		const seeds: ExpressionValue[] = [{ val: 1 }, { val: "a" }, { ref: "data", path: "$.n" }, { ref: "data", path: "$.s" }, { ref: "v" }];
		const fillers: ExpressionValue[] = [{ val: 1 }, { val: "a" }];
		/** The operation `depth` times around the seed, each time at argument `position`. */
		const nest = (fn: string, arity: number, position: number, seed: ExpressionValue, filler: ExpressionValue, depth: number) => {
			let value = seed;
			for (let d = 0; d < depth; d++) {
				value = { fn, args: Array.from({ length: arity }, (_, i) => (i === position ? value : filler)) };
			}
			return value;
		};
		const compiles = (compile: () => unknown): boolean => {
			try {
				compile();
				return true;
			} catch {
				return false;
			}
		};

		await runInDurableObject(stub, (_instance, state) => {
			const store = new PartitionStore(state.storage);
			const contexts = {
				condition: (value: ExpressionValue) => {
					const condition: ConditionExpression = { op: "eq", args: [value, value] };
					return {
						compile: () => compileConditionExpression(condition),
						run: () => store.evaluateCondition(condition, key.hashKey, key.sortKey),
					};
				},
				projection: (value: ExpressionValue) => {
					const projection = [{ expr: value, as: "x" }];
					return {
						compile: () => compileProjectionExpression(projection),
						run: () => store.getItemProjected(projection, key.hashKey, key.sortKey),
					};
				},
				update: (value: ExpressionValue) => {
					const update: UpdateExpression = [{ action: "set", target: { ref: "data", path: "$.out" }, value }];
					return {
						compile: () => compileUpdateExpression(update),
						run: () => {
							store.probeUpdate(update, key.hashKey, key.sortKey);
							store.updateItemSingleShot({ hk: key.hashKey, sk: key.sortKey, plan: update, txOrderTs: 1 });
						},
					};
				},
			};

			const nested = new Set<string>();
			const failures: string[] = [];
			for (const [name, definition] of OPERATION_REGISTRY) {
				const arity = Math.max(definition.arity[0], 1);
				for (let position = 0; position < Math.min(arity, 3); position++) {
					for (const [contextName, make] of Object.entries(contexts)) {
						// The first seed and filler that give a valid nesting of this operation.
						const pair = seeds
							.flatMap((seed) => fillers.map((filler) => [seed, filler] as const))
							.find(([seed, filler]) => compiles(make(nest(name, arity, position, seed, filler, 3)).compile));
						if (pair === undefined) {
							continue;
						}
						// The deepest nesting that compiles.
						let depth = EXPRESSION_LIMITS.astDepth;
						while (!compiles(make(nest(name, arity, position, pair[0], pair[1], depth)).compile)) {
							depth--;
						}
						nested.add(name);
						try {
							make(nest(name, arity, position, pair[0], pair[1], depth)).run();
						} catch (error) {
							const cause = (error as { cause?: { cause?: { message?: string } } }).cause?.cause?.message;
							failures.push(`${contextName} ${name}: ${cause ?? (error as Error).message}`);
						}
					}
				}
			}

			for (const name of ["+", "-", "*", "if_not_exists", "attribute_type", "sqlite.coalesce", "sqlite.iif", "sqlite.abs"]) {
				expect(nested, name).toContain(name);
			}
			// `hex` and `quote` make their value two times longer at each level. The value, not the
			// statement, is then above a limit, and no other operation fails.
			for (const failure of failures) {
				expect(failure).toMatch(/ sqlite\.(hex|quote): .*(string or blob too big|item_too_large)/);
			}
		});
	});
});
