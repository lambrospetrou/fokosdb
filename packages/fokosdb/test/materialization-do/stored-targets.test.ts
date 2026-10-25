import { env, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { MaterializationDO } from "../../src/server/materialization/do-materialization.js";
import { MaterializationStore } from "../../src/server/materialization/materialization-store.js";
import type { FokosTargetDefinition } from "../../src/server/materialization/targets.js";
import type { ExpressionValue } from "../../src/shared/expression/types.js";

/** A tree of `+` with 8 levels: it passes the semantic checks, and its compiled SQL is above the SQL limit. */
function aboveSqlLimit(): ExpressionValue {
	let value: ExpressionValue = { ref: "data", path: "$.n" };
	for (let i = 0; i < 8; i++) {
		value = { fn: "+", args: [value, value] };
	}
	return value;
}

/** Stores `targets` as the target list of a new partition, restarts its instance, and returns its errors. */
async function errorsOfNewInstance(targets: readonly FokosTargetDefinition[]): Promise<Array<Record<string, unknown>>> {
	const name = `stored-targets.${crypto.randomUUID()}`;
	const stub = env.MATERIALIZATION_DO.getByName(name);
	await runInDurableObject(stub, (_instance: MaterializationDO, state: DurableObjectState) => {
		new MaterializationStore(state.storage).storeTargets(targets);
	});
	await runInDurableObject(stub, (_instance: MaterializationDO, state: DurableObjectState) => {
		state.abort("test restart");
	}).catch(() => {});
	const errors = vi.spyOn(console, "error").mockImplementation(() => {});
	// A stub breaks with its instance, so a new stub reaches the new instance. Its constructor checks
	// the stored list before this callback runs.
	await runInDurableObject(env.MATERIALIZATION_DO.getByName(name), () => {});
	return errors.mock.calls.map(([fields]) => fields as Record<string, unknown>);
}

describe("MaterializationDO — the check of the stored target list", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("logs one error for each target that is not valid, with the partition and the target", async () => {
		const errors = await errorsOfNewInstance([
			{ kind: "gsi", targetId: "ok", hashKey: { ref: "data", path: "$.a" } },
			{ kind: "gsi", targetId: "tooLarge", hashKey: aboveSqlLimit() },
			{ kind: "push_sink", targetId: "noBinding", binding: "", method: "push" },
		]);
		expect(errors).toEqual([
			expect.objectContaining({
				message: "fokos/mat: a stored target is not valid",
				actorId: expect.any(String),
				targetId: "tooLarge",
				kind: "gsi",
				error: expect.stringContaining("SQL limit"),
			}),
			expect.objectContaining({
				message: "fokos/mat: a stored target is not valid",
				actorId: expect.any(String),
				targetId: "noBinding",
				kind: "push_sink",
				attributes: { detail: expect.stringContaining("has no binding") },
			}),
		]);
	});

	it("logs one error when only the rules of the whole list fail", async () => {
		const push = { kind: "push_sink", targetId: "same", binding: "SINK", method: "push" } as const;
		expect(await errorsOfNewInstance([push, push])).toEqual([
			expect.objectContaining({
				message: "fokos/mat: the stored target list is not valid",
				actorId: expect.any(String),
				attributes: { detail: expect.stringContaining("not unique") },
			}),
		]);
	});

	it("logs nothing for a valid list", async () => {
		expect(await errorsOfNewInstance([{ kind: "gsi", targetId: "ok", hashKey: { ref: "data", path: "$.a" } }])).toEqual([]);
	});
});
