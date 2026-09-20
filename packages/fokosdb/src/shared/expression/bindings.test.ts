import { describe, expect, it } from "vitest";
import { KeyCodec } from "../partition-topology/key-codec.js";
import { materializedPlanBindings, materializeExpressionBindings } from "./bindings.js";
import type { ExpressionBindingDescriptor } from "./plan.js";

describe("materializedPlanBindings", () => {
	const bindings: readonly ExpressionBindingDescriptor[] = [
		{ kind: "val", value: 7 },
		{ kind: "keyText", value: "order#10" },
	];

	it("returns the same array for the same plan and the same layout", () => {
		const plan = { bindings };
		const direct = materializedPlanBindings(plan);
		expect(direct).toEqual([7, KeyCodec.encode("order#10")]);
		expect(materializedPlanBindings(plan)).toBe(direct);
		expect(materializedPlanBindings(plan, "direct")).toBe(direct);
	});

	it("keeps the direct and the pool layouts apart", () => {
		const plan = { bindings };
		const pool = materializedPlanBindings(plan, "pool");
		expect(pool).toEqual(materializeExpressionBindings(bindings, "pool"));
		expect(pool).not.toBe(materializedPlanBindings(plan));
		expect(materializedPlanBindings(plan, "pool")).toBe(pool);
	});

	it("materializes again for a plan with its own descriptor array", () => {
		const a = materializedPlanBindings({ bindings: [...bindings] });
		const b = materializedPlanBindings({ bindings: [...bindings] });
		expect(a).toEqual(b);
		expect(a).not.toBe(b);
	});
});
