import { describe, expect, it } from "vitest";
import { areMutableOptionsEqual, PartitionContextCreator } from "./partition-context.js";
import type { PartitionContext } from "./partition-context.js";

function makeOpts(overrides?: Partial<Parameters<typeof PartitionContextCreator.create>[0]>) {
	return {
		ns: "PARTITION_DO" as const,
		nsTx: "TRANSACTION_COORDINATOR_DO" as const,
		tableName: "testdb",
		rootTreesN: 1,
		hashSplitN: 4,
		hashSplitConditions: { maxSizeMb: 100 },
		...overrides,
	};
}

describe("PartitionContextCreator.create — rangeAncestorsConfig", () => {
	it("defaults to { fromRoot: 0, fromLeaf: 3 } when omitted", () => {
		const ctx = PartitionContextCreator.create(makeOpts());
		expect(ctx.rangeAncestorsConfig).toEqual({ fromRoot: 0, fromLeaf: 3 });
	});

	it("keeps an explicit rangeAncestorsConfig", () => {
		const ctx = PartitionContextCreator.create(makeOpts({ rangeAncestorsConfig: { fromRoot: 1, fromLeaf: 3 } }));
		expect(ctx.rangeAncestorsConfig).toEqual({ fromRoot: 1, fromLeaf: 3 });
	});

	it.each([
		{ fromRoot: -1, fromLeaf: 2 },
		{ fromRoot: 11, fromLeaf: 2 },
		{ fromRoot: 2, fromLeaf: -1 },
		{ fromRoot: 2, fromLeaf: 11 },
	])("rejects out-of-bounds config %j", (rangeAncestorsConfig) => {
		expect(() => PartitionContextCreator.create(makeOpts({ rangeAncestorsConfig }))).toThrow();
	});

	it.each([
		{ fromRoot: 0, fromLeaf: 0 },
		{ fromRoot: 10, fromLeaf: 10 },
	])("accepts boundary values %j", (rangeAncestorsConfig) => {
		const ctx = PartitionContextCreator.create(makeOpts({ rangeAncestorsConfig }));
		expect(ctx.rangeAncestorsConfig).toEqual(rangeAncestorsConfig);
	});
});

describe("areMutableOptionsEqual — rangeAncestorsConfig", () => {
	function withConfig(config: { fromRoot: number; fromLeaf: number } | undefined): PartitionContext {
		return PartitionContextCreator.create(makeOpts({ rangeAncestorsConfig: config }));
	}

	it("treats equal configs as equal", () => {
		expect(areMutableOptionsEqual(withConfig({ fromRoot: 2, fromLeaf: 2 }), withConfig({ fromRoot: 2, fromLeaf: 2 }))).toBe(true);
	});

	it("treats different configs as unequal", () => {
		expect(areMutableOptionsEqual(withConfig({ fromRoot: 2, fromLeaf: 2 }), withConfig({ fromRoot: 1, fromLeaf: 2 }))).toBe(false);
		expect(areMutableOptionsEqual(withConfig({ fromRoot: 2, fromLeaf: 2 }), withConfig({ fromRoot: 2, fromLeaf: 3 }))).toBe(false);
	});
});
