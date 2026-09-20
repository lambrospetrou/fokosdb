import { describe, expect, it } from "vitest";
import { PartitionContextCreator } from "./partition-context.js";
import { structurallyEqual } from "../sharding/route-context.js";

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
		const cfg = PartitionContextCreator.create(makeOpts());
		expect(cfg.rangeConfig.rangeAncestors).toEqual({ fromRoot: 0, fromLeaf: 3 });
	});

	it("keeps an explicit rangeAncestorsConfig", () => {
		const cfg = PartitionContextCreator.create(makeOpts({ rangeAncestorsConfig: { fromRoot: 1, fromLeaf: 3 } }));
		expect(cfg.rangeConfig.rangeAncestors).toEqual({ fromRoot: 1, fromLeaf: 3 });
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
		const cfg = PartitionContextCreator.create(makeOpts({ rangeAncestorsConfig }));
		expect(cfg.rangeConfig.rangeAncestors).toEqual(rangeAncestorsConfig);
	});
});

describe("PartitionContextCreator.create — the split of one table configuration", () => {
	it("puts the immutable fields in the topology and the FokosDB fields in the policy", () => {
		const cfg = PartitionContextCreator.create(makeOpts({ jurisdiction: "eu", locationHint: "weur" }));
		expect(cfg.topology).toEqual({ shardGroup: "testdb", rootTreesN: 1, hashSplitN: 4, jurisdiction: "eu" });
		expect(cfg.rangeConfig).toEqual({ rangeSplitN: 4, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } });
		expect(cfg.policy).toEqual({
			ns: "PARTITION_DO",
			nsTx: "TRANSACTION_COORDINATOR_DO",
			locationHint: "weur",
			hashSplitConditions: { maxSizeMb: 100 },
			rangeSplitConditions: { maxSizeMb: 500 },
		});
	});

	it("stores no jurisdiction and no location hint keys when neither is given", () => {
		const cfg = PartitionContextCreator.create(makeOpts());
		expect("jurisdiction" in cfg.topology).toBe(false);
		expect("locationHint" in cfg.policy).toBe(false);
	});
});

describe("structurallyEqual over the mutable parts", () => {
	it("treats equal range configs as equal and different ones as unequal", () => {
		const a = PartitionContextCreator.create(makeOpts({ rangeAncestorsConfig: { fromRoot: 2, fromLeaf: 2 } }));
		const b = PartitionContextCreator.create(makeOpts({ rangeAncestorsConfig: { fromRoot: 2, fromLeaf: 2 } }));
		const c = PartitionContextCreator.create(makeOpts({ rangeAncestorsConfig: { fromRoot: 1, fromLeaf: 2 } }));
		expect(structurallyEqual(a.rangeConfig, b.rangeConfig)).toBe(true);
		expect(structurallyEqual(a.rangeConfig, c.rangeConfig)).toBe(false);
	});

	it("compares the location hint inside the policy", () => {
		const weur = PartitionContextCreator.create(makeOpts({ locationHint: "weur" }));
		const weur2 = PartitionContextCreator.create(makeOpts({ locationHint: "weur" }));
		const eeur = PartitionContextCreator.create(makeOpts({ locationHint: "eeur" }));
		const none = PartitionContextCreator.create(makeOpts());
		expect(structurallyEqual(weur.policy, weur2.policy)).toBe(true);
		expect(structurallyEqual(weur.policy, eeur.policy)).toBe(false);
		expect(structurallyEqual(weur.policy, none.policy)).toBe(false);
	});

	it("treats an undefined key as absent", () => {
		expect(structurallyEqual({ a: 1, b: undefined }, { a: 1 })).toBe(true);
		expect(structurallyEqual({ a: 1 }, { a: 1, b: null })).toBe(false);
		expect(structurallyEqual([1, { x: [2] }], [1, { x: [2] }])).toBe(true);
		expect(structurallyEqual([1, 2], [2, 1])).toBe(false);
	});
});

describe("PartitionContextCreator.create option errors", () => {
	it.each([
		["rootTreesN", { rootTreesN: 0 }, 0],
		["hashSplitN", { hashSplitN: 1 }, 1],
		["shardGroup", { tableName: "fokos.mine" }, "fokos.mine"],
		["shardGroup", { tableName: "" }, ""],
		["hashSplitConditions.maxItems", { hashSplitConditions: { maxSizeMb: 100, maxItems: -1 } }, -1],
		["rangeAncestors.fromLeaf", { rangeAncestorsConfig: { fromRoot: 0, fromLeaf: 11 } }, 11],
	])("reports an invalid %s as partition_context_options_invalid", (option, overrides, value) => {
		expect(() => PartitionContextCreator.create(makeOpts(overrides))).toThrow(
			expect.objectContaining({
				_tag: "FokosValidationError",
				code: "partition_context_options_invalid",
				origin: "caller",
				attributes: { option, value },
			}),
		);
	});
});
