import { describe, expect, it } from "vitest";
import { coordinatorShardGroup, createTableConfig, type FokosTableIdentity, type FokosTableOptions } from "./partition-context.js";
import { structurallyEqual } from "../sharding/route-context.js";
import { FokosRouter } from "../sharding/router.js";
import { KeyCodec } from "../sharding/key-codec.js";

type FlatOptions = Partial<Omit<FokosTableIdentity, "name"> & Omit<FokosTableOptions, "table"> & { tableName: string }>;

function makeOpts(overrides?: FlatOptions): FokosTableOptions {
	const { tableName, ns, nsTx, rootTreesN, hashSplitN, jurisdiction, ...rest } = overrides ?? {};
	return {
		table: {
			name: tableName ?? "testdb",
			ns: ns ?? "PARTITION_DO",
			nsTx: nsTx ?? "TRANSACTION_COORDINATOR_DO",
			rootTreesN: rootTreesN ?? 1,
			hashSplitN: hashSplitN ?? 4,
			...(jurisdiction === undefined ? {} : { jurisdiction }),
		},
		hashSplitConditions: { maxSizeMb: 100 },
		...rest,
	};
}

describe("createTableConfig — rangeAncestorsConfig", () => {
	it("defaults to { fromRoot: 0, fromLeaf: 3 } when omitted", () => {
		const cfg = createTableConfig(makeOpts());
		expect(cfg.rangeConfig.rangeAncestors).toEqual({ fromRoot: 0, fromLeaf: 3 });
	});

	it("keeps an explicit rangeAncestorsConfig", () => {
		const cfg = createTableConfig(makeOpts({ rangeAncestorsConfig: { fromRoot: 1, fromLeaf: 3 } }));
		expect(cfg.rangeConfig.rangeAncestors).toEqual({ fromRoot: 1, fromLeaf: 3 });
	});

	it.each([
		{ fromRoot: -1, fromLeaf: 2 },
		{ fromRoot: 11, fromLeaf: 2 },
		{ fromRoot: 2, fromLeaf: -1 },
		{ fromRoot: 2, fromLeaf: 11 },
	])("rejects out-of-bounds config %j", (rangeAncestorsConfig) => {
		expect(() => createTableConfig(makeOpts({ rangeAncestorsConfig }))).toThrow();
	});

	it.each([
		{ fromRoot: 0, fromLeaf: 0 },
		{ fromRoot: 10, fromLeaf: 10 },
	])("accepts boundary values %j", (rangeAncestorsConfig) => {
		const cfg = createTableConfig(makeOpts({ rangeAncestorsConfig }));
		expect(cfg.rangeConfig.rangeAncestors).toEqual(rangeAncestorsConfig);
	});
});

describe("createTableConfig — the split of one table configuration", () => {
	it("puts the immutable fields in the topology and the FokosDB fields in the policy", () => {
		const cfg = createTableConfig(makeOpts({ jurisdiction: "eu", locationHint: "weur" }));
		expect(cfg.topology).toEqual({ shardGroup: "fokos.p.testdb", rootTreesN: 1, hashSplitN: 4, jurisdiction: "eu" });
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
		const cfg = createTableConfig(makeOpts());
		expect("jurisdiction" in cfg.topology).toBe(false);
		expect("locationHint" in cfg.policy).toBe(false);
	});
});

describe("createTableConfig — the shard groups of a table", () => {
	it("names the partitions fokos.p.<tableName> and the coordinators fokos.tc.<tableName>", () => {
		const cfg = createTableConfig(makeOpts({ tableName: "orders" }));
		expect(cfg.topology.shardGroup).toBe("fokos.p.orders");
		expect(coordinatorShardGroup(cfg.topology)).toBe("fokos.tc.orders");
		const hk = KeyCodec.encode("hk");
		const partitions = new FokosRouter(cfg.topology, cfg.rangeConfig, cfg.policy);
		expect(partitions.rootContext(hk).doName).toMatch(/^fokos\.p\.orders~h\.\d+$/);
		const coordinators = new FokosRouter({ ...cfg.topology, shardGroup: coordinatorShardGroup(cfg.topology) }, cfg.rangeConfig, cfg.policy);
		expect(coordinators.rootContext(hk).doName).toMatch(/^fokos\.tc\.orders~h\.\d+$/);
	});
});

describe("createTableConfig — defaults", () => {
	it("keeps a given rangeSplitN when rangeSplitConditions is omitted", () => {
		const cfg = createTableConfig(makeOpts({ rangeSplitN: 8 }));
		expect(cfg.rangeConfig.rangeSplitN).toBe(8);
		expect(cfg.policy.rangeSplitConditions).toEqual({ maxSizeMb: 500 });
	});

	it("defaults rangeSplitN when only rangeSplitConditions is given", () => {
		const cfg = createTableConfig(makeOpts({ rangeSplitConditions: { maxSizeMb: 200 } }));
		expect(cfg.rangeConfig.rangeSplitN).toBe(4);
		expect(cfg.policy.rangeSplitConditions).toEqual({ maxSizeMb: 200 });
	});

	it("does not change the options object of the caller", () => {
		const opts = makeOpts({ hashSplitN: 8 });
		const before = structuredClone(opts);
		createTableConfig(opts);
		expect(opts).toEqual(before);
	});
});

describe("structurallyEqual over the mutable parts", () => {
	it("treats equal range configs as equal and different ones as unequal", () => {
		const a = createTableConfig(makeOpts({ rangeAncestorsConfig: { fromRoot: 2, fromLeaf: 2 } }));
		const b = createTableConfig(makeOpts({ rangeAncestorsConfig: { fromRoot: 2, fromLeaf: 2 } }));
		const c = createTableConfig(makeOpts({ rangeAncestorsConfig: { fromRoot: 1, fromLeaf: 2 } }));
		expect(structurallyEqual(a.rangeConfig, b.rangeConfig)).toBe(true);
		expect(structurallyEqual(a.rangeConfig, c.rangeConfig)).toBe(false);
	});

	it("compares the location hint inside the policy", () => {
		const weur = createTableConfig(makeOpts({ locationHint: "weur" }));
		const weur2 = createTableConfig(makeOpts({ locationHint: "weur" }));
		const eeur = createTableConfig(makeOpts({ locationHint: "eeur" }));
		const none = createTableConfig(makeOpts());
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

describe("createTableConfig option errors", () => {
	it.each([
		["rootTreesN", { rootTreesN: 0 }, 0],
		["hashSplitN", { hashSplitN: 1 }, 1],
		["table.name", { tableName: "fokos.mine" }, "fokos.mine"],
		["table.name", { tableName: "" }, ""],
		["shardGroup", { tableName: "a~b" }, "fokos.p.a~b"],
		["hashSplitConditions.maxSizeMb", { hashSplitConditions: { maxSizeMb: -1 } }, -1],
		["rangeAncestors.fromLeaf", { rangeAncestorsConfig: { fromRoot: 0, fromLeaf: 11 } }, 11],
	])("reports an invalid %s as partition_context_options_invalid", (option, overrides, value) => {
		expect(() => createTableConfig(makeOpts(overrides))).toThrow(
			expect.objectContaining({
				_tag: "FokosValidationError",
				code: "partition_context_options_invalid",
				origin: "caller",
				attributes: { option, value },
			}),
		);
	});
});

describe("createTableConfig — limits", () => {
	it("omits limits from the policy when the table overrides nothing", () => {
		expect(createTableConfig(makeOpts()).policy).not.toHaveProperty("limits");
		expect(createTableConfig(makeOpts({ limits: {} })).policy).not.toHaveProperty("limits");
		expect(createTableConfig(makeOpts({ limits: { maxHashKeyBytes: undefined } })).policy).not.toHaveProperty("limits");
	});

	it("keeps only the known overrides, and keeps an override that is equal to the default", () => {
		const limits = { maxHashKeyBytes: 1_024, maxFutureBytes: 7 } as FokosTableOptions["limits"];
		expect(createTableConfig(makeOpts({ limits })).policy.limits).toEqual({ maxHashKeyBytes: 1_024 });
	});

	it("refuses a limit that is not valid", () => {
		expect(() => createTableConfig(makeOpts({ limits: { maxSortKeyBytes: 0 } }))).toThrow(
			expect.objectContaining({ code: "partition_context_options_invalid", attributes: { option: "limits.maxSortKeyBytes", value: 0 } }),
		);
	});
});
