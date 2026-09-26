import { describe, expect, it } from "vitest";
import { PartitionContextCreator } from "../shared/partition-context.js";
import { FokosRouter } from "./router.js";
import { KeyCodec } from "./key-codec.js";

function makeRouter(rootTreesN: number, jurisdiction?: DurableObjectJurisdiction) {
	const cfg = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: `router.${crypto.randomUUID()}`,
		rootTreesN,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
		...(jurisdiction === undefined ? {} : { jurisdiction }),
	});
	return new FokosRouter(cfg.topology, cfg.rangeConfig, cfg.policy);
}

describe("FokosRouter.rootContext", () => {
	it("routes one hash key to one root and every root context carries the whole configuration", () => {
		const router = makeRouter(3, "eu");
		const ctx = router.rootContext(KeyCodec.encode("hk"));

		expect(router.allRoots()).toContain(ctx);
		expect(ctx.schema).toBe(2);
		expect(ctx.topology).toBe(router.topology);
		expect(ctx.rangeConfig).toBe(router.rangeConfig);
		expect(ctx.policy).toBe(router.policy);
		expect(ctx.doName).toBe(`${router.topology.shardGroup}.h.${ctx.doName.split(".h.")[1]}`);
		expect(new Set(router.allRoots().map((r) => r.doName)).size).toBe(3);
	});

	it("rejects an invalid topology at construction", () => {
		expect(
			() =>
				new FokosRouter(
					{ shardGroup: "", rootTreesN: 1, hashSplitN: 2 },
					{ rangeSplitN: 2, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } },
					{},
				),
		).toThrow(expect.objectContaining({ code: "partition_context_options_invalid" }));
	});
});
