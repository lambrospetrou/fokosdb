import { runInDurableObject } from "cloudflare:test";
import { describe, it } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import type { FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { FOKOS_KV_KEYS } from "../../src/sharding/sharding-store.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { kb, makeStub } from "./helpers.js";

/** The same partition, with other split thresholds and another policy version. */
function withPolicy(ctx: FokosDBRouteContext, policyVersion: number, maxSizeMb: number): FokosDBRouteContext {
	return { ...ctx, policyVersion, policy: { ...ctx.policy, hashSplitConditions: { maxSizeMb } } };
}

describe("PartitionDO - policy version", () => {
	it("stores a higher version, ignores a lower one, and lets the last writer win at an equal version", async ({ expect }) => {
		const { ctx, stub, rpc } = makeStub({ policyVersion: 1, hashSplitConditions: { maxSizeMb: 100 } });
		const put = async (c: FokosDBRouteContext) =>
			await rpc.apiPutItem(c, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });
		// Read the stored values WITHOUT a request, so the read does not write them.
		const stored = async () =>
			await runInDurableObject(stub, (instance: PartitionDO, state) => ({
				maxSizeMb: instance.fokos.policy().hashSplitConditions.maxSizeMb,
				policyVersion: instance.fokos.routeContext().policyVersion,
				kv: state.storage.kv.get<{ policyVersion: number }>(FOKOS_KV_KEYS.POLICY)?.policyVersion,
			}));

		await put(ctx);
		expect(await stored()).toEqual({ maxSizeMb: 100, policyVersion: 1, kv: 1 });

		// An older client during a deploy.
		await put(withPolicy(ctx, 0, 50));
		expect(await stored()).toEqual({ maxSizeMb: 100, policyVersion: 1, kv: 1 });

		await put(withPolicy(ctx, 1, 70));
		expect(await stored()).toEqual({ maxSizeMb: 70, policyVersion: 1, kv: 1 });

		await put(withPolicy(ctx, 2, 30));
		expect(await stored()).toEqual({ maxSizeMb: 30, policyVersion: 2, kv: 2 });
	});

	it("rejects a request with a policy version that is not a non-negative integer", async ({ expect }) => {
		const { ctx, stub, rpc } = makeStub();
		const req = { hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" as const };
		await rpc.apiPutItem(ctx, req);

		// runInDurableObject keeps the caught rejection inside the execution context of the DO, so it does
		// not leak as an unhandled rejection at the worker level.
		await runInDurableObject(stub, async (instance: PartitionDO) => {
			await expect(instance.apiPutItem(withPolicy(ctx, 1.5, 50), req)).rejects.toThrow(fokosErrorWith("partition_context_options_invalid"));
		});
	});
});
