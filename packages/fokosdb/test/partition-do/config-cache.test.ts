import { runInDurableObject } from "cloudflare:test";
import { describe, it } from "vitest";
import type { FokosDBPartitionDO } from "../../src/server/do-partition.js";
import type { PartitionDOConfig, PartitionDOConfigOverrides } from "../../src/server/host-config.js";
import type { FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { kb, makeStub } from "./helpers.js";

/** The private settings hooks of one instance. The override goes on the instance, never on the prototype. */
type ConfigHooks = { fokosConfig: () => PartitionDOConfigOverrides; config: () => PartitionDOConfig };

describe("FokosDBPartitionDO - fokosConfig", () => {
	it("resolves the settings again after the partition stores its identity, and after a new policy", async ({ expect }) => {
		const { ctx, stub, rpc } = makeStub({ policyVersion: 1, hashSplitConditions: { maxSizeMb: 100 } });
		const put = async (c: FokosDBRouteContext) =>
			await rpc.apiPutItem(c, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });

		// The override reads the policy only after the initialization, as the contract of `fokosConfig` tells.
		const maxClockSkewMs = async () =>
			await runInDurableObject(stub, (instance: FokosDBPartitionDO) => {
				const hooks = instance as unknown as ConfigHooks;
				hooks.fokosConfig = () =>
					instance.fokos?.initialized() ? { maxClockSkewMs: instance.fokos.policy().hashSplitConditions.maxSizeMb } : { maxClockSkewMs: 1 };
				return hooks.config().maxClockSkewMs;
			});

		expect(await maxClockSkewMs()).toBe(1);

		// The first request stores the identity of the root.
		await put(ctx);
		expect(await maxClockSkewMs()).toBe(100);

		await put({ ...ctx, policyVersion: 2, policy: { ...ctx.policy, hashSplitConditions: { maxSizeMb: 30 } } });
		expect(await maxClockSkewMs()).toBe(30);
	});
});
