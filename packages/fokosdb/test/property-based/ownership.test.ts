// Property-based tests for the ownership rule of a split plan.
//
// Two functions of the sharding runtime decide who owns a key after a split. The migration filters
// the rows of the source with `belongsToTarget`, and routing sends a request to `resolveOwner`. When
// the two do not agree, a target receives a row that the router never sends a request for, or the
// router sends a request to a target that did not receive the row.
//
// For random keys, each property checks on a settled router that exactly one target of its split
// plan accepts the key in `belongsToTarget`, and that this target is the one `resolveOwner` gives.
// The hash plan comes from the example host of the runtime, and the range plan from `PartitionDO`.
//
// The tree of each property is built once. The check only reads the router, so every run shares it.
import { runInDurableObject } from "cloudflare:test";
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import {
	FokosShardingStore,
	KeyCodec,
	RepartitionSource,
	type FokosOperationSpec,
	type FokosShardingRuntime,
	type RepartitionSourceDeps,
	type RouteKey,
} from "../../src/sharding/index.js";
import { kb } from "../partition-do/helpers.js";
import { makeTriggeredRangeRoot } from "../partition-do/partition-harness.js";
import type { CounterPartitionDO } from "../sharding/counter-host.js";
import { makeCounterTable, stub } from "../sharding/counter-table.js";
import { propertyRuns } from "./harness.js";

const NO_SORT_KEY = KeyCodec.asKeyBytes(new Uint8Array());

/** A key must not be empty. */
const arbKeyText = fc.string({ minLength: 1, maxLength: 32 });

const notUsed = (): never => {
	throw new Error("the ownership check reads only the stored plan and the identity");
};

/**
 * Runs the property on one router. Call it inside `runInDurableObject` of the router: the source half
 * reads the stored plan and targets from the storage of that partition.
 */
function assertOwnershipAgrees<TPolicy, Ops extends FokosOperationSpec>(
	fokos: FokosShardingRuntime<TPolicy, Ops>,
	storage: DurableObjectStorage,
	keys: fc.Arbitrary<RouteKey>,
): void {
	const deps: RepartitionSourceDeps = {
		identity: () => ({ ctx: fokos.routeContext(), identity: fokos.identity() }),
		getPeer: notUsed,
		hooks: { evaluateSplit: notUsed, migration: { buildPage: notUsed, applyPage: notUsed, validatePage: notUsed } },
		scheduleWork: notUsed,
		logParams: notUsed,
	};
	const source = new RepartitionSource(new FokosShardingStore(storage), deps);
	expect(fokos.lifecycle().role).toBe("router");
	const targets = source
		.splitTargets()
		.map((t) => ({ doName: t.doName, belongs: source.belongsToTarget(source.materializeSlice(t.slice)) }));
	expect(targets.length).toBeGreaterThan(1);

	fc.assert(
		fc.property(keys, (key) => {
			const owner = fokos.resolveOwner(key);
			expect(owner).toMatchObject({ kind: "remote", speculative: false });
			const accepted = targets.filter((t) => t.belongs(key)).map((t) => t.doName);
			expect(accepted).toEqual([owner.kind === "remote" ? owner.target.doName : null]);
		}),
		{ numRuns: propertyRuns(500) },
	);
}

describe("Property: belongsToTarget and resolveOwner agree", () => {
	it("routes and filters every hash key to the same child of a hash split", async () => {
		const table = makeCounterTable();
		for (let i = 0; i < 5; i++) {
			await table.increment(`k-${i}`);
		}
		await table.settle((n) => n[0].stats.role === "router");

		await runInDurableObject(stub(table.root.doName), (instance: CounterPartitionDO, state) => {
			const keys = arbKeyText.map((s) => ({ hashKey: KeyCodec.encode(s), sortKey: NO_SORT_KEY }));
			assertOwnershipAgrees(instance.fokos, state.storage, keys);
		});
	});

	it("routes and filters every sort key to the same child of a range split", async () => {
		const { root } = await makeTriggeredRangeRoot(4);
		await root.awaitSplitCompleted();

		await runInDurableObject(root.stub, (instance: PartitionDO, state: DurableObjectState) => {
			const keys = arbKeyText.map((sk) => ({ hashKey: kb("alice"), sortKey: kb(sk) }));
			assertOwnershipAgrees(instance.fokos, state.storage, keys);
		});
	});
});
