/**
 * `traverseForDestroy`, driven with fake callbacks.
 *
 * The router owns the traversal order and the dedup only. The caller fences each partition and pages
 * through its status. The callbacks here record the calls, so each test asserts the order of a real
 * destroy with no Durable Object.
 */
import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { PartitionContextCreator } from "./partition-context.js";
import { PartitionTopologyRouterImpl } from "./router.js";
import { KeyCodec } from "./key-codec.js";
import type { FokosPartitionRef } from "./repartition-types.js";

function makeRouter(rootTreesN: number, jurisdiction?: DurableObjectJurisdiction) {
	return new PartitionTopologyRouterImpl(
		PartitionContextCreator.create({
			ns: "PARTITION_DO",
			nsTx: "TRANSACTION_COORDINATOR_DO",
			tableName: `router.${crypto.randomUUID()}`,
			rootTreesN,
			hashSplitN: 2,
			rangeSplitN: 2,
			hashSplitConditions: { maxSizeMb: 100 },
			rangeSplitConditions: { maxSizeMb: 500 },
			...(jurisdiction === undefined ? {} : { jurisdiction }),
		}),
	);
}

const ref = (doName: string): FokosPartitionRef => ({ partitionId: `pid-${doName}`, doName });

describe("PartitionTopologyRouterImpl.traverseForDestroy", () => {
	it("destroys every target before the partition that links it, and passes a root context only to a root", async () => {
		const router = makeRouter(1);
		const root = router.rootPartitionContexts()[0];
		// One split child that split again, and one range root that the promotion of a key created.
		const links: Record<string, FokosPartitionRef[]> = {
			[root.doName]: [ref("child-a"), ref("range-root")],
			"child-a": [ref("grandchild")],
		};
		const contexts: (string | undefined)[] = [];
		const destroyed: string[] = [];

		await router.traverseForDestroy(
			async (partition, rootContext) => {
				contexts.push(rootContext?.doName);
				return links[partition.doName] ?? [];
			},
			async (partition) => void destroyed.push(partition.doName),
		);

		expect(destroyed).toEqual(["grandchild", "child-a", "range-root", root.doName]);
		expect(contexts).toEqual([root.doName, undefined, undefined, undefined]);
	});

	it("visits a range root shared by two hash children once", async () => {
		const router = makeRouter(1);
		const root = router.rootPartitionContexts()[0];
		// Both children inherited the same finished promotion, so both link the same range root.
		const links: Record<string, FokosPartitionRef[]> = {
			[root.doName]: [ref("child-a"), ref("child-b")],
			"child-a": [ref("range-root")],
			"child-b": [ref("range-root")],
		};
		const discovered: string[] = [];
		const destroyed: string[] = [];

		await router.traverseForDestroy(
			async (partition) => {
				discovered.push(partition.doName);
				return links[partition.doName] ?? [];
			},
			async (partition) => void destroyed.push(partition.doName),
		);

		expect(destroyed).toEqual(["range-root", "child-a", "child-b", root.doName]);
		// The traversal skips the second link whole, so it reads the fence and the status pages once.
		expect(discovered).toEqual([root.doName, "child-a", "range-root", "child-b"]);
	});

	it("destroys a partition with no targets as a leaf, for every root", async () => {
		const router = makeRouter(3);
		const roots = router.rootPartitionContexts().map((ctx) => ctx.doName);
		const destroyed: string[] = [];

		await router.traverseForDestroy(
			async () => [],
			async (partition) => void destroyed.push(partition.doName),
		);

		expect(destroyed).toEqual(roots);
	});
});

describe("PartitionTopologyRouterImpl.pickPartition", () => {
	it("returns the id that the plain namespace binding resolves for the partition name", () => {
		const router = makeRouter(1);
		const { doId, partitionContext } = router.pickPartition(KeyCodec.encode("hk"));

		expect(doId.toString()).toBe(env.PARTITION_DO.idFromName(partitionContext.doName).toString());
	});

	it("throws on a context that selects a jurisdiction, because workerd implements none", () => {
		// workerd throws "Jurisdiction restrictions are not implemented in workerd." synchronously on
		// every jurisdiction() call. The throw proves pickPartition resolves its namespace through the
		// accessor, so the no-jurisdiction path above never reaches jurisdiction().
		const router = makeRouter(1, "eu");

		expect(() => router.pickPartition(KeyCodec.encode("hk"))).toThrow("Jurisdiction restrictions are not implemented in workerd.");
	});
});
