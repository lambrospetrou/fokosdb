/**
 * `FokosRouter.walk`, driven with recorded stub doubles.
 *
 * The router owns the traversal: the fence, the status pages, the target order and the dedup. The
 * doubles here record every call, so each test asserts the order of a real destroy with no Durable
 * Object.
 */
import { describe, expect, it } from "vitest";
import { PartitionContextCreator } from "../shared/partition-context.js";
import { FokosRouter, type FokosWalkStub } from "./router.js";
import { KeyCodec } from "./key-codec.js";
import type { FokosPartitionRef, FokosRouteContext } from "./route-context.js";
import type { FokosStatusEntry, FokosStatusPage } from "./repartition-types.js";

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

const ref = (doName: string): FokosPartitionRef => ({ partitionId: `pid-${doName}`, doName });

/** A recorded cluster: every partition answers its status from `links`, one target per page. */
function makeCluster(links: Record<string, FokosPartitionRef[]>) {
	/** Every call in order: `fence:<name>`, `page:<name>#<seq>`, `destroy:<name>`. */
	const events: string[] = [];
	const contexts: (string | undefined)[] = [];
	const stub = (_ctx: FokosRouteContext<unknown>, doName: string) =>
		({
			async fokosPrepareDestroy(req) {
				events.push(`fence:${doName}`);
				contexts.push(req.rootContext?.doName);
			},
			async fokosStatus(req): Promise<FokosStatusPage> {
				const targets = links[doName] ?? [];
				const seq = (req.cursor?.seq ?? 0) as number;
				events.push(`page:${doName}#${seq}`);
				const entry: FokosStatusEntry | undefined = targets[seq]
					? {
							repartition: { id: `r${seq}`, seq, kind: "hash_split", state: "completed", hashKey: null },
							target: { index: 0, ref: targets[seq], initialization: "initialized", acknowledged: true },
						}
					: undefined;
				return {
					initialized: true,
					destroying: true,
					ref: null,
					importState: null,
					entries: entry ? [entry] : [],
					nextCursor: seq + 1 < targets.length ? { seq: seq + 1, targetIndex: 0 } : null,
				};
			},
			async destroy() {
				events.push(`destroy:${doName}`);
			},
		}) satisfies FokosWalkStub & { destroy(): Promise<void> };
	const of = (kind: string) => events.filter((e) => e.startsWith(`${kind}:`)).map((e) => e.slice(kind.length + 1));
	return { stub, events, contexts, fenced: () => of("fence"), destroyed: () => of("destroy") };
}

describe("FokosRouter.walk", () => {
	it("fences first, destroys every target before the partition that links it, and passes a root context only to a root", async () => {
		const router = makeRouter(1);
		const root = router.allRoots()[0];
		// One split child that split again, and one range root that the promotion of a key created.
		const c = makeCluster({
			[root.doName]: [ref("child-a"), ref("range-root")],
			"child-a": [ref("grandchild")],
		});

		await router.walk(c.stub, async (_ctx, stub) => await stub.destroy());

		expect(c.destroyed()).toEqual(["grandchild", "child-a", "range-root", root.doName]);
		expect(c.fenced()).toEqual([root.doName, "child-a", "grandchild", "range-root"]);
		expect(c.contexts).toEqual([root.doName, undefined, undefined, undefined]);
		// The fence of a partition comes before its first page, and its pages before its destroy.
		expect(c.events.slice(0, 2)).toEqual([`fence:${root.doName}`, `page:${root.doName}#0`]);
		expect(c.events.at(-1)).toBe(`destroy:${root.doName}`);
	});

	it("visits a range root shared by two hash children once", async () => {
		const router = makeRouter(1);
		const root = router.allRoots()[0];
		// Both children inherited the same finished promotion, so both link the same range root.
		const c = makeCluster({
			[root.doName]: [ref("child-a"), ref("child-b")],
			"child-a": [ref("range-root")],
			"child-b": [ref("range-root")],
		});

		await router.walk(c.stub, async (_ctx, stub) => await stub.destroy());

		expect(c.destroyed()).toEqual(["range-root", "child-a", "child-b", root.doName]);
		// The traversal skips the second link whole, so it reads the fence and the status pages once.
		expect(c.fenced()).toEqual([root.doName, "child-a", "range-root", "child-b"]);
	});

	it("destroys a partition with no targets as a leaf, for every root", async () => {
		const router = makeRouter(3);
		const roots = router.allRoots().map((ctx) => ctx.doName);
		const c = makeCluster({});

		await router.walk(c.stub, async (_ctx, stub) => await stub.destroy());

		expect(c.destroyed()).toEqual(roots);
	});

	it("gives a target a route context of the same shard group with the target's own identity", async () => {
		const router = makeRouter(1);
		const root = router.allRoots()[0];
		const c = makeCluster({ [root.doName]: [ref("child-a")] });
		const seen: FokosRouteContext<unknown>[] = [];

		await router.walk(
			(ctx, doName) => {
				seen.push(ctx);
				return c.stub(ctx, doName);
			},
			async () => {},
		);

		expect(seen.map((ctx) => ctx.doName)).toEqual([root.doName, "child-a"]);
		expect(seen[1]).toEqual({ ...root, ...ref("child-a") });
	});
});

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
