/**
 * `FokosShardingClient`, driven with recorded stub doubles.
 *
 * The client owns the send pipeline and the two traversals. The doubles here record every call, so
 * each test asserts the RPCs, the retries and the traversal order with no Durable Object.
 */
import { describe, expect, it } from "vitest";
import { FokosShardingClient, type FokosCallCost, type FokosRetryPolicy } from "./client.js";
import { attachRouting } from "./envelope.js";
import { SHARDING_UNAVAILABLE_CODES } from "./errors.js";
import { KeyCodec } from "./key-codec.js";
import type { FokosPromotionsPage, FokosStatusEntry, FokosStatusPage, RouteKey } from "./repartition-types.js";
import type { FokosPartitionRef, FokosRouteContext } from "./route-context.js";
import type { FokosEnvelope, FokosRouteNode, FokosRouting } from "./runtime-types.js";
import type { RepartitionKind, RepartitionState } from "./sharding-store.js";
import { FokosError, FokosUnavailableError } from "../shared/errors.js";

type EchoOps = { echo: { req: string; res: string } };

const ref = (doName: string): FokosPartitionRef => ({ partitionId: `00${doName}`, doName });
const rangeRef = (doName: string): FokosPartitionRef => ({ partitionId: `01${doName}`, doName });

const node = (doName: string): FokosRouteNode => ({
	ref: ref(doName),
	actorId: doName,
	hashDepth: 0,
	rangeDepth: 0,
	role: "executed",
	_rangeAncestors: [],
});

const routingOf = (forwardCount: number): FokosRouting => ({ servedBy: [node("leaf")], forwardCount, servedByTruncated: false });

const key = (hashKey: string): RouteKey => ({ hashKey: KeyCodec.encode(hashKey), sortKey: KeyCodec.encodeOptional(undefined) });

/** Fast retries, so the tests do not wait for the backoff. */
const retryAll = (maxAttempts: number): FokosRetryPolicy => ({
	shouldRetry: (_err, nextAttempt) => nextAttempt <= maxAttempts,
	baseDelayMs: 1,
	maxDelayMs: 2,
});

function makeClient(
	stub: (ctx: FokosRouteContext<unknown>, doName: string) => unknown,
	opts: { rootTreesN?: number; retry?: FokosRetryPolicy } = {},
) {
	return new FokosShardingClient<unknown, EchoOps>({
		topology: { shardGroup: `client_${crypto.randomUUID()}`, rootTreesN: opts.rootTreesN ?? 1, hashSplitN: 2 },
		rangeConfig: { rangeSplitN: 2, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } },
		policy: {},
		stub: (ctx, doName) => stub(ctx, doName) as DurableObjectStub,
		retry: opts.retry,
	});
}

/** A stub whose `echo` answers each attempt from `answers` in order: an envelope, or an error to throw. */
function scripted(answers: Array<FokosEnvelope<string> | Error>) {
	const calls: string[] = [];
	const stub = (_ctx: FokosRouteContext<unknown>, doName: string) => ({
		async echo(ctx: FokosRouteContext<unknown>, req: string): Promise<FokosEnvelope<string>> {
			calls.push(`${doName}:${ctx.doName}:${req}`);
			const answer = answers.shift();
			if (answer instanceof Error) {
				throw answer;
			}
			if (!answer) {
				throw new Error("no answer left");
			}
			return answer;
		},
	});
	return { stub, calls };
}

const migrating = (routing?: FokosRouting) => {
	const err = new FokosUnavailableError(SHARDING_UNAVAILABLE_CODES.partition_migrating, { message: "migrating" });
	return routing ? attachRouting(err, routing) : err;
};

describe("FokosShardingClient.send", () => {
	it("sends once to the root of the key, and removes the internal hints", async () => {
		const { stub, calls } = scripted([{ value: "ok", routing: routingOf(1) }]);
		const client = makeClient(stub);
		const root = client.resolve(key("k"));

		const result = await client.point("echo", key("k"), "req");

		expect(calls).toEqual([`${root.doName}:${root.doName}:req`]);
		expect(result).toEqual({
			value: "ok",
			routing: { servedBy: [{ ref: ref("leaf"), actorId: "leaf", hashDepth: 0, rangeDepth: 0, role: "executed" }], forwardCount: 1 },
			clientRpcs: 1,
			totalForwardCount: 1,
		});
	});

	it("does not retry without a policy", async () => {
		const { stub, calls } = scripted([migrating(), { value: "ok", routing: routingOf(0) }]);
		const client = makeClient(stub);

		await expect(client.point("echo", key("k"), "req")).rejects.toMatchObject({ code: "partition_migrating", clientRpcs: 1 });
		expect(calls).toHaveLength(1);
	});

	it("retries by the client policy, and a call policy replaces it", async () => {
		const client = (answers: Array<FokosEnvelope<string> | Error>) => {
			const s = scripted(answers);
			return { ...s, client: makeClient(s.stub, { retry: retryAll(5) }) };
		};

		const byClient = client([migrating(), migrating(), { value: "ok", routing: routingOf(0) }]);
		await expect(byClient.client.point("echo", key("k"), "req")).resolves.toMatchObject({ value: "ok", clientRpcs: 3 });

		const byCall = client([migrating(), { value: "ok", routing: routingOf(0) }]);
		await expect(byCall.client.point("echo", key("k"), "req", { retry: retryAll(1) })).rejects.toMatchObject({ clientRpcs: 1 });
		expect(byCall.calls).toHaveLength(1);
	});

	it("gives shouldRetry the error and nextAttempt 2 on the first retry", async () => {
		const seen: Array<[unknown, number]> = [];
		const err = migrating();
		const { stub } = scripted([err, err, { value: "ok", routing: routingOf(0) }]);
		const client = makeClient(stub);

		await client.point("echo", key("k"), "req", {
			retry: {
				shouldRetry: (e, nextAttempt) => {
					seen.push([e, nextAttempt]);
					return true;
				},
				baseDelayMs: 1,
				maxDelayMs: 2,
			},
		});

		expect(seen).toEqual([
			[err, 2],
			[err, 3],
		]);
	});

	it("counts a failed attempt at a root that serves locally", async () => {
		const { stub } = scripted([migrating(routingOf(0)), { value: "ok", routing: routingOf(0) }]);
		const client = makeClient(stub, { retry: retryAll(2) });

		const result = await client.point("echo", key("k"), "req");

		expect([result.clientRpcs, result.totalForwardCount, result.routing.forwardCount]).toEqual([2, 0, 0]);
	});

	it("adds the forwards of a failed attempt to the forwards of the attempt that succeeds", async () => {
		const { stub } = scripted([migrating(routingOf(1)), { value: "ok", routing: routingOf(2) }]);
		const client = makeClient(stub, { retry: retryAll(2) });

		const result = await client.point("echo", key("k"), "req");

		expect([result.clientRpcs, result.totalForwardCount, result.routing.forwardCount]).toEqual([2, 3, 2]);
	});

	it("puts the cost and the public routing on the error of a call that fails", async () => {
		const transport = new Error("Network connection lost.");
		const { stub } = scripted([migrating(routingOf(1)), transport]);
		const client = makeClient(stub, { retry: retryAll(2) });

		const err = (await client.point("echo", key("k"), "req").catch((e: unknown) => e)) as Error & FokosCallCost;

		expect(err).toBe(transport);
		expect([err.clientRpcs, err.totalForwardCount]).toEqual([2, 1]);

		const routed = (await makeClient(scripted([migrating(routingOf(1))]).stub)
			.point("echo", key("k"), "req")
			.catch((e: unknown) => e)) as FokosError & FokosCallCost & { routing: unknown };
		expect(FokosError.isCode(routed, SHARDING_UNAVAILABLE_CODES.partition_migrating)).toBe(true);
		expect(routed.routing).toEqual({
			servedBy: [{ ref: ref("leaf"), actorId: "leaf", hashDepth: 0, rangeDepth: 0, role: "executed" }],
			forwardCount: 1,
		});
		expect([routed.clientRpcs, routed.totalForwardCount]).toEqual([1, 1]);
	});
});

describe("FokosShardingClient.resolveAll", () => {
	it("groups the keys by root, keeps the input order, and maps each key back to its position", () => {
		const client = makeClient(() => ({}), { rootTreesN: 64 });
		const keys = Array.from({ length: 20 }, (_, i) => key(`k${i}`));

		const groups = client.resolveAll(keys);

		expect(groups.length).toBeGreaterThan(1);
		expect(groups.flatMap((g) => g.indexes).sort((a, b) => a - b)).toEqual(keys.map((_, i) => i));
		for (const { ctx, indexes } of groups) {
			expect(indexes).toEqual([...indexes].sort((a, b) => a - b));
			for (const i of indexes) {
				expect(client.resolve(keys[i])).toBe(ctx);
			}
		}
	});
});

type Partition = {
	role?: "owner" | "router";
	initialized?: boolean;
	targets?: Array<{ ref: FokosPartitionRef; state?: RepartitionState }>;
};

/**
 * A recorded cluster: every partition answers from `partitions`. A target whose ref is a range root is
 * a promotion, one per promotions page. Any other target is a split target in the status.
 */
function makeCluster(partitions: Record<string, Partition>) {
	/** Every call in order: `fence:<name>`, `status:<name>`, `page:<name>#<seq>`, `destroy:<name>`. */
	const events: string[] = [];
	const contexts: (string | undefined)[] = [];
	const statusContexts: (string | undefined)[] = [];
	const entryOf = (
		seq: number,
		index: number,
		target: { ref: FokosPartitionRef; state?: RepartitionState },
		kind: RepartitionKind,
	): FokosStatusEntry => ({
		repartition: { id: `r${seq}`, seq, kind, state: target.state ?? "completed", hashKey: null },
		target: { index, ref: target.ref, initialization: "initialized", acknowledged: true },
	});
	const isPromotion = (target: { ref: FokosPartitionRef }) => target.ref.partitionId.startsWith("01");
	const stub = (_ctx: FokosRouteContext<unknown>, doName: string) => ({
		async fokosPrepareDestroy(req: { rootContext?: FokosRouteContext<unknown> }) {
			events.push(`fence:${doName}`);
			contexts.push(req.rootContext?.doName);
		},
		async fokosStatus(req: { rootContext?: FokosRouteContext<unknown> }): Promise<FokosStatusPage> {
			const p = partitions[doName] ?? {};
			events.push(`status:${doName}`);
			statusContexts.push(req.rootContext?.doName);
			const initialized = p.initialized ?? true;
			return {
				initialized,
				destroying: false,
				ref: initialized ? ref(doName) : null,
				role: initialized ? (p.role ?? "owner") : null,
				importState: null,
				split: (p.targets ?? []).filter((t) => !isPromotion(t)).map((t, i) => entryOf(0, i, t, "hash_split")),
			};
		},
		async fokosPromotions(req: { cursor: { seq: number } | null }): Promise<FokosPromotionsPage> {
			const promotions = (partitions[doName]?.targets ?? []).filter(isPromotion);
			const seq = req.cursor ? req.cursor.seq + 1 : 0;
			events.push(`page:${doName}#${seq}`);
			const target = promotions[seq];
			return {
				entries: target ? [entryOf(seq, 0, target, "key_promotion")] : [],
				nextCursor: seq + 1 < promotions.length ? { seq } : null,
			};
		},
		async fokosDestroy() {
			events.push(`destroy:${doName}`);
		},
	});
	const of = (kind: string) => events.filter((e) => e.startsWith(`${kind}:`)).map((e) => e.slice(kind.length + 1));
	return { stub, events, contexts, statusContexts, fenced: () => of("fence"), destroyed: () => of("destroy") };
}

const child = (doName: string, state?: RepartitionState) => ({ ref: ref(doName), state });

describe("FokosShardingClient.destroy", () => {
	it("fences first, destroys every target before the partition that links it, and passes a root context only to a root", async () => {
		// One split child that split again, and one range root that the promotion of a key created.
		const { cluster, root } = clusterOf((r) => ({
			[r]: { targets: [child("child-a"), { ref: rangeRef("range-root") }] },
			"child-a": { targets: [child("grandchild")] },
		}));
		const destroyed: string[] = [];

		await cluster.client.destroy({ onDestroyed: (r) => destroyed.push(r.doName) });

		expect(cluster.destroyed()).toEqual(["grandchild", "child-a", "range-root", root]);
		expect(destroyed).toEqual(cluster.destroyed());
		expect(cluster.fenced()).toEqual([root, "child-a", "grandchild", "range-root"]);
		expect(cluster.contexts).toEqual([root, undefined, undefined, undefined]);
		// The fence of a partition comes before its status, and its pages before its destroy.
		expect(cluster.events.slice(0, 2)).toEqual([`fence:${root}`, `status:${root}`]);
		expect(cluster.events.at(-1)).toBe(`destroy:${root}`);
	});

	it("visits a range root shared by two hash children once", async () => {
		const shape = (root: string) => ({
			[root]: { targets: [child("child-a"), child("child-b")] },
			"child-a": { targets: [{ ref: rangeRef("range-root") }] },
			"child-b": { targets: [{ ref: rangeRef("range-root") }] },
		});
		const { cluster, root } = clusterOf(shape);

		await cluster.client.destroy();

		expect(cluster.destroyed()).toEqual(["range-root", "child-a", "child-b", root]);
		// The traversal skips the second link whole, so it reads the fence and the status pages once.
		expect(cluster.fenced()).toEqual([root, "child-a", "range-root", "child-b"]);
	});

	it("reads every promotions page and destroys each range root before the partition that promoted it", async () => {
		const { cluster, root } = clusterOf((r) => ({
			[r]: { targets: [child("child-a"), { ref: rangeRef("range-1") }, { ref: rangeRef("range-2") }, { ref: rangeRef("range-3") }] },
		}));

		await cluster.client.destroy();

		expect(cluster.destroyed()).toEqual(["child-a", "range-1", "range-2", "range-3", root]);
		expect(cluster.events.filter((e) => e.endsWith(`:${root}`) || e.startsWith(`page:${root}#`))).toEqual([
			`fence:${root}`,
			`status:${root}`,
			`page:${root}#0`,
			`page:${root}#1`,
			`page:${root}#2`,
			`destroy:${root}`,
		]);
	});

	it("destroys a partition with no targets as a leaf, for every root", async () => {
		const c = makeCluster({});
		const client = makeClient(c.stub, { rootTreesN: 3 });
		const roots = new Set<string>();
		for (let i = 0; roots.size < 3; i++) {
			roots.add(client.resolve(key(`k${i}`)).doName);
		}

		await client.destroy();

		expect(new Set(c.destroyed())).toEqual(roots);
		expect(c.destroyed()).toHaveLength(3);
	});

	it("gives a target a route context of the same shard group with the target's own identity", async () => {
		const seen: FokosRouteContext<unknown>[] = [];
		const { cluster, root } = clusterOf(
			(r) => ({ [r]: { targets: [child("child-a")] } }),
			(ctx) => seen.push(ctx),
		);

		await cluster.client.destroy();

		const rootCtx = cluster.client.resolve(key("k"));
		expect(seen.map((ctx) => ctx.doName)).toEqual([root, "child-a"]);
		expect(seen[1]).toEqual({ ...rootCtx, ...ref("child-a") });
	});
});

describe("FokosShardingClient.walk", () => {
	it("yields a parent before its children with no fence and no root context, and skips a partition without an identity", async () => {
		const { cluster, root } = clusterOf((r) => ({
			[r]: { role: "router", targets: [child("child-a"), child("child-b")] },
			"child-a": { targets: [{ ref: rangeRef("range-root") }] },
			"child-b": { initialized: false },
		}));

		const nodes = [];
		for await (const n of cluster.client.walk()) {
			nodes.push(n);
		}

		expect(nodes.map((n) => [n.ctx.doName, n.kind, n.role, n.parent?.ref.doName ?? null, n.parent?.via ?? null])).toEqual([
			[root, "hash", "router", null, null],
			["child-a", "hash", "owner", root, "hash_split"],
			["range-root", "range", "owner", "child-a", "key_promotion"],
		]);
		expect(cluster.fenced()).toEqual([]);
		expect(cluster.destroyed()).toEqual([]);
		expect(cluster.statusContexts.every((c) => c === undefined)).toBe(true);
	});

	it("yields no router and no target before its cutover with scope owners", async () => {
		const { cluster } = clusterOf((r) => ({
			[r]: { role: "router", targets: [child("child-a", "planned"), child("child-b", "cutover")] },
		}));

		const all: string[] = [];
		for await (const n of cluster.client.walk()) {
			all.push(n.ctx.doName);
		}
		const owners: string[] = [];
		for await (const n of cluster.client.walk({ scope: "owners" })) {
			owners.push(n.ctx.doName);
		}

		expect(all).toHaveLength(3);
		expect(owners).toEqual(["child-b"]);
	});

	it("stops reading when the caller stops early", async () => {
		const { cluster, root } = clusterOf((r) => ({ [r]: { targets: [child("child-a")] } }));

		for await (const n of cluster.client.walk()) {
			expect(n.ctx.doName).toBe(root);
			break;
		}

		expect(cluster.events).toEqual([`status:${root}`]);
	});
});

/** A client over a cluster whose shape names the root of the client. */
function clusterOf(shape: (root: string) => Record<string, Partition>, onStub?: (ctx: FokosRouteContext<unknown>) => void) {
	const probe = makeClient(() => ({}));
	const root = probe.resolve(key("k")).doName;
	const cluster = makeCluster(shape(root));
	const client = makeClientFor(probe, (ctx, doName) => {
		onStub?.(ctx);
		return cluster.stub(ctx, doName);
	});
	return { cluster: { ...cluster, client }, root };
}

/** A client of the same shard group as `like`, with another stub callback. */
function makeClientFor(like: FokosShardingClient<unknown, EchoOps>, stub: (ctx: FokosRouteContext<unknown>, doName: string) => unknown) {
	const ctx = like.resolve(key("k"));
	return new FokosShardingClient<unknown, EchoOps>({
		topology: ctx.topology,
		rangeConfig: ctx.rangeConfig,
		policy: ctx.policy,
		stub: (c, doName) => stub(c, doName) as DurableObjectStub,
	});
}
