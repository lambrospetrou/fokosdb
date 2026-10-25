import { describe, expect, it, vi } from "vitest";
import { FokosRoutingError } from "../shared/errors.js";
import { FokosShardingClient } from "./client.js";
import { SHARDING_ROUTING_CODES } from "./errors.js";
import { hashChildIndex, hashRootIndex } from "./hash-primitives.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import { PartitionIdHelper, resolveRangePartitionContext } from "./partition-id.js";
import type { FokosRouteContext } from "./route-context.js";
import type { FokosEnvelope } from "./runtime-types.js";
import {
	cachedHashTopology,
	cutTopologyHint,
	TOPOLOGY_CUT_MAX,
	topologyGetForRangePath,
	topologyGetPath,
	type FokosTopologyCacheOptions,
} from "./topology-cache.js";
import { findLeaf } from "./topology-codec.js";
import { encodeHashBitmap, encodeHashBody, encodeRangeBody, hashNodeKey, promotedKeysBloom } from "./topology-encode.js";

type Ops = { op: { req: { n: number }; res: string } };

const NO_SORT_KEY = KeyCodec.encodeOptional(undefined);

function hashBody(opts: { version: number; R?: number; K?: number; splits?: Uint8Array[]; promoted?: KeyBytes[] }): Uint8Array {
	const R = opts.R ?? 2;
	const K = opts.K ?? 2;
	const promoted = opts.promoted ?? [];
	// Each test splits roots only, so the order of `splits` is the byte order of the node keys.
	const nodes = (opts.splits ?? []).map(hashNodeKey);
	return encodeHashBody({
		epoch: 1,
		version: opts.version,
		rootTreesN: R,
		hashSplitN: K,
		bitmap: encodeHashBitmap(R, K, nodes.length, () => nodes),
		bloom: promotedKeysBloom(promoted.length, () => promoted),
	});
}

/**
 * A client of a new shard group with R 2 and K 2. `bodies` gives the answer of each path, and
 * `failAt` the DO names that answer that they have no identity.
 */
function setup(
	bodies: Map<string, () => Uint8Array>,
	failAt: (doName: string) => FokosRoutingError | null = () => null,
	overrides: Partial<FokosTopologyCacheOptions> = {},
) {
	const shardGroup = `tc_${crypto.randomUUID()}`;
	const pending: Promise<unknown>[] = [];
	const fetched: string[] = [];
	const sent: string[] = [];
	const cache: FokosTopologyCacheOptions = {
		fetch: async (path) => {
			fetched.push(path);
			const body = bodies.get(path.replace(shardGroupB64(shardGroup), shardGroupB64("SG")));
			return body ? new Response(body()) : new Response(null, { status: 503 });
		},
		waitUntil: (p) => pending.push(p),
		refreshMs: -1,
		...overrides,
	};
	const client = new FokosShardingClient<null, Ops>({
		topology: { shardGroup, rootTreesN: 2, hashSplitN: 2 },
		rangeConfig: { rangeSplitN: 2, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } },
		policy: null,
		topologyCache: cache,
		stub: (_ctx, doName) =>
			({
				op: async (ctx: FokosRouteContext<null>): Promise<FokosEnvelope<string>> => {
					sent.push(doName);
					const error = failAt(doName);
					if (error) {
						throw error;
					}
					return { value: ctx.doName, routing: { servedBy: [], forwardCount: 0, servedByTruncated: false } };
				},
			}) as unknown as DurableObjectStub,
	});
	const settle = async () => {
		while (pending.length > 0) {
			await pending.shift();
		}
	};
	return { shardGroup, client, cache, pending, fetched, sent, settle };
}

function shardGroupB64(shardGroup: string): string {
	return new TextEncoder().encode(shardGroup).toBase64({ alphabet: "base64url", omitPadding: true });
}

/** The path key of the hash child of `hashKey` below the root, with R 2 and K 2. */
function childOf(hashKey: KeyBytes): { root: number; child: number } {
	return { root: hashRootIndex(hashKey, 2), child: hashChildIndex(hashKey, 0, 2) };
}

const GET = topologyGetPath("SG");

/** The partition ID bytes of the hash root `root`. */
function rootId(root: number): Uint8Array {
	return PartitionIdHelper.fromHashIdxs("sg", [root]).encode(false).bytes;
}

describe("topology cache — hash entries", () => {
	it("starts one fetch through waitUntil, keeps no promise, and enters at the leaf after the fetch", async () => {
		const key = { hashKey: KeyCodec.encode("alpha"), sortKey: NO_SORT_KEY };
		const { root, child } = childOf(key.hashKey);
		const t = setup(new Map([[GET, () => hashBody({ version: 1, splits: [rootId(root)] })]]));
		const first = t.client.resolve(key);
		expect(first.doName).toBe(PartitionIdHelper.hashId(t.shardGroup, root).doName);
		// The refresh is in flight, so a second read starts no fetch.
		t.client.resolve(key);
		expect(t.pending).toHaveLength(1);
		await t.settle();
		expect(t.client.resolve(key).doName).toBe(PartitionIdHelper.hashId(t.shardGroup, root, [child]).doName);
		// The entry is old at once (refreshMs -1), so each read starts a new fetch.
		t.client.resolve(key);
		expect(t.pending).toHaveLength(1);
		await t.settle();
		expect(t.fetched).toHaveLength(2);
	});

	it("stops waiting for a fetch that does not end, aborts its signal, and can refresh again", async () => {
		vi.spyOn(console, "warn").mockImplementation(() => {});
		const key = { hashKey: KeyCodec.encode("hung"), sortKey: NO_SORT_KEY };
		const signals: AbortSignal[] = [];
		// The fetch ignores the signal and never settles, so only the timeout of the refresh can end it.
		const t = setup(new Map(), undefined, {
			fetchTimeoutMs: 50,
			fetch: (_path, signal) => {
				signals.push(signal);
				return new Promise<Response>(() => {});
			},
		});
		t.client.resolve(key);
		await t.settle();
		expect(signals[0].aborted).toBe(true);
		t.client.resolve(key);
		expect(t.pending).toHaveLength(1);
		await t.settle();
		expect(signals).toHaveLength(2);
	});

	it("refuses an older version", async () => {
		const key = { hashKey: KeyCodec.encode("beta"), sortKey: NO_SORT_KEY };
		const { root, child } = childOf(key.hashKey);
		let version = 5;
		const t = setup(new Map([[GET, () => hashBody({ version, splits: version === 5 ? [rootId(root)] : [] })]]));
		t.client.resolve(key);
		await t.settle();
		version = 3;
		t.client.resolve(key);
		await t.settle();
		expect(t.client.resolve(key).doName).toBe(PartitionIdHelper.hashId(t.shardGroup, root, [child]).doName);
	});

	it("drops a body of another hashSplitN, and a body larger than the budget", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		const key = { hashKey: KeyCodec.encode("gamma"), sortKey: NO_SORT_KEY };
		const { root } = childOf(key.hashKey);
		const rootName = PartitionIdHelper.hashId("", root).doName;
		for (const body of [
			() => hashBody({ version: 1, K: 3, splits: [rootId(root)] }),
			// 17 MiB of bitmap, with the number of splits that this bit count needs.
			() => {
				const bits = new Uint8Array(17 * 1024 * 1024);
				const splits = (bits.length * 8 - 2) / 2;
				bits.fill(0xff, 0, splits >> 3);
				bits[splits >> 3] = (1 << (splits & 7)) - 1;
				return encodeHashBody({
					epoch: 1,
					version: 1,
					rootTreesN: 2,
					hashSplitN: 2,
					bitmap: { bits, bitCount: bits.length * 8 },
					bloom: null,
				});
			},
		]) {
			const t = setup(new Map([[GET, body]]));
			t.client.resolve(key);
			await t.settle();
			expect(t.client.resolve(key).doName).toBe(t.shardGroup + rootName);
		}
		const errors = warn.mock.calls.map(([entry]) => String((entry as { error: string }).error));
		expect(errors.some((e) => e.includes("hashSplitN 3"))).toBe(true);
		expect(errors.some((e) => e.includes("larger than the cache budget"))).toBe(true);
	});

	it("after a miss at a topology entry, sends again to the parent and cuts the hint", async () => {
		const key = { hashKey: KeyCodec.encode("delta"), sortKey: NO_SORT_KEY };
		const { root, child } = childOf(key.hashKey);
		const deep = (sg: string) => PartitionIdHelper.hashId(sg, root, [child]).doName;
		let shardGroup = "";
		const t = setup(new Map([[GET, () => hashBody({ version: 1, splits: [rootId(root)] })]]), (doName) =>
			doName === deep(shardGroup)
				? new FokosRoutingError(SHARDING_ROUTING_CODES.hash_partition_not_initialized, { message: "no identity", attributes: { doName } })
				: null,
		);
		shardGroup = t.shardGroup;
		t.client.resolve(key);
		await t.settle();
		const result = await t.client.point("op", key, { n: 1 });
		expect(result.value).toBe(PartitionIdHelper.hashId(t.shardGroup, root).doName);
		expect(result.clientRpcs).toBe(2);
		expect(t.sent).toEqual([deep(t.shardGroup), PartitionIdHelper.hashId(t.shardGroup, root).doName]);
		// The cut stays for this version: the next request goes to the parent first.
		expect(t.client.resolve(key).doName).toBe(PartitionIdHelper.hashId(t.shardGroup, root).doName);
	});
});

describe("topology cache — bounded fallback", () => {
	const key = { hashKey: KeyCodec.encode("stale"), sortKey: NO_SORT_KEY };
	const { root } = childOf(key.hashKey);
	const missing = (doName: string) =>
		new FokosRoutingError(SHARDING_ROUTING_CODES.hash_partition_not_initialized, {
			message: "no identity",
			attributes: { doName },
		});
	const chain = (depth: number) => {
		const path = Array.from({ length: depth }, (_, d) => hashChildIndex(key.hashKey, d, 2));
		return Array.from({ length: depth }, (_, d) => path.slice(0, d));
	};
	const setupTree = (
		paths: number[][],
		failAt = (doName: string): FokosRoutingError | null => (doName.includes(`~h.${root}.`) ? missing(doName) : null),
	) => {
		const nodes = paths.map((path) => hashNodeKey(PartitionIdHelper.fromHashIdxs("sg", [root, ...path]).encode(false).bytes));
		const bytes = encodeHashBody({
			epoch: 1,
			version: 1,
			rootTreesN: 2,
			hashSplitN: 2,
			bitmap: encodeHashBitmap(2, 2, nodes.length, () => nodes),
			bloom: promotedKeysBloom(0, () => []),
		});
		const t = setup(new Map([[GET, () => bytes]]), failAt, { refreshMs: Number.MAX_SAFE_INTEGER });
		return { ...t, rootName: PartitionIdHelper.hashId(t.shardGroup, root).doName };
	};

	it("enters at the root when a full cut set cannot remove the failed hint", async () => {
		const paths = Array.from({ length: 11 }, (_, depth) =>
			Array.from({ length: 2 ** depth }, (_, n) => Array.from({ length: depth }, (_, d) => (n >>> (depth - d - 1)) & 1)),
		).flat();
		const t = setupTree(paths);
		t.client.resolve(key);
		await t.settle();
		const target = t.client.resolve(key);
		const entry = cachedHashTopology(t.cache, target.topology)!;
		const leaf = findLeaf(entry.view, 2, 2, key.hashKey, entry.cut);
		for (let pos = 2048; entry.cut.size < TOPOLOGY_CUT_MAX; pos++) {
			if (pos !== leaf.pos) {
				cutTopologyHint(entry, pos);
			}
		}
		const result = await t.client.point("op", key, { n: 1 });
		expect(result.value).toBe(t.rootName);
		expect(result.clientRpcs).toBe(2);
		expect(t.sent).toEqual([target.doName, t.rootName]);
		expect(entry.cut.size).toBe(TOPOLOGY_CUT_MAX);
	});

	it.each([17, 32])("enters at the root after bounded fallback through %i stale levels", async (depth) => {
		const t = setupTree(chain(depth));
		t.client.resolve(key);
		await t.settle();
		const result = await t.client.point("op", key, { n: 1 });
		expect(result.value).toBe(t.rootName);
		expect(result.clientRpcs).toBeLessThanOrEqual(18);
		expect(t.sent.at(-1)).toBe(t.rootName);
		expect(t.sent.filter((name) => name === t.rootName)).toHaveLength(1);
	});

	it("propagates a miss at the root after the final fallback", async () => {
		const t = setupTree(chain(32), missing);
		t.client.resolve(key);
		await t.settle();
		await expect(t.client.point("op", key, { n: 1 })).rejects.toMatchObject({ attributes: { doName: t.rootName } });
		expect(t.sent.length).toBeLessThanOrEqual(18);
		expect(t.sent.at(-1)).toBe(t.rootName);
		expect(t.sent.filter((name) => name === t.rootName)).toHaveLength(1);
	});

	it("does not fall back after a nested miss at the fallback bound", async () => {
		let calls = 0;
		const t = setupTree(chain(32), (doName) => missing(++calls === 17 ? "another-partition" : doName));
		t.client.resolve(key);
		await t.settle();
		await expect(t.client.point("op", key, { n: 1 })).rejects.toMatchObject({ attributes: { doName: "another-partition" } });
		expect(t.sent).toHaveLength(17);
		expect(t.sent).not.toContain(t.rootName);
	});
});

describe("topology cache — range entries", () => {
	const hashKey = KeyCodec.encode("u42");
	const k = (s: string) => KeyCodec.encode(s);
	// R [~min, ~max) into A [~min, g), B [g, p), C [p, ~max); B into [g, k), [k, p).
	const rangeBody = () =>
		encodeRangeBody({
			epoch: 1,
			version: 1,
			boundaries: [k("g"), k("k"), k("p")],
			levels: [0, 1, 0],
			depths: [1, 2, 2, 1],
			truncatedAtRank: null,
		});
	const emptyRange = () => encodeRangeBody({ epoch: 1, version: 1, boundaries: [], levels: [], depths: [], truncatedAtRank: null });
	const hash = () => hashBody({ version: 1, promoted: [hashKey] });
	const name = (sg: string, start: string | null, end: string | null) =>
		resolveRangePartitionContext(
			{ topology: { shardGroup: sg } } as FokosRouteContext<null>,
			hashKey,
			start === null ? null : k(start),
			end === null ? null : k(end),
		).doName;
	const RANGE = topologyGetForRangePath("SG", hashKey);

	it("enters at the range root before the range entry, then at the deepest known range partition", async () => {
		const t = setup(
			new Map([
				[GET, hash],
				[RANGE, rangeBody],
			]),
		);
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		expect(t.client.resolve({ hashKey, sortKey: k("h") }).doName).toBe(name(t.shardGroup, null, null));
		await t.settle();
		expect(t.client.resolve({ hashKey, sortKey: k("h") }).doName).toBe(name(t.shardGroup, "g", "k"));
		expect(t.client.resolve({ hashKey, sortKey: k("a") }).doName).toBe(name(t.shardGroup, null, "g"));
		const range = (lo: string, hi: string) =>
			t.client.resolveRange({
				hashKey,
				descending: false,
				interval: { lower: { value: k(lo), inclusive: true }, upper: { value: k(hi), inclusive: false } },
			}).doName;
		expect(range("h", "m")).toBe(name(t.shardGroup, "g", "p"));
		expect(range("a", "h")).toBe(name(t.shardGroup, null, null));
	});

	it("uses the hash path when the range entry is empty", async () => {
		const t = setup(
			new Map([
				[GET, hash],
				[RANGE, emptyRange],
			]),
		);
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		expect(t.client.resolve({ hashKey, sortKey: k("h") }).doName).toBe(
			PartitionIdHelper.hashId(t.shardGroup, hashRootIndex(hashKey, 2)).doName,
		);
	});

	it("completes through the hash path when the range root of a Bloom hit does not exist", async () => {
		let shardGroup = "";
		const t = setup(new Map([[GET, hash]]), (doName) =>
			doName === name(shardGroup, null, null)
				? new FokosRoutingError(SHARDING_ROUTING_CODES.range_partition_not_initialized, { message: "no identity", attributes: { doName } })
				: null,
		);
		shardGroup = t.shardGroup;
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		const result = await t.client.point("op", { hashKey, sortKey: k("h") }, { n: 1 });
		expect(result.clientRpcs).toBe(2);
		expect(result.value).toBe(PartitionIdHelper.hashId(t.shardGroup, hashRootIndex(hashKey, 2)).doName);
	});
});
