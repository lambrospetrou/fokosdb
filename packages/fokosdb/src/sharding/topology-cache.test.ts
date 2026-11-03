import { describe, expect, it, vi } from "vitest";
import { FokosRoutingError } from "../shared/errors.js";
import { FokosShardingClient } from "./client.js";
import { SHARDING_ROUTING_CODES } from "./errors.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import { PartitionIdHelper, resolveRangePartitionContext } from "./partition-id.js";
import type { FokosRouteContext } from "./route-context.js";
import type { FokosEnvelope } from "./runtime-types.js";
import {
	cachedHashTopology,
	cachedRangeTopology,
	cutTopologyHint,
	TOPOLOGY_CUT_MAX,
	topologyGetForRangePath,
	topologyGetPath,
	type FokosTopologyCacheOptions,
} from "./topology-cache.js";
import { findLeaf, isPromotedHashPrefixBucket, parseHashBody } from "./topology-codec.js";
import {
	encodeHashBitmap,
	encodeHashBody,
	encodeRangeBody,
	hashNodeKey,
	mergePromotedBuckets,
	promotedKeysBloom,
} from "./topology-encode.js";
import { hashDigits } from "../../test/hash-digits.js";
import { compareHashPrefixBuckets, hashPrefixBucketOf, type HashPrefixBucket } from "./hash-primitives.js";

type Ops = { op: { req: { n: number }; res: string } };

const NO_SORT_KEY = KeyCodec.encodeOptional(undefined);

function hashBody(opts: {
	version: number;
	R?: number;
	K?: number;
	splits?: Uint8Array[];
	promoted?: HashPrefixBucket[];
	/** The section the promoted set goes into, as the keeper picks by its count. Default "list". */
	promotedAs?: "list" | "bloom";
}): Uint8Array {
	const R = opts.R ?? 2;
	const K = opts.K ?? 2;
	const promoted = opts.promoted ?? [];
	const asList = (opts.promotedAs ?? "list") === "list";
	// Each test splits roots only, so the order of `splits` is the byte order of the node keys.
	const nodes = (opts.splits ?? []).map(hashNodeKey);
	return encodeHashBody({
		epoch: 1,
		version: opts.version,
		rootTreesN: R,
		hashSplitN: K,
		bitmap: encodeHashBitmap(R, K, nodes.length, () => nodes),
		promoted: asList ? mergePromotedBuckets([], [...promoted].sort(compareHashPrefixBuckets)) : null,
		bloom: asList ? null : promotedKeysBloom(promoted.length, () => promoted),
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
	shardGroup = newShardGroup(),
) {
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

/** A new shard group, so no two tests share a cache entry. */
function newShardGroup(): string {
	return `tc_${crypto.randomUUID()}`;
}

/** The root and the hash child of `hashKey` below the root, with R 2 and K 2. */
function childOf(shardGroup: string, hashKey: KeyBytes): { root: number; child: number } {
	const { root, path } = hashDigits({ shardGroup, rootTreesN: 2, hashSplitN: 2 }, hashKey, 1);
	return { root, child: path[0] };
}

const GET = topologyGetPath("SG");

/** The partition ID bytes of the hash root `root`. */
function rootId(root: number): Uint8Array {
	return PartitionIdHelper.fromHashIdxs("sg", [root]).encode(false).bytes;
}

describe("topology cache — hash entries", () => {
	it("starts one fetch through waitUntil, keeps no promise, and enters at the leaf after the fetch", async () => {
		const key = { hashKey: KeyCodec.encode("alpha"), sortKey: NO_SORT_KEY };
		const shardGroup = newShardGroup();
		const { root, child } = childOf(shardGroup, key.hashKey);
		const t = setup(new Map([[GET, () => hashBody({ version: 1, splits: [rootId(root)] })]]), undefined, {}, shardGroup);
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
		const shardGroup = newShardGroup();
		const { root, child } = childOf(shardGroup, key.hashKey);
		let version = 5;
		const t = setup(new Map([[GET, () => hashBody({ version, splits: version === 5 ? [rootId(root)] : [] })]]), undefined, {}, shardGroup);
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
		for (const body of [
			(root: number) => hashBody({ version: 1, K: 3, splits: [rootId(root)] }),
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
					promoted: null,
					bloom: null,
				});
			},
		]) {
			const shardGroup = newShardGroup();
			const { root } = childOf(shardGroup, key.hashKey);
			const t = setup(new Map([[GET, () => body(root)]]), undefined, {}, shardGroup);
			t.client.resolve(key);
			await t.settle();
			expect(t.client.resolve(key).doName).toBe(PartitionIdHelper.hashId(shardGroup, root).doName);
		}
		const errors = warn.mock.calls.map(([entry]) => String((entry as { error: string }).error));
		expect(errors.some((e) => e.includes("hashSplitN 3"))).toBe(true);
		expect(errors.some((e) => e.includes("larger than the cache budget"))).toBe(true);
	});

	it("after a miss at a topology entry, sends again to the parent and cuts the hint", async () => {
		const key = { hashKey: KeyCodec.encode("delta"), sortKey: NO_SORT_KEY };
		const shardGroup = newShardGroup();
		const { root, child } = childOf(shardGroup, key.hashKey);
		const deep = (sg: string) => PartitionIdHelper.hashId(sg, root, [child]).doName;
		const t = setup(
			new Map([[GET, () => hashBody({ version: 1, splits: [rootId(root)] })]]),
			(doName) =>
				doName === deep(shardGroup)
					? new FokosRoutingError(SHARDING_ROUTING_CODES.hash_partition_not_initialized, { message: "no identity", attributes: { doName } })
					: null,
			{},
			shardGroup,
		);
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

describe("topology cache — scope", () => {
	it("keeps the hash and range entries of two scopes apart, also for one topology object", async () => {
		const topology = { shardGroup: `tc_${crypto.randomUUID()}`, rootTreesN: 2, hashSplitN: 2 };
		const hashKey = hashPrefixBucketOf(KeyCodec.encode("scoped"), topology.shardGroup);
		const pending: Promise<unknown>[] = [];
		const fetched: string[] = [];
		const options = (scope: string | undefined, version: number): FokosTopologyCacheOptions => ({
			fetch: async (path) => {
				fetched.push(`${scope}:${path.includes("GetForRange") ? "range" : "hash"}`);
				const body = path.includes("GetForRange")
					? encodeRangeBody({ epoch: 1, version, boundaries: [], levels: [], depths: [], truncatedAtRank: null })
					: hashBody({ version });
				return new Response(body);
			},
			waitUntil: (p) => pending.push(p),
			...(scope === undefined ? {} : { scope }),
		});
		const all = [options("a", 1), options("b", 2), options(undefined, 3)];
		for (const opts of all) {
			expect(cachedHashTopology(opts, topology)).toBeUndefined();
			expect(cachedRangeTopology(opts, topology.shardGroup, hashKey)).toBeUndefined();
		}
		await Promise.all(pending);
		expect(fetched.sort()).toEqual(["a:hash", "a:range", "b:hash", "b:range", "undefined:hash", "undefined:range"]);
		expect(all.map((opts) => cachedHashTopology(opts, topology)?.view.version)).toEqual([1n, 2n, 3n]);
		expect(all.map((opts) => cachedRangeTopology(opts, topology.shardGroup, hashKey)?.view.version)).toEqual([1n, 2n, 3n]);
	});
});

describe("topology cache — bounded fallback", () => {
	const key = { hashKey: KeyCodec.encode("stale"), sortKey: NO_SORT_KEY };
	const missing = (doName: string) =>
		new FokosRoutingError(SHARDING_ROUTING_CODES.hash_partition_not_initialized, {
			message: "no identity",
			attributes: { doName },
		});
	/**
	 * A client of a new shard group, with a topology of the split nodes that `makePaths` gives under the
	 * root of the key. `chain(depth)` is the path of the key and each of its ancestors, `depth` levels.
	 */
	const setupTree = (
		makePaths: (chain: (depth: number) => number[][]) => number[][],
		failAt?: (doName: string) => FokosRoutingError | null,
	) => {
		const shardGroup = newShardGroup();
		const chain = (depth: number) => {
			const { path } = hashDigits({ shardGroup, rootTreesN: 2, hashSplitN: 2 }, key.hashKey, depth);
			return Array.from({ length: depth }, (_, d) => path.slice(0, d));
		};
		const { root } = childOf(shardGroup, key.hashKey);
		const paths = makePaths(chain);
		const nodes = paths.map((path) => hashNodeKey(PartitionIdHelper.fromHashIdxs("sg", [root, ...path]).encode(false).bytes));
		const bytes = encodeHashBody({
			epoch: 1,
			version: 1,
			rootTreesN: 2,
			hashSplitN: 2,
			bitmap: encodeHashBitmap(2, 2, nodes.length, () => nodes),
			promoted: null,
			bloom: promotedKeysBloom(0, () => []),
		});
		const t = setup(
			new Map([[GET, () => bytes]]),
			failAt ?? ((doName) => (doName.includes(`~h.${root}.`) ? missing(doName) : null)),
			{ refreshMs: Number.MAX_SAFE_INTEGER },
			shardGroup,
		);
		return { ...t, rootName: PartitionIdHelper.hashId(t.shardGroup, root).doName };
	};

	it("enters at the root when a full cut set cannot remove the failed hint", async () => {
		const paths = Array.from({ length: 11 }, (_, depth) =>
			Array.from({ length: 2 ** depth }, (_, n) => Array.from({ length: depth }, (_, d) => (n >>> (depth - d - 1)) & 1)),
		).flat();
		const t = setupTree(() => paths);
		t.client.resolve(key);
		await t.settle();
		const target = t.client.resolve(key);
		const entry = cachedHashTopology(t.cache, target.topology)!;
		const leaf = findLeaf(entry.view, 2, 2, hashPrefixBucketOf(key.hashKey, t.shardGroup), entry.cut);
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
		const t = setupTree((chain) => chain(depth));
		t.client.resolve(key);
		await t.settle();
		const result = await t.client.point("op", key, { n: 1 });
		expect(result.value).toBe(t.rootName);
		expect(result.clientRpcs).toBeLessThanOrEqual(18);
		expect(t.sent.at(-1)).toBe(t.rootName);
		expect(t.sent.filter((name) => name === t.rootName)).toHaveLength(1);
	});

	it("propagates a miss at the root after the final fallback", async () => {
		const t = setupTree((chain) => chain(32), missing);
		t.client.resolve(key);
		await t.settle();
		await expect(t.client.point("op", key, { n: 1 })).rejects.toMatchObject({ attributes: { doName: t.rootName } });
		expect(t.sent.length).toBeLessThanOrEqual(18);
		expect(t.sent.at(-1)).toBe(t.rootName);
		expect(t.sent.filter((name) => name === t.rootName)).toHaveLength(1);
	});

	it("does not fall back after a nested miss at the fallback bound", async () => {
		let calls = 0;
		const t = setupTree(
			(chain) => chain(32),
			(doName) => missing(++calls === 17 ? "another-partition" : doName),
		);
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
	/** A boundary of the hash key `u42`. */
	const b = (s: string) => ({ hk: hashKey, sk: k(s) });
	// R [~min, ~max) into A [~min, g), B [g, p), C [p, ~max); B into [g, k), [k, p). Each boundary is of `u42`.
	const rangeBody = () =>
		encodeRangeBody({
			epoch: 1,
			version: 1,
			boundaries: [b("g"), b("k"), b("p")],
			levels: [0, 1, 0],
			depths: [1, 2, 2, 1],
			truncatedAtRank: null,
		});
	const emptyRange = () => encodeRangeBody({ epoch: 1, version: 1, boundaries: [], levels: [], depths: [], truncatedAtRank: null });
	/** The bodies of a new shard group: the promoted set and the range path hold the hashPrefixBucket of `u42` in it. */
	const fixture = (range: (() => Uint8Array) | null) => {
		const shardGroup = newShardGroup();
		const bucket = hashPrefixBucketOf(hashKey, shardGroup);
		const bodies = new Map([[GET, () => hashBody({ version: 1, promoted: [bucket] })]]);
		if (range) {
			bodies.set(topologyGetForRangePath("SG", bucket), range);
		}
		const name = (start: string | null, end: string | null) =>
			resolveRangePartitionContext(
				{ topology: { shardGroup } } as FokosRouteContext<null>,
				bucket,
				start === null ? null : b(start),
				end === null ? null : b(end),
			).doName;
		return { shardGroup, bodies, name };
	};

	it("enters at the range root before the range entry, then at the deepest known range partition", async () => {
		const f = fixture(rangeBody);
		const t = setup(f.bodies, undefined, {}, f.shardGroup);
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		expect(t.client.resolve({ hashKey, sortKey: k("h") }).doName).toBe(f.name(null, null));
		await t.settle();
		expect(t.client.resolve({ hashKey, sortKey: k("h") }).doName).toBe(f.name("g", "k"));
		expect(t.client.resolve({ hashKey, sortKey: k("a") }).doName).toBe(f.name(null, "g"));
		const range = (lo: string, hi: string) =>
			t.client.resolveRange({
				hashKey,
				descending: false,
				interval: { lower: { value: k(lo), inclusive: true }, upper: { value: k(hi), inclusive: false } },
			}).doName;
		expect(range("h", "m")).toBe(f.name("g", "p"));
		expect(range("a", "h")).toBe(f.name(null, null));
		// With no upper bound, the request ends below the next hash key.
		expect(t.client.resolveRange({ hashKey, descending: false, interval: { lower: { value: k("q"), inclusive: true } } }).doName).toBe(
			f.name("p", null),
		);
	});

	it("completes through the hash path when the range root of a Bloom hit does not exist", async () => {
		const f = fixture(null);
		const t = setup(
			f.bodies,
			(doName) =>
				doName === f.name(null, null)
					? new FokosRoutingError(SHARDING_ROUTING_CODES.range_partition_not_initialized, {
							message: "no identity",
							attributes: { doName },
						})
					: null,
			{},
			f.shardGroup,
		);
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		const result = await t.client.point("op", { hashKey, sortKey: k("h") }, { n: 1 });
		expect(result.clientRpcs).toBe(2);
		expect(result.value).toBe(PartitionIdHelper.hashId(t.shardGroup, childOf(t.shardGroup, hashKey).root).doName);
	});

	it.each(["list", "bloom"] as const)("starts one range fetch on a positive %s answer, and none on a negative", async (promotedAs) => {
		const shardGroup = newShardGroup();
		const hit = hashPrefixBucketOf(hashKey, shardGroup);
		const body = hashBody({ version: 1, promoted: [hit], promotedAs });
		// A Bloom filter can hold a key that is not promoted, so the miss comes from the built body.
		const view = parseHashBody(body);
		const missKey = Array.from({ length: 500 }, (_, i) => `miss-${i}`).find(
			(candidate) => !isPromotedHashPrefixBucket(view, hashPrefixBucketOf(KeyCodec.encode(candidate), shardGroup)),
		)!;
		const miss = { hashKey: KeyCodec.encode(missKey), sortKey: k("h") };
		const bodies = new Map([[GET, () => body]]);
		bodies.set(topologyGetForRangePath("SG", hit), rangeBody);
		// The entries stay fresh, so only a promoted-set hit starts a read.
		const t = setup(bodies, undefined, { refreshMs: Number.MAX_SAFE_INTEGER }, shardGroup);
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		expect(t.fetched).toEqual([topologyGetPath(shardGroup)]);
		// The hit needs two reads: the hash body, then the range body.
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		expect(t.fetched).toEqual([topologyGetPath(shardGroup), topologyGetForRangePath(shardGroup, hit)]);
		expect(t.client.resolve({ hashKey, sortKey: k("h") }).doName).toContain("~r.");
		// The miss stops at the hash body: no range fetch, and the answer is the hash partition.
		t.client.resolve(miss);
		await t.settle();
		expect(t.fetched).toHaveLength(2);
		expect(t.client.resolve(miss).doName).toBe(PartitionIdHelper.hashId(shardGroup, childOf(shardGroup, miss.hashKey).root).doName);
	});

	it.each([
		["list", true],
		["bloom", false],
	] as const)("with speculative false, sends a %s hit with no range entry to the range root: %s", async (promotedAs, toRangeRoot) => {
		const shardGroup = newShardGroup();
		const hit = hashPrefixBucketOf(hashKey, shardGroup);
		// No range body: the range entry is not in the cache.
		const bodies = new Map([[GET, () => hashBody({ version: 1, promoted: [hit], promotedAs })]]);
		const t = setup(bodies, undefined, { refreshMs: Number.MAX_SAFE_INTEGER }, shardGroup);
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		const doName = t.client.resolve({ hashKey, sortKey: k("h") }, { speculative: false }).doName;
		// A list hit is exact, so its range root exists. A Bloom hit can be false, so it stays at the hash partition.
		if (toRangeRoot) {
			expect(doName).toContain("~r.");
		} else {
			expect(doName).toBe(PartitionIdHelper.hashId(shardGroup, childOf(shardGroup, hashKey).root).doName);
		}
	});

	it.each([
		["list", true],
		["bloom", false],
	] as const)("sends a %s hit with a range entry that has no segment to the range root: %s", async (promotedAs, toRangeRoot) => {
		const shardGroup = newShardGroup();
		const hit = hashPrefixBucketOf(hashKey, shardGroup);
		// The keeper serves a range body with no segment until it builds the range body of the bucket.
		const bodies = new Map([[GET, () => hashBody({ version: 1, promoted: [hit], promotedAs })]]);
		bodies.set(topologyGetForRangePath("SG", hit), emptyRange);
		const t = setup(bodies, undefined, { refreshMs: Number.MAX_SAFE_INTEGER }, shardGroup);
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		t.client.resolve({ hashKey, sortKey: k("h") });
		await t.settle();
		expect(t.fetched).toEqual([topologyGetPath(shardGroup), topologyGetForRangePath(shardGroup, hit)]);
		const doName = t.client.resolve({ hashKey, sortKey: k("h") }).doName;
		// A list hit is exact, so the range root exists. For a Bloom hit, the entry shows a false positive.
		if (toRangeRoot) {
			expect(doName).toContain("~r.");
		} else {
			expect(doName).toBe(PartitionIdHelper.hashId(shardGroup, childOf(shardGroup, hashKey).root).doName);
		}
	});
});
