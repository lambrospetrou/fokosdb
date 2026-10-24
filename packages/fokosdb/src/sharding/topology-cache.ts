/**
 * The topology cache of the isolate: the views over the encoded topology bodies of the shard groups
 * that this isolate calls. Every `FokosShardingClient` of the isolate shares it, so the module must
 * stay in one chunk.
 *
 * A read does no I/O. When an entry is absent or old, the read starts one fetch through the
 * `waitUntil` of the caller and answers what the cache holds now. The cache keeps no promise: the
 * Workers runtime refuses I/O for one request from the context of another request.
 */
import { LRUCache } from "../shared/cache-lru.js";
import type { KeyBytes } from "./key-codec.js";
import {
	compareTopologyVersions,
	parseHashBody,
	parseRangeBody,
	type HashTopologyView,
	type RangeTopologyView,
	type TopologyVersion,
} from "./topology-codec.js";
import type { FokosTopology } from "./route-context.js";

export type FokosTopologyCacheOptions = {
	/** Host code. Fetches one encoded topology, for example through `ctx.exports.FokosTopologyCache.fetch`. */
	fetch(this: void, path: string): Promise<Response>;
	/** The `waitUntil` of the request (Worker) or of the `DurableObjectState` (Durable Object). */
	waitUntil(this: void, promise: Promise<unknown>): void;
	/** The age of an entry that starts a refresh. Default 10,000. */
	refreshMs?: number;
	/** The time after which a refresh that did not end can start again. Default 30,000. */
	refreshTimeoutMs?: number;
};

/** The memory of the topology cache of one isolate, in bytes. */
export const TOPOLOGY_CACHE_BUDGET_BYTES = 16 * 1024 * 1024;
/** The most positions in the `cut` set of one entry. A miss after that costs one more hop each time. */
export const TOPOLOGY_CUT_MAX = 1_024;

const DEFAULT_REFRESH_MS = 10_000;
const DEFAULT_REFRESH_TIMEOUT_MS = 30_000;

export type TopologyEntry<V> = {
	key: string;
	view: V;
	fetchedAt: number;
	/** The hints that a miss removed. They stay until a newer topology version replaces the entry. */
	cut: Set<number>;
};

type AnyEntry = TopologyEntry<HashTopologyView> | TopologyEntry<RangeTopologyView>;

function weightOf(entry: AnyEntry): number {
	// A hash cut is one bitmap position. A range cut is one pair of positions.
	const perCut = "words" in entry.view ? 8 : 16;
	return entry.view.byteLength + perCut * entry.cut.size;
}

const cache = new LRUCache<string, AnyEntry>(Number.MAX_SAFE_INTEGER, { maxWeight: TOPOLOGY_CACHE_BUDGET_BYTES, weightOf });
/** The start time of the refresh of each key that is in flight. */
const refreshStartedAt = new Map<string, number>();

function toBase64Url(bytes: Uint8Array): string {
	return bytes.toBase64({ alphabet: "base64url", omitPadding: true });
}

const utf8Encoder = new TextEncoder();

/** The path of `topology.Get` for a shard group. */
export function topologyGetPath(shardGroup: string): string {
	return `/internal/v1/topology.Get?sg=${toBase64Url(utf8Encoder.encode(shardGroup))}`;
}

/** The path of `topology.GetForRange` for one hash key of a shard group. */
export function topologyGetForRangePath(shardGroup: string, hashKey: KeyBytes): string {
	return `${topologyGetPath(shardGroup).replace("topology.Get", "topology.GetForRange")}&hk=${toBase64Url(hashKey)}`;
}

/** The cache key of the hash entry of each topology object. A client keeps one topology object. */
const hashKeys = new WeakMap<FokosTopology, string>();

/**
 * The hash entry of a shard group, or undefined. It starts a refresh when the entry is absent or old.
 * A body whose `rootTreesN` or `hashSplitN` differs from `topology` is dropped. A fresh entry costs
 * two map reads and no allocation.
 */
export function cachedHashTopology(opts: FokosTopologyCacheOptions, topology: FokosTopology): TopologyEntry<HashTopologyView> | undefined {
	let key = hashKeys.get(topology);
	if (key === undefined) {
		key = `h|${topology.shardGroup}|${topology.rootTreesN}|${topology.hashSplitN}`;
		hashKeys.set(topology, key);
	}
	const entry = cache.get(key) as TopologyEntry<HashTopologyView> | undefined;
	if (shouldRefresh(opts, key, entry)) {
		opts.waitUntil(
			refresh(opts, key, topologyGetPath(topology.shardGroup), (body) => {
				const view = parseHashBody(body);
				if (view.bitCount > 0 && (view.rootTreesN !== topology.rootTreesN || view.hashSplitN !== topology.hashSplitN)) {
					throw new Error(
						`the body has rootTreesN ${view.rootTreesN} and hashSplitN ${view.hashSplitN}, and the caller has ${topology.rootTreesN} and ${topology.hashSplitN}`,
					);
				}
				return view;
			}),
		);
	}
	return entry;
}

/** The range entry of one hash key, or undefined. It starts a refresh when the entry is absent or old. */
export function cachedRangeTopology(
	opts: FokosTopologyCacheOptions,
	shardGroup: string,
	hashKey: KeyBytes,
): TopologyEntry<RangeTopologyView> | undefined {
	const key = `r|${shardGroup}|${toBase64Url(hashKey)}`;
	const entry = cache.get(key) as TopologyEntry<RangeTopologyView> | undefined;
	if (shouldRefresh(opts, key, entry)) {
		opts.waitUntil(refresh(opts, key, topologyGetForRangePath(shardGroup, hashKey), parseRangeBody));
	}
	return entry;
}

/**
 * Removes one hint from an entry. The weight of the entry changes, so the cache stores it again,
 * which runs the eviction. An entry that a newer version replaced is not stored again.
 */
export function cutTopologyHint(entry: AnyEntry, position: number): void {
	if (entry.cut.size >= TOPOLOGY_CUT_MAX || entry.cut.has(position)) {
		return;
	}
	entry.cut.add(position);
	if (cache.get(entry.key) === entry) {
		cache.set(entry.key, entry);
	}
}

/**
 * True when the entry is absent or old and no refresh of the key started in the last
 * `refreshTimeoutMs`. A true answer records the start of the refresh.
 */
function shouldRefresh(opts: FokosTopologyCacheOptions, key: string, entry: AnyEntry | undefined): boolean {
	const now = Date.now();
	if (entry && now - entry.fetchedAt <= (opts.refreshMs ?? DEFAULT_REFRESH_MS)) {
		return false;
	}
	const started = refreshStartedAt.get(key);
	if (started !== undefined && now - started < (opts.refreshTimeoutMs ?? DEFAULT_REFRESH_TIMEOUT_MS)) {
		return false;
	}
	refreshStartedAt.set(key, now);
	return true;
}

async function refresh(
	opts: FokosTopologyCacheOptions,
	key: string,
	path: string,
	parse: (body: Uint8Array) => HashTopologyView | RangeTopologyView,
): Promise<void> {
	const startedAt = refreshStartedAt.get(key);
	try {
		const response = await opts.fetch(path);
		if (response.status !== 200) {
			await response.body?.cancel();
			throw new Error(`the topology read answered ${response.status}`);
		}
		const view = parse(new Uint8Array(await response.arrayBuffer()));
		const current = cache.get(key);
		if (current && compareTopologyVersions(view, current.view as TopologyVersion) <= 0) {
			// The same version keeps its cut set, and an older version changes nothing.
			current.fetchedAt = Date.now();
			return;
		}
		const entry = { key, view, fetchedAt: Date.now(), cut: new Set<number>() } as AnyEntry;
		if (weightOf(entry) > TOPOLOGY_CACHE_BUDGET_BYTES) {
			throw new Error(`the topology body of ${weightOf(entry)} bytes is larger than the cache budget`);
		}
		cache.set(key, entry);
	} catch (error) {
		console.warn({
			message: "fokos/topology-cache: a topology refresh failed. The cache keeps the entry it has.",
			key,
			error: String(error),
		});
	} finally {
		if (refreshStartedAt.get(key) === startedAt) {
			refreshStartedAt.delete(key);
		}
	}
}
