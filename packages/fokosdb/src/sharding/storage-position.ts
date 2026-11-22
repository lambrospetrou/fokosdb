/**
 * Positions in the storage order `(routingHash, hk, sk)`. The sharding client and the runtime both use
 * this module, so it imports only modules that the client bundle already holds.
 */
import { asRoutingHash, compareRoutingHashes, routingHashOf, type RoutingHash, type RoutingHashSeedResolver } from "./hash-primitives.js";
import { KeyCodec } from "./key-codec.js";
import type { RouteKey } from "./repartition-types.js";
import type { FokosStorageKey, FokosStoragePosition } from "./runtime-types.js";

/** The empty key: the minimum `hk` or `sk` of a position. */
const NO_KEY = KeyCodec.encodeOptional(undefined);

/**
 * The position of a route key or of a storage key in the storage order. A route key costs one hash in
 * `shardGroup`. A storage key gets a length check on its `routingHash`, and an absent `hk` or `sk` gets
 * the empty key, which is the minimum.
 */
export function storagePositionOf(
	key: RouteKey | FokosStorageKey,
	shardGroup: string,
	routingHashSeedResolver: RoutingHashSeedResolver,
): FokosStoragePosition {
	if ("routingHash" in key) {
		return { routingHash: asRoutingHash(key.routingHash), hk: key.hk ?? NO_KEY, sk: key.sk ?? NO_KEY };
	}
	return { routingHash: routingHashOf(key.hashKey, shardGroup, routingHashSeedResolver), hk: key.hashKey, sk: key.sortKey };
}

/** The order of two positions in the storage order `(routingHash, hk, sk)`. An absent `hk` or `sk` is the empty key. */
export function compareStorageKeys(a: FokosStorageKey, b: FokosStorageKey): number {
	return (
		compareRoutingHashes(a.routingHash as RoutingHash, b.routingHash as RoutingHash) ||
		KeyCodec.compare(a.hk ?? NO_KEY, b.hk ?? NO_KEY) ||
		KeyCodec.compare(a.sk ?? NO_KEY, b.sk ?? NO_KEY)
	);
}
