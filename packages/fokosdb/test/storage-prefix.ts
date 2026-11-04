import { routingHashOf, type RoutingHash } from "../src/sharding/hash-primitives.js";
import type { KeyBytes } from "../src/sharding/key-codec.js";
import type { FokosStorageInterval } from "../src/sharding/runtime-types.js";

/**
 * The storage prefix of a hash key in the shard group `shardGroup`. A store treats the prefix as opaque
 * bytes, so a store test can use any group. A test that writes rows that a partition reads later must
 * use the group of that partition.
 */
export function prefixOf(hashKey: KeyBytes, shardGroup = "fokos.p.store-test"): RoutingHash {
	return routingHashOf(hashKey, shardGroup);
}

/** The storage interval of every row of a table. */
export const ALL_ROWS: FokosStorageInterval = { start: { prefix: new Uint8Array() }, end: null };

/**
 * The storage prefix function of the partition that `state` belongs to. A partition named by its DO
 * name has the shard group before its first `~` (`SHARD_GROUP_SEPARATOR`), and the prefix uses the
 * seed of that group, as the partition does.
 */
export function partitionPrefixOf(state: DurableObjectState): (hashKey: KeyBytes) => RoutingHash {
	const name = state.id.name;
	if (name === undefined || !name.includes("~")) {
		throw new Error(`partitionPrefixOf: ${String(name)} is not the DO name of a partition`);
	}
	const shardGroup = name.slice(0, name.indexOf("~"));
	return (hashKey) => routingHashOf(hashKey, shardGroup);
}
