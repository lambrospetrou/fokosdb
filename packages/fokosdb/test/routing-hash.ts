import { routingHashOf, routingHashSeedOf, type RoutingHash } from "../src/sharding/hash-primitives.js";
import { fokosRoutingSeedOf, RESERVED_SHARD_GROUP_PREFIX } from "../src/shared/partition-context.js";
import type { KeyBytes } from "../src/sharding/key-codec.js";
import type { FokosStorageInterval } from "../src/sharding/runtime-types.js";

/**
 * The seed function of the host of `shardGroup` in the tests. A FokosDB Durable Object serves each
 * `fokos.` group, and a test host of the sharding library serves every other group with the default.
 */
export function testRoutingSeedOf(shardGroup: string): string {
	return shardGroup.startsWith(RESERVED_SHARD_GROUP_PREFIX) ? fokosRoutingSeedOf(shardGroup) : routingHashSeedOf(shardGroup);
}

/**
 * The routingHash of a hash key in the shard group `shardGroup`. A store treats the routingHash as opaque
 * bytes, so a store test can use any group. A test that writes rows that a partition reads later must
 * use the group of that partition.
 */
export function routingHashFor(hashKey: KeyBytes, shardGroup = "fokos.p.store-test"): RoutingHash {
	return routingHashOf(hashKey, shardGroup, testRoutingSeedOf);
}

/** The storage interval of every row of a table. */
export const ALL_ROWS: FokosStorageInterval = { start: { routingHash: new Uint8Array() }, end: null };

/**
 * The routingHash function of the partition that `state` belongs to. A partition named by its DO
 * name has the shard group before its first `~` (`SHARD_GROUP_SEPARATOR`), and the routingHash uses the
 * seed of that group, as the partition does.
 */
export function partitionRoutingHashOf(state: DurableObjectState): (hashKey: KeyBytes) => RoutingHash {
	const name = state.id.name;
	if (name === undefined || !name.includes("~")) {
		throw new Error(`partitionRoutingHashOf: ${String(name)} is not the DO name of a partition`);
	}
	const shardGroup = name.slice(0, name.indexOf("~"));
	return (hashKey) => routingHashOf(hashKey, shardGroup, testRoutingSeedOf);
}
