/**
 * The route context that every request to a partition carries, and the identity a partition stores.
 *
 * A Durable Object takes no parameters at creation, so every request carries what the partition
 * needs: its own identity, the immutable topology of its shard group, the range split parameters,
 * and the host policy. The sharding code reads the first three and treats the policy as an opaque
 * value that it stores, compares, and forwards.
 */
import type { FokosCompositeKey } from "./key-codec.js";
import type { RoutingHash } from "./hash-primitives.js";
import type { RangeAncestorInfo } from "./types.js";
import { FokosValidationError } from "../shared/errors.js";
import { SHARDING_VALIDATION_CODES } from "./errors.js";
import { SHARD_GROUP_SEPARATOR } from "./partition-id.js";

/**
 * The immutable topology of a shard group. `shardGroup`, `rootTreesN`, and `hashSplitN` must never
 * change after initialization, because they decide the partition of each stored hash key.
 *
 * Each hash key maps to a 64-bit routingHash. Read as a fraction in `[0, 1)`, its first digit
 * in base `rootTreesN` selects the root, and each next digit in base `hashSplitN` selects the child
 * at one level. Thus each hash partition owns one interval of routingHashes.
 *
 * Use a power of two for both `rootTreesN` and `hashSplitN`. Then each node interval is an exact
 * binary fraction of 2^64: all siblings have the same size, and the interval edges need no rounding.
 * Other values are correct, but sibling sizes can differ by one routingHash. Also, at the depth
 * limit some leaves can hold two routingHashes, and such a leaf cannot split.
 */
export type FokosTopology = {
	/**
	 * The name of the shard group, for example a table, a GSI, or a coordinator group. It names the
	 * Durable Objects of the group and gives the seed of its routingHash
	 * (`routingHashSeedOf`). It must not be empty and must not contain `SHARD_GROUP_SEPARATOR`.
	 */
	shardGroup: string;
	/**
	 * The number of root partitions, from 1 to `FOKOS_HASH_PARTITIONS_MAX`. All roots exist from the
	 * start, so set it for the write load before the first split. A power of two is best (see above).
	 */
	rootTreesN: number;
	/**
	 * The number of children of each hash split, from 2 to 255. The depth limit is the largest
	 * depth `d` with `rootTreesN * hashSplitN^d <= 2^64`, and at most `MAX_HASH_DEPTH`
	 * (`maxHashDepth`). A power of two is best (see above).
	 */
	hashSplitN: number;
	/**
	 * The Durable Object jurisdiction of every partition of this shard group. It is part of the
	 * identity of the group: a jurisdiction that changes names a different set of objects.
	 */
	jurisdiction?: DurableObjectJurisdiction;
};

export type FokosRangeConfig = {
	/** The child count of the next range split. Range children are named by their boundaries, so it can change. */
	rangeSplitN: number;
	/** The bounded ancestor set a new range child receives: the shallowest `fromRoot` and the deepest `fromLeaf`. */
	rangeAncestors: { fromRoot: number; fromLeaf: number };
};

export type FokosRouteContext<TPolicy> = {
	/** 2: the shape differs from the earlier partition context, and a reader must reject the old record. */
	schema: 2;
	/**
	 * Immutable identity of the target partition, the wire format of `PartitionIdHelper`: hex text for
	 * a hash partition, and `"01"` and two base64url parts for a range partition.
	 */
	partitionId: string;
	/** `<shardGroup>~h.<root>[.<child>...]` or `<shardGroup>~r.<routingHash hex>.<start>.<end>`. */
	doName: string;
	/** Immutable topology of the shard group. Persisted at creation. A later mismatch is an error. */
	topology: FokosTopology;
	/** Read when a range split is planned. Mutable, and `policyVersion` orders its changes. */
	rangeConfig: FokosRangeConfig;
	/** Host policy. Opaque to the sharding code. Mutable, and `policyVersion` orders its changes. */
	policy: TPolicy;
	/**
	 * The version of `rangeConfig` and `policy`, a non-negative integer. A partition stores the request
	 * values when this version is higher than the stored one, and ignores them when it is lower. With an
	 * equal version, the last writer wins. So two Worker versions that send different values during a
	 * deploy do not replace the stored values on each request, if the newer one sends a higher version.
	 */
	policyVersion: number;
};

/** The immutable identity of one partition. A name alone is a value the caller chose. */
export type FokosPartitionRef = Pick<FokosRouteContext<unknown>, "partitionId" | "doName">;

/** The identity a partition stores under `__fokos/identity`, written once at bootstrap or `fokosInit`. */
export type FokosPartitionIdentity = {
	schema: 1;
	ref: FokosPartitionRef;
	kind: "hash" | "range";
	/**
	 * Hash: the root index and the child path, decoded from `ref.partitionId`, and the routingHash
	 * interval `[start, end)` of the node. `depth` is `path.length`, and a null `end` has no upper bound.
	 */
	hash?: { rootIndex: number; path: number[]; depth: number; start: RoutingHash; end: RoutingHash | null };
	/**
	 * Range: the promoted routingHash of the range tree, and the composite interval
	 * `[start, end)` of this node inside it. A null edge is unbounded.
	 */
	range?: {
		routingHash: RoutingHash;
		start: FokosCompositeKey | null;
		end: FokosCompositeKey | null;
		depth: number;
		/** The bounded ancestor set from `fokosInit`. Immutable. */
		ancestors: RangeAncestorInfo[];
	};
	topology: FokosTopology;
	/**
	 * `routingHashSeedOf(topology.shardGroup)` when the identity was written. A partition whose
	 * stored seed differs from the seed that the code computes now refuses all work, because its
	 * stored rows and its routes use the stored seed.
	 */
	routingHashSeed: string;
};

/** The mutable part of the last route context a partition received, under `__fokos/policy`. */
export type FokosStoredPolicy<TPolicy> = { rangeConfig: FokosRangeConfig; policy: TPolicy; policyVersion: number };

export function refOf(ctx: FokosPartitionRef): FokosPartitionRef {
	return { partitionId: ctx.partitionId, doName: ctx.doName };
}

function invalid(option: string, value: unknown, message: string): FokosValidationError {
	return new FokosValidationError(SHARDING_VALIDATION_CODES.partition_context_options_invalid, { message, attributes: { option, value } });
}

/**
 * The maximum number of root partitions of a shard group. The partition ID keeps the root index in
 * two bytes, so the value must stay below 65536.
 */
export const FOKOS_HASH_PARTITIONS_MAX = 65_000;

export function validateTopology(topology: FokosTopology): void {
	if (typeof topology.shardGroup !== "string" || topology.shardGroup.length === 0) {
		throw invalid("shardGroup", topology.shardGroup, "shardGroup must be a non-empty string");
	}
	if (topology.shardGroup.includes(SHARD_GROUP_SEPARATOR)) {
		throw invalid("shardGroup", topology.shardGroup, `shardGroup must not contain "${SHARD_GROUP_SEPARATOR}"`);
	}
	if (!Number.isInteger(topology.rootTreesN) || topology.rootTreesN < 1 || topology.rootTreesN > FOKOS_HASH_PARTITIONS_MAX) {
		throw invalid("rootTreesN", topology.rootTreesN, `rootTreesN must be between 1 and ${FOKOS_HASH_PARTITIONS_MAX}`);
	}
	if (!Number.isInteger(topology.hashSplitN) || topology.hashSplitN < 2 || topology.hashSplitN > 255) {
		throw invalid("hashSplitN", topology.hashSplitN, "hashSplitN must be between 2 and 255");
	}
}

export function validateRangeConfig(rangeConfig: FokosRangeConfig): void {
	if (!Number.isInteger(rangeConfig.rangeSplitN) || rangeConfig.rangeSplitN < 2 || rangeConfig.rangeSplitN > 255) {
		throw invalid("rangeSplitN", rangeConfig.rangeSplitN, "rangeSplitN must be between 2 and 255");
	}
	const { fromRoot, fromLeaf } = rangeConfig.rangeAncestors;
	if (!Number.isInteger(fromRoot) || fromRoot < 0 || fromRoot > 10) {
		throw invalid("rangeAncestors.fromRoot", fromRoot, "rangeAncestors.fromRoot must be between 0 and 10");
	}
	if (!Number.isInteger(fromLeaf) || fromLeaf < 0 || fromLeaf > 10) {
		throw invalid("rangeAncestors.fromLeaf", fromLeaf, "rangeAncestors.fromLeaf must be between 0 and 10");
	}
}

export function validatePolicyVersion(policyVersion: number): void {
	if (!Number.isSafeInteger(policyVersion) || policyVersion < 0) {
		throw invalid("policyVersion", policyVersion, "policyVersion must be a non-negative integer");
	}
}

/**
 * Exhaustive over the type: adding a field to `FokosTopology` without a compare here is a compile
 * error, so a new field cannot slip past the identity check.
 */
export function topologiesEqual(a: FokosTopology, b: FokosTopology): boolean {
	const compared = {
		shardGroup: a.shardGroup === b.shardGroup,
		rootTreesN: a.rootTreesN === b.rootTreesN,
		hashSplitN: a.hashSplitN === b.hashSplitN,
		jurisdiction: a.jurisdiction === b.jurisdiction,
	} satisfies Record<keyof FokosTopology, boolean>;
	return Object.values(compared).every(Boolean);
}

/**
 * Structural equality over JSON-like values. A key whose value is `undefined` counts as absent, so
 * an object built with and one built without the optional field compare equal.
 */
export function structurallyEqual(a: unknown, b: unknown): boolean {
	if (a === b) {
		return true;
	}
	if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) {
		return false;
	}
	if (Array.isArray(a) || Array.isArray(b)) {
		if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) {
			return false;
		}
		return a.every((value, i) => structurallyEqual(value, b[i]));
	}
	const keysA = Object.keys(a).filter((k) => (a as Record<string, unknown>)[k] !== undefined);
	const keysB = Object.keys(b).filter((k) => (b as Record<string, unknown>)[k] !== undefined);
	if (keysA.length !== keysB.length) {
		return false;
	}
	return keysA.every((k) => structurallyEqual((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]));
}
