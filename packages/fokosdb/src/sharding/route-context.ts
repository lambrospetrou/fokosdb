/**
 * The route context that every request to a partition carries, and the identity a partition stores.
 *
 * A Durable Object takes no parameters at creation, so every request carries what the partition
 * needs: its own identity, the immutable topology of its shard group, the range split parameters,
 * and the host policy. The sharding code reads the first three and treats the policy as an opaque
 * value that it stores, compares, and forwards.
 */
import type { KeyBytes } from "./key-codec.js";
import type { RangeAncestorInfo } from "./types.js";
import { FokosValidationError, VALIDATION_CODES } from "../shared/errors.js";

export type FokosTopology = {
	shardGroup: string;
	rootTreesN: number;
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
	/** Immutable identity of the target partition: hex-encoded opaque bytes, the wire format of `PartitionIdHelper`. */
	partitionId: string;
	/** `<shardGroup>.h.<root>[.<child>...]` or `<shardGroup>.r.<hk>.<start>.<end>`. */
	doName: string;
	/** Immutable topology of the shard group. Persisted at creation. A later mismatch is an error. */
	topology: FokosTopology;
	/** Read when a range split is planned. Mutable, last writer wins. */
	rangeConfig: FokosRangeConfig;
	/** Host policy. Opaque to the sharding code. Persisted and replaced when a request carries a new value. */
	policy: TPolicy;
};

/** The immutable identity of one partition. A name alone is a value the caller chose. */
export type FokosPartitionRef = Pick<FokosRouteContext<unknown>, "partitionId" | "doName">;

/** The identity a partition stores under `__fokos/identity`, written once at bootstrap or `fokosInit`. */
export type FokosPartitionIdentity = {
	schema: 1;
	ref: FokosPartitionRef;
	kind: "hash" | "range";
	/** Hash: the root index and the child path, decoded from `ref.partitionId`. */
	hash?: { rootIndex: number; path: number[] };
	range?: {
		hashKey: KeyBytes;
		start: KeyBytes | null;
		end: KeyBytes | null;
		depth: number;
		/** The bounded ancestor set from `fokosInit`. Immutable. */
		ancestors: RangeAncestorInfo[];
	};
	topology: FokosTopology;
};

/** The mutable part of the last route context a partition received, under `__fokos/policy`. */
export type FokosStoredPolicy<TPolicy> = { rangeConfig: FokosRangeConfig; policy: TPolicy };

export const FOKOS_IDENTITY_KV_KEY = "__fokos/identity";
export const FOKOS_POLICY_KV_KEY = "__fokos/policy";

/** The first byte of a partition ID names its schema; see `PartitionIdHelper`. */
const HASH_SCHEMA_PREFIX = "00";
const RANGE_SCHEMA_PREFIX = "01";

export function isHashPartition(ref: FokosPartitionRef): boolean {
	return ref.partitionId.startsWith(HASH_SCHEMA_PREFIX);
}

export function isRangePartition(ref: FokosPartitionRef): boolean {
	return ref.partitionId.startsWith(RANGE_SCHEMA_PREFIX);
}

export function refOf(ctx: FokosPartitionRef): FokosPartitionRef {
	return { partitionId: ctx.partitionId, doName: ctx.doName };
}

/** Names FokosDB reserves for its own Durable Objects. A host shard group must not start with it. */
export const RESERVED_SHARD_GROUP_PREFIX = "fokos.";

function invalid(option: string, value: unknown, message: string): FokosValidationError {
	return new FokosValidationError(VALIDATION_CODES.partition_context_options_invalid, { message, attributes: { option, value } });
}

export function validateTopology(topology: FokosTopology): void {
	if (typeof topology.shardGroup !== "string" || topology.shardGroup.length === 0) {
		throw invalid("shardGroup", topology.shardGroup, "shardGroup must be a non-empty string");
	}
	if (topology.shardGroup.startsWith(RESERVED_SHARD_GROUP_PREFIX)) {
		throw invalid("shardGroup", topology.shardGroup, `shardGroup must not start with "${RESERVED_SHARD_GROUP_PREFIX}"`);
	}
	if (!Number.isInteger(topology.rootTreesN) || topology.rootTreesN < 1 || topology.rootTreesN > 65000) {
		throw invalid("rootTreesN", topology.rootTreesN, "rootTreesN must be between 1 and 65000");
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
	if (a === b) return true;
	if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) return false;
	if (Array.isArray(a) || Array.isArray(b)) {
		if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
		return a.every((value, i) => structurallyEqual(value, b[i]));
	}
	const keysA = Object.keys(a).filter((k) => (a as Record<string, unknown>)[k] !== undefined);
	const keysB = Object.keys(b).filter((k) => (b as Record<string, unknown>)[k] !== undefined);
	if (keysA.length !== keysB.length) return false;
	return keysA.every((k) => structurallyEqual((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]));
}
