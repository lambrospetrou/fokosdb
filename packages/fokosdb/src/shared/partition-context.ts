/**
 * The FokosDB half of the route context: the policy the two Durable Object classes read, and the
 * function that validates the options of a table.
 *
 * The sharding code treats the policy as an opaque value. Everything FokosDB-specific that a request
 * must carry is here: the two namespace bindings, the location hint, and the split thresholds.
 */
// Type-only imports: the emit erases them, so this module carries no runtime dependency on the
// Durable Object implementations. The namespace-key filter below needs them to match by class
// identity. A structural alternative collapses to `any` when TypeScript resolves it mid-cycle from
// do-partition.ts itself.
import type { PartitionDO } from "../server/do-partition.js";
import type { TransactionCoordinatorDO } from "../server/do-transaction-coordinator.js";
import type { MaterializationDO } from "../server/materialization/do-materialization.js";
import type { FokosRangeConfig, FokosRouteContext, FokosTopology } from "../sharding/route-context.js";
import { validatePolicyVersion, validateRangeConfig, validateTopology } from "../sharding/route-context.js";
import { FokosValidationError } from "./errors.js";
import { SHARDING_VALIDATION_CODES } from "../sharding/errors.js";
import invariant from "./invariant.js";
import { resolveLimits, type FokosDBLimitOverrides, type FokosDBLimits } from "./transaction-limits.js";

export type SplitConditions = {
	/** The size in megabytes that makes the partition split. */
	maxSizeMb?: number;
};

export type PartitionNamespaceKey = {
	[K in keyof Env]: Env[K] extends DurableObjectNamespace<PartitionDO> ? K : never;
}[keyof Env];

export type TransactionCoordinatorNamespaceKey = {
	[K in keyof Env]: Env[K] extends DurableObjectNamespace<TransactionCoordinatorDO> ? K : never;
}[keyof Env];

export type MaterializationNamespaceKey = {
	[K in keyof Env]: Env[K] extends DurableObjectNamespace<MaterializationDO> ? K : never;
}[keyof Env];

/**
 * What FokosDB needs on every partition and coordinator, beside the topology. A request with a
 * higher `policyVersion` replaces it, and with an equal version the last writer wins. The exception is
 * `ns` and `nsTx`: a change to those selects another Durable Object namespace, which a
 * partition inside the old namespace cannot detect, so a shard group must keep them for life.
 */
export type FokosDBPolicy = {
	ns: PartitionNamespaceKey;
	nsTx: TransactionCoordinatorNamespaceKey;
	/** The binding of the `MaterializationDO` namespace. Present when the table has one. */
	nsMat?: MaterializationNamespaceKey;
	/**
	 * The location hint for the Durable Objects of this table. It is best-effort placement advice for
	 * the first time each object spawns. It is not part of the identity of an object.
	 */
	locationHint?: DurableObjectLocationHint;
	hashSplitConditions: SplitConditions;
	rangeSplitConditions: SplitConditions;
	/**
	 * The key size limits that the table overrides. Absent when the table uses the defaults, so a table
	 * with no overrides sends no extra bytes. Only the client reads it, through `resolveLimits`.
	 */
	limits?: FokosDBLimitOverrides;
	/**
	 * True when each partition records its item changes for export. Absent when false, so a table
	 * without capture sends no extra bytes.
	 */
	captureChanges?: true;
};

export type FokosDBRouteContext = FokosRouteContext<FokosDBPolicy>;

/**
 * FokosDB names its own shard groups with this prefix, so a table name must not start with it. The
 * partition group of a table is `fokos.p.<tableName>`, and its coordinator group is `fokos.tc.<tableName>`.
 */
export const RESERVED_SHARD_GROUP_PREFIX = "fokos.";

const PARTITION_SHARD_GROUP_PREFIX = `${RESERVED_SHARD_GROUP_PREFIX}p.`;
const MATERIALIZATION_SHARD_GROUP_PREFIX = `${RESERVED_SHARD_GROUP_PREFIX}m.`;

/** The coordinator group of a table, `fokos.tc.<tableName>`, from the partition group of the table. */
export function coordinatorShardGroup(topology: FokosTopology): string {
	return `${RESERVED_SHARD_GROUP_PREFIX}tc.${tableNameOf(topology)}`;
}

/** The materialization group of a table, `fokos.m.<tableName>`, from the partition group of the table. */
export function materializationShardGroup(topology: FokosTopology): string {
	return `${MATERIALIZATION_SHARD_GROUP_PREFIX}${tableNameOf(topology)}`;
}

/**
 * The configuration of the materialization group of a table. It has the topology, the range
 * configuration, and the policy of the partition group, with its own shard group name.
 */
export function materializationTableConfig(table: FokosDBTableConfig): FokosDBTableConfig {
	return { ...table, topology: { ...table.topology, shardGroup: materializationShardGroup(table.topology) } };
}

/**
 * The shard group of a GSI, `fokos.gsiv1.<n>.<tableName>.<indexName>`, from the partition group or the
 * materialization group of the table. `<n>` is the length of the table name, so each pair of names
 * gives one shard group. The name is the address of the GSI data, so it must never change.
 */
export function gsiShardGroup(topology: FokosTopology, indexName: string): string {
	const tableName = topology.shardGroup.startsWith(MATERIALIZATION_SHARD_GROUP_PREFIX)
		? topology.shardGroup.slice(MATERIALIZATION_SHARD_GROUP_PREFIX.length)
		: tableNameOf(topology);
	return `${RESERVED_SHARD_GROUP_PREFIX}gsiv1.${tableName.length}.${tableName}.${indexName}`;
}

/** True for the shard group of a GSI. */
export function isGsiShardGroup(shardGroup: string): boolean {
	return shardGroup.startsWith(`${RESERVED_SHARD_GROUP_PREFIX}gsiv1.`);
}

/**
 * The key limits of a GSI table. `h` and `s` are the limits of the base table. The physical sort key
 * holds the tag byte, the escaped GSI sort key, base hash key, and base sort key (each 0x00 can double),
 * two separators, and the longest trailer: a raw copy of the GSI sort key, its length, and two bytes.
 */
export function gsiLimits(base: FokosDBLimits): FokosDBLimits {
	const h = base.maxHashKeyBytes;
	const s = base.maxSortKeyBytes;
	return { maxHashKeyBytes: h, maxSortKeyBytes: 1 + 2 * (2 * s + h) + 4 + s + String(s).length + 2 };
}

/**
 * The configuration of the GSI table `indexName`. It has the topology, the range configuration, and
 * the policy of the base table, with its own shard group, the GSI key limits, no change capture, and
 * no materialization group.
 */
export function gsiTableConfig(table: FokosDBTableConfig, indexName: string): FokosDBTableConfig {
	const { captureChanges: _capture, nsMat: _nsMat, ...policy } = table.policy;
	return {
		...table,
		topology: { ...table.topology, shardGroup: gsiShardGroup(table.topology, indexName) },
		policy: { ...policy, limits: gsiLimits(resolveLimits(table.policy.limits)) },
	};
}

function tableNameOf(topology: FokosTopology): string {
	invariant(
		topology.shardGroup.startsWith(PARTITION_SHARD_GROUP_PREFIX),
		`fokos/partition-context: the partition shard group must start with "${PARTITION_SHARD_GROUP_PREFIX}"`,
	);
	return topology.shardGroup.slice(PARTITION_SHARD_GROUP_PREFIX.length);
}

/** The part of a route context that selects a namespace and a stub: enough for a Worker with no partition in mind. */
export type FokosDBStubContext = Pick<FokosDBRouteContext, "topology" | "policy">;

/** One table configuration: what `FokosRouter` is built from. */
export type FokosDBTableConfig = {
	topology: FokosTopology;
	rangeConfig: FokosRangeConfig;
	policy: FokosDBPolicy;
	policyVersion: number;
};

/**
 * The identity of a table. FokosDB derives the Durable Object names of the table from these values,
 * so they must never change after the table has data.
 *
 * A change to `name`, `ns`, `nsTx` or `jurisdiction` sends every request to other, empty Durable
 * Objects. A change to `rootTreesN`, `hashSplitN` or `coordinatorRootsN` makes the existing partitions
 * or coordinators reject each request with `partition_context_mismatch`. In each case the data of the
 * table does not change, but the client can no longer reach it.
 */
export type FokosTableIdentity = {
	/** The name of the table. It must not start with `"fokos."`. */
	readonly name: string;
	/** The binding of the `PartitionDO` namespace. */
	readonly ns: PartitionNamespaceKey;
	/** The binding of the `TransactionCoordinatorDO` namespace. */
	readonly nsTx: TransactionCoordinatorNamespaceKey;
	/** The binding of the `MaterializationDO` namespace. Required when `captureChanges` is true. */
	readonly nsMat?: MaterializationNamespaceKey;
	/** The number of root partitions, from 1 to 65,000. A hash of the hash key selects one. */
	readonly rootTreesN: number;
	/** The number of children of each hash split, from 2 to 255. */
	readonly hashSplitN: number;
	/**
	 * The number of root transaction coordinators. Default: two per root partition, at most 65,000.
	 * Each root splits by hash when it grows larger than `hashSplitConditions.maxSizeMb`.
	 */
	readonly coordinatorRootsN?: number;
	/**
	 * The Durable Object jurisdiction of every partition and coordinator of the table. Cloudflare gives
	 * different object IDs in different jurisdictions.
	 */
	readonly jurisdiction?: DurableObjectJurisdiction;
};

/** The options of a table. Only the options in `table` are permanent; the others can change. */
export type FokosTableOptions = {
	/**
	 * WARNING:: DO NOT EVER change the values of `table` after the table has data.
	 *
	 * The identity of the table. Never change these values after the table has data.
	 **/
	table: FokosTableIdentity;

	/**
	 * The location hint for the Durable Objects of this table. It is best-effort placement advice for
	 * the first time each object spawns.
	 */
	locationHint?: DurableObjectLocationHint;

	/** Default: `{ maxSizeMb: 500 }`. */
	hashSplitConditions?: SplitConditions;
	/** The number of children of the next range split, from 2 to 255. It applies to later splits only. Default: 4. */
	rangeSplitN?: number;
	/** Default: `{ maxSizeMb: 500 }`. */
	rangeSplitConditions?: SplitConditions;
	/**
	 * The ancestors that a new range child keeps, part of responses too for ancestors to learn about splits.
	 * Default: `{ fromRoot: 0, fromLeaf: 3 }`.
	 **/
	rangeAncestorsConfig?: { fromRoot: number; fromLeaf: number }; /**
	 * The key size limits of the table. Every client of the table must use the same values. Never
	 * decrease a key size limit after items with larger keys exist.
	 */
	limits?: FokosDBLimitOverrides;
	/**
	 * Records each item change in the partition, for the export to the global secondary indexes and
	 * the push targets. Default: false.
	 *
	 * Set it only when the table is created. A partition records only the changes after it receives
	 * the value, so a table that turns it on when it has data never exports its older items.
	 */
	captureChanges?: boolean;
	/**
	 * The version of the options above, a non-negative integer. Default: 0.
	 *
	 * Each partition and coordinator stores the options of the requests it receives. It stores them
	 * when the version is higher than the stored version, and ignores them when it is lower. With an
	 * equal version, the last request wins. Increase the version each time you change these options,
	 * so that requests from an older deploy do not replace them. To go back to earlier options, deploy
	 * them with a higher version: a lower version has no effect on partitions that saw the higher one.
	 */
	policyVersion?: number;
};

/** Validates the options of a table, applies the defaults, and splits them into the parts of a route context. */
export function createTableConfig(opts: FokosTableOptions): FokosDBTableConfig {
	const { table } = opts;
	// Each option defaults on its own, so a value the caller gives is never replaced. The caller's
	// object stays unchanged.
	const hashSplitConditions = opts.hashSplitConditions ?? { maxSizeMb: 500 };
	const rangeSplitN = opts.rangeSplitN ?? 4;
	const rangeSplitConditions = opts.rangeSplitConditions ?? { maxSizeMb: 500 };
	const rangeAncestors = opts.rangeAncestorsConfig ?? { fromRoot: 0, fromLeaf: 3 };
	const invalid = (option: string, value: unknown, message: string) =>
		new FokosValidationError(SHARDING_VALIDATION_CODES.partition_context_options_invalid, { message, attributes: { option, value } });

	// No default: `hashSplitN` is part of the topology and must never change, so the caller states it.
	if (!table.hashSplitN) {
		throw invalid("table.hashSplitN", table.hashSplitN, "table.hashSplitN must be provided");
	}

	if (typeof table.name !== "string" || table.name.length === 0) {
		throw invalid("table.name", table.name, "table.name must be a non-empty string");
	}
	if (table.name.startsWith(RESERVED_SHARD_GROUP_PREFIX)) {
		throw invalid("table.name", table.name, `table.name must not start with "${RESERVED_SHARD_GROUP_PREFIX}"`);
	}
	if (opts.captureChanges !== undefined && typeof opts.captureChanges !== "boolean") {
		throw invalid("captureChanges", opts.captureChanges, "captureChanges must be a boolean");
	}
	if (opts.captureChanges === true && !table.nsMat) {
		throw invalid("table.nsMat", table.nsMat, "table.nsMat must be provided when captureChanges is true");
	}
	const topology: FokosTopology = {
		shardGroup: `${PARTITION_SHARD_GROUP_PREFIX}${table.name}`,
		rootTreesN: table.rootTreesN,
		hashSplitN: table.hashSplitN,
		// A table that selects no jurisdiction stores a topology byte-identical to one built without
		// the option, so an existing record and a new one compare equal.
		...(table.jurisdiction === undefined ? {} : { jurisdiction: table.jurisdiction }),
	};
	validateTopology(topology);

	const rangeConfig: FokosRangeConfig = { rangeSplitN, rangeAncestors };
	validateRangeConfig(rangeConfig);

	validateSplitConditions("hashSplitConditions", hashSplitConditions, invalid);
	validateSplitConditions("rangeSplitConditions", rangeSplitConditions, invalid);

	// Only the known overrides travel. An override equal to the default stays, so a pinned value does
	// not change when a later version changes the default.
	const maxHashKeyBytes = opts.limits?.maxHashKeyBytes;
	const maxSortKeyBytes = opts.limits?.maxSortKeyBytes;
	let limits: FokosDBLimitOverrides | undefined;
	if (maxHashKeyBytes !== undefined || maxSortKeyBytes !== undefined) {
		limits = {
			...(maxHashKeyBytes === undefined ? {} : { maxHashKeyBytes }),
			...(maxSortKeyBytes === undefined ? {} : { maxSortKeyBytes }),
		};
	}
	if (limits !== undefined) {
		resolveLimits(limits);
	}

	const policy: FokosDBPolicy = {
		ns: table.ns,
		nsTx: table.nsTx,
		...(table.nsMat === undefined ? {} : { nsMat: table.nsMat }),
		hashSplitConditions,
		rangeSplitConditions,
		...(opts.locationHint === undefined ? {} : { locationHint: opts.locationHint }),
		...(limits === undefined ? {} : { limits }),
		...(opts.captureChanges === true ? { captureChanges: true as const } : {}),
	};
	const policyVersion = opts.policyVersion ?? 0;
	validatePolicyVersion(policyVersion);
	return { topology, rangeConfig, policy, policyVersion };
}

function validateSplitConditions(
	name: string,
	conditions: SplitConditions,
	invalid: (option: string, value: unknown, message: string) => FokosValidationError,
): void {
	if (conditions.maxSizeMb && conditions.maxSizeMb <= 0) {
		throw invalid(`${name}.maxSizeMb`, conditions.maxSizeMb, `${name}.maxSizeMb must be greater than 0`);
	}
}
