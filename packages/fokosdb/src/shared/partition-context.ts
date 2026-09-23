/**
 * The FokosDB half of the route context: the policy the two Durable Object classes read, and the
 * creator that validates a table configuration.
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
import type { FokosRangeConfig, FokosRouteContext, FokosTopology } from "../sharding/route-context.js";
import { validateRangeConfig, validateTopology } from "../sharding/route-context.js";
import { FokosValidationError, VALIDATION_CODES } from "./errors.js";

export type SplitConditions = {
	/** The size in megabytes that makes the partition split. */
	maxSizeMb?: number;
	/**
	 * The number of items that makes the partition split.
	 * FIXME: Nothing reads this value. Remove it, or make the split policy use it.
	 */
	maxItems?: number;
};

export type PartitionNamespaceKey = {
	[K in keyof Env]: Env[K] extends DurableObjectNamespace<PartitionDO> ? K : never;
}[keyof Env];

export type TransactionCoordinatorNamespaceKey = {
	[K in keyof Env]: Env[K] extends DurableObjectNamespace<TransactionCoordinatorDO> ? K : never;
}[keyof Env];

/**
 * What FokosDB needs on every partition and coordinator, beside the topology. Last writer wins,
 * except `ns` and `nsTx`: a change to those selects another Durable Object namespace, which a
 * partition inside the old namespace cannot detect, so a shard group must keep them for life.
 */
export type FokosDbPolicy = {
	ns: PartitionNamespaceKey;
	nsTx: TransactionCoordinatorNamespaceKey;
	/**
	 * The location hint for the Durable Objects of this table. It is best-effort placement advice for
	 * the first time each object spawns. It is not part of the identity of an object.
	 */
	locationHint?: DurableObjectLocationHint;
	hashSplitConditions: SplitConditions;
	rangeSplitConditions: SplitConditions;
};

export type FokosDbRouteContext = FokosRouteContext<FokosDbPolicy>;

/**
 * FokosDB names its own shard groups with this prefix, so a table name must not start with it. The
 * coordinator group of a table is `fokos.tc.<tableName>`.
 */
export const RESERVED_SHARD_GROUP_PREFIX = "fokos.";

/** The part of a route context that selects a namespace and a stub: enough for a Worker with no partition in mind. */
export type FokosDbStubContext = Pick<FokosDbRouteContext, "topology" | "policy">;

/** One table configuration: what `FokosRouter` is built from. */
export type FokosDbTableConfig = {
	topology: FokosTopology;
	rangeConfig: FokosRangeConfig;
	policy: FokosDbPolicy;
};

export class PartitionContextCreator {
	static create(opts: {
		ns: PartitionNamespaceKey;
		nsTx: TransactionCoordinatorNamespaceKey;
		tableName: string;
		rootTreesN: number;
		hashSplitN: number;
		hashSplitConditions: SplitConditions;
		rangeSplitN?: number;
		rangeSplitConditions?: SplitConditions;
		rangeAncestorsConfig?: { fromRoot: number; fromLeaf: number };
		jurisdiction?: DurableObjectJurisdiction;
		locationHint?: DurableObjectLocationHint;
	}): FokosDbTableConfig {
		// Assert the input options and default to reasonable values if not provided.
		if (!opts.rangeSplitConditions) {
			opts.rangeSplitN = 4;
			opts.rangeSplitConditions = { maxSizeMb: 500 };
		}
		if (!opts.hashSplitConditions) {
			opts.hashSplitN = 4;
			opts.hashSplitConditions = { maxSizeMb: 100 };
		}
		if (!opts.rangeAncestorsConfig) {
			opts.rangeAncestorsConfig = { fromRoot: 0, fromLeaf: 3 };
		}
		const invalid = (option: string, value: unknown, message: string) =>
			new FokosValidationError(VALIDATION_CODES.partition_context_options_invalid, { message, attributes: { option, value } });

		if (!opts.hashSplitN) {
			throw invalid("hashSplitN", opts.hashSplitN, "hashSplitN must be provided if hashSplitConditions is provided");
		}
		if (!opts.rangeSplitN) {
			throw invalid("rangeSplitN", opts.rangeSplitN, "rangeSplitN must be provided if rangeSplitConditions is provided");
		}

		const topology: FokosTopology = {
			shardGroup: opts.tableName,
			rootTreesN: opts.rootTreesN,
			hashSplitN: opts.hashSplitN,
			// A table that selects no jurisdiction stores a topology byte-identical to one built without
			// the option, so an existing record and a new one compare equal.
			...(opts.jurisdiction === undefined ? {} : { jurisdiction: opts.jurisdiction }),
		};
		validateTopology(topology);
		if (topology.shardGroup.startsWith(RESERVED_SHARD_GROUP_PREFIX)) {
			throw invalid("shardGroup", topology.shardGroup, `shardGroup must not start with "${RESERVED_SHARD_GROUP_PREFIX}"`);
		}

		const rangeConfig: FokosRangeConfig = { rangeSplitN: opts.rangeSplitN, rangeAncestors: opts.rangeAncestorsConfig };
		validateRangeConfig(rangeConfig);

		validateSplitConditions("hashSplitConditions", opts.hashSplitConditions, invalid);
		validateSplitConditions("rangeSplitConditions", opts.rangeSplitConditions, invalid);

		const policy: FokosDbPolicy = {
			ns: opts.ns,
			nsTx: opts.nsTx,
			hashSplitConditions: opts.hashSplitConditions,
			rangeSplitConditions: opts.rangeSplitConditions,
			...(opts.locationHint === undefined ? {} : { locationHint: opts.locationHint }),
		};
		return { topology, rangeConfig, policy };
	}
}

function validateSplitConditions(
	name: string,
	conditions: SplitConditions,
	invalid: (option: string, value: unknown, message: string) => FokosValidationError,
): void {
	if (conditions.maxSizeMb && conditions.maxSizeMb <= 0) {
		throw invalid(`${name}.maxSizeMb`, conditions.maxSizeMb, `${name}.maxSizeMb must be greater than 0`);
	}
	if (conditions.maxItems && conditions.maxItems < 1) {
		throw invalid(`${name}.maxItems`, conditions.maxItems, `${name}.maxItems must be at least 1`);
	}
}
