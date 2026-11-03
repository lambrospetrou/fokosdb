import { FokosDB } from "fokosdb/client";
import type { WorkloadRunnerDO } from "./runner-do.js";

/** The Durable Object location hints the demo places runner objects in. */
export const LOCATION_HINTS = ["wnam", "enam", "weur", "eeur", "apac", "apac-ne", "apac-se", "oc"] as const;
/** The Durable Object jurisdictions the demo places runner objects in. `fedramp` needs an entitled account. */
export const JURISDICTIONS = ["eu", "us"] as const;

export type Placement = (typeof LOCATION_HINTS)[number] | (typeof JURISDICTIONS)[number];

export const ALL_PLACEMENTS: readonly Placement[] = [...LOCATION_HINTS, ...JURISDICTIONS];

/** A jurisdiction run also hints inside its region, so its objects spawn predictably within it. */
const JURISDICTION_HINT: Record<(typeof JURISDICTIONS)[number], DurableObjectLocationHint> = { eu: "weur", us: "enam" };

export function isJurisdiction(placement: string): placement is (typeof JURISDICTIONS)[number] {
	return (JURISDICTIONS as readonly string[]).includes(placement);
}

export function isPlacement(placement: string): placement is Placement {
	return (ALL_PLACEMENTS as readonly string[]).includes(placement);
}

/** The immutable table values. A change to either makes the existing partitions reject every request. */
export const ROOT_TREES_N = 4;
export const HASH_SPLIT_N = 4;

/**
 * The names that identify one runner Durable Object and its table:
 * the object is `wl:<workload>:<placement>:<instance>` and the table is `canary-<workload>-<placement>-<instance>`.
 */
export type DemoNames = {
	workload: string;
	placement: Placement;
	instance: string;
	jurisdiction?: DurableObjectJurisdiction;
	locationHint: DurableObjectLocationHint;
	doName: string;
	table: string;
};

export function demoNames(workload: string, placement: Placement, instance: string): DemoNames {
	const jurisdiction = isJurisdiction(placement) ? placement : undefined;
	return {
		workload,
		placement,
		instance,
		...(jurisdiction ? { jurisdiction } : {}),
		locationHint: isJurisdiction(placement) ? JURISDICTION_HINT[placement] : placement,
		doName: `wl:${workload}:${placement}:${instance}`,
		table: `canary-${workload}-${placement}-${instance}`,
	};
}

/** Parses a runner Durable Object name back into its parts, or null when the name is not a runner name. */
export function parseRunnerName(doName: string): DemoNames | null {
	const [tag, workload, placement, instance, ...rest] = doName.split(":");
	if (tag !== "wl" || !workload || !placement || !instance || rest.length > 0 || !isPlacement(placement)) {
		return null;
	}
	return demoNames(workload, placement, instance);
}

/**
 * The stub of the runner Durable Object `names` names. The object spawns in `names.placement` on its
 * first `get`: a jurisdiction run resolves the jurisdictional namespace, a hint run passes the hint.
 */
export function runnerStub(env: Env, names: DemoNames): DurableObjectStub<WorkloadRunnerDO> {
	const ns = names.jurisdiction ? env.WORKLOAD_RUNNER_DO.jurisdiction(names.jurisdiction) : env.WORKLOAD_RUNNER_DO;
	return ns.get(ns.idFromName(names.doName), { locationHint: names.locationHint });
}

/** The FokosDB client of the runner's own table. `env` inside a Durable Object is the DO's own `env`. */
export function makeFokosDB(names: DemoNames): FokosDB {
	return new FokosDB({
		table: {
			name: names.table,
			ns: "DEMO_PARTITION_DO",
			nsTx: "DEMO_TX_DO",
			rootTreesN: ROOT_TREES_N,
			hashSplitN: HASH_SPLIT_N,
			...(names.jurisdiction ? { jurisdiction: names.jurisdiction } : {}),
		},
		locationHint: names.locationHint,
		nsAdmin: "FOKOS_ADMIN_DO",
		topologyEntrypoint: "FokosDBTopologyCache",
	});
}
