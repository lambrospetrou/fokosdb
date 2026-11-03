import type { FokosDB, FokosMeta } from "fokosdb/client";
import type { Metrics, OpCall } from "../metrics.js";
import type { DemoNames } from "../shared.js";
import { batch } from "./batch.js";
import { crud } from "./crud.js";
import { query } from "./query.js";
import { ttl } from "./ttl.js";
import { tx } from "./tx.js";

/** What one `tick` call tunes: the work volume and the wall-clock cap of a run. */
export type WorkloadParams = {
	/** The iterations the run tries to do before `budgetMs` ends. */
	iterations: number;
	/** The lanes that run iterations at the same time. At most 6: a caller has ~6 outgoing calls that can wait. */
	concurrency: number;
	/** The wall-clock cap of the run. The run stops starting iterations after it. */
	budgetMs: number;
};

/** The ceiling of `budgetMs`: a cron run every minute needs the buffer to the next run. */
export const MAX_BUDGET_MS = 45_000;

/** What a workload gets to run with. `op` is the only way a workload calls FokosDB: it times and meters. */
export type RunScope = {
	db: FokosDB;
	names: DemoNames;
	params: WorkloadParams;
	metrics: Metrics;
	kv: SyncKvStorage;
	/** Runs one measured FokosDB call: times it, meters it, and reports first-seen partitions. */
	op<T extends { meta?: FokosMeta }>(fn: () => Promise<T>): Promise<OpCall<T>>;
	/** Emits an `iter` point: one iteration's latencies and routing data. */
	iter(doubles: number[], outcome?: string, blobs?: string[]): void;
	/** Emits an event point, for example a first-seen partition or a TTL deletion. */
	event(kind: string, doubles: number[], blobs?: string[]): void;
	/** Runs `fn` for `i = 0..iterations` over `concurrency` lanes, stopping when the budget ends. */
	runIterations(fn: (i: number) => Promise<void>, overrides?: { iterations?: number; concurrency?: number }): Promise<number>;
};

export type Workload = {
	/** The params a `tick` uses when the request names none. */
	defaults: WorkloadParams;
	/** Seeds the workload's table. Runs once per seeded flag: `tick` calls it lazily when the flag is absent. */
	setup?(s: RunScope): Promise<{ seededItems?: number } | void>;
	/** One run of the workload. Returns the iterations it completed inside the budget. */
	tick(s: RunScope): Promise<{ iterationsDone: number }>;
};

export const WORKLOADS: Record<string, Workload> = { crud, tx, query, batch, ttl };

export function isWorkloadName(name: string): name is keyof typeof WORKLOADS {
	return Object.hasOwn(WORKLOADS, name);
}

export function resolveParams(defaults: WorkloadParams, params: Partial<WorkloadParams> | undefined): WorkloadParams {
	const int = (v: number | undefined, min: number, max: number, fallback: number): number => {
		if (v === undefined) {
			return fallback;
		}
		if (!Number.isInteger(v)) {
			return fallback;
		}
		return Math.min(Math.max(v, min), max);
	};
	return {
		iterations: int(params?.iterations, 1, 100_000, defaults.iterations),
		concurrency: int(params?.concurrency, 1, 6, defaults.concurrency),
		budgetMs: int(params?.budgetMs, 1_000, MAX_BUDGET_MS, defaults.budgetMs),
	};
}
