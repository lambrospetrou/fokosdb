import { isFokosDBError, type FokosMeta } from "fokosdb/client";
import type { DemoNames } from "./shared.js";

/**
 * Workers Analytics Engine accepts at most 250 data points per invocation, 20 blobs, 20 doubles and
 * one index per point. Every point carries `pointType, workload, placement, instance, outcome` as its
 * first blobs, so a dashboard can select and decode the doubles by point type.
 */
const POINT_CAP = 250;

export class Metrics {
	#env: Env;
	#base: string[];
	#index: string[];
	#written = 0;
	#dropped = 0;

	constructor(env: Env, names: DemoNames) {
		this.#env = env;
		this.#base = [names.workload, names.placement, names.instance];
		this.#index = [`${names.placement}|${names.workload}|${names.instance}`];
	}

	point(pointType: string, doubles: number[], blobs: string[] = [], outcome = "ok"): void {
		if (this.#written >= POINT_CAP) {
			this.#dropped++;
			return;
		}
		this.#written++;
		this.#env.ANALYTICS.writeDataPoint({
			blobs: [pointType, ...this.#base, outcome, ...blobs],
			doubles,
			indexes: this.#index,
		});
	}

	get written(): number {
		return this.#written;
	}
	get dropped(): number {
		return this.#dropped;
	}
}

/** The routing data every FokosDB answer and error carries, reduced to what a point records. */
export type MetaStats = {
	partitions: number;
	forwards: number;
	/** The largest `databaseSize` over the partitions that answered, in bytes. -1 when none reported. */
	databaseSize: number;
	hashDepth: number;
	rangeDepth: number;
	actorNames: string[];
};

export function metaStats(meta: FokosMeta | undefined): MetaStats {
	const stats: MetaStats = { partitions: 0, forwards: 0, databaseSize: -1, hashDepth: -1, rangeDepth: -1, actorNames: [] };
	if (!meta) {
		return stats;
	}
	stats.partitions = meta.partitions.length;
	stats.forwards = meta.forwardCount;
	for (const p of meta.partitions) {
		if (p.databaseSize !== undefined) {
			stats.databaseSize = Math.max(stats.databaseSize, p.databaseSize);
		}
		stats.hashDepth = Math.max(stats.hashDepth, p.hashDepth ?? -1);
		stats.rangeDepth = Math.max(stats.rangeDepth, p.rangeDepth ?? -1);
		if (p.servedByActorName) {
			stats.actorNames.push(p.servedByActorName);
		}
	}
	if (meta.txCoordinator) {
		stats.partitions += 1;
		if (meta.txCoordinator.servedByActorName) {
			stats.actorNames.push(meta.txCoordinator.servedByActorName);
		}
	}
	return stats;
}

/** One measured FokosDB call: its wall time, its outcome code, and the routing data of its answer. */
export type OpCall<T> = {
	result?: T;
	/** `ok` or the FokosDB error code. A `transaction_cancelled` is a normal outcome of a write transaction. */
	outcome: string;
	ms: number;
	stats: MetaStats;
};

export async function measure<T extends { meta?: FokosMeta }>(fn: () => Promise<T>): Promise<OpCall<T>> {
	const start = Date.now();
	try {
		const result = await fn();
		return { result, outcome: "ok", ms: Date.now() - start, stats: metaStats(result.meta) };
	} catch (e) {
		const code = isFokosDBError(e) ? e.code : "unknown_error";
		const meta = isFokosDBError(e) ? e.meta : undefined;
		return { outcome: code, ms: Date.now() - start, stats: metaStats(meta) };
	}
}

export function allOk(calls: readonly OpCall<unknown>[]): string {
	for (const c of calls) {
		if (c.outcome !== "ok") {
			return c.outcome;
		}
	}
	return "ok";
}

export function maxDbSize(calls: readonly OpCall<unknown>[]): number {
	let size = -1;
	for (const c of calls) {
		size = Math.max(size, c.stats.databaseSize);
	}
	return size;
}

export function sumForwards(calls: readonly OpCall<unknown>[]): number {
	let forwards = 0;
	for (const c of calls) {
		forwards += c.stats.forwards;
	}
	return forwards;
}

/** The partitions a runner has seen serve an answer, persisted so the first-seen set survives restarts. */
export class PartitionTracker {
	static readonly #KEY = "seenPartitions";
	#seen = new Map<string, number>();
	#kv: SyncKvStorage;
	#loaded = false;

	constructor(kv: SyncKvStorage) {
		this.#kv = kv;
	}

	#load(): void {
		if (this.#loaded) {
			return;
		}
		this.#loaded = true;
		const stored = this.#kv.get<Record<string, number>>(PartitionTracker.#KEY);
		if (stored) {
			for (const [name, at] of Object.entries(stored)) {
				this.#seen.set(name, at);
			}
		}
	}

	/** True the first time `name` appears. */
	mark(name: string): boolean {
		this.#load();
		if (this.#seen.has(name)) {
			return false;
		}
		this.#seen.set(name, Date.now());
		return true;
	}

	save(): void {
		this.#load();
		this.#kv.put(PartitionTracker.#KEY, Object.fromEntries(this.#seen));
	}

	reset(): void {
		this.#seen.clear();
		this.#loaded = true;
		this.#kv.delete(PartitionTracker.#KEY);
	}
}

/** The colo of this isolate, like `fokosGetColoInfo` records. Best-effort: failures give empty fields. */
export async function fetchRunnerColo(): Promise<{ colo: string; loc: string }> {
	const text = await fetch("https://cloudflare.com/cdn-cgi/trace").then((r) => r.text());
	const fields: Record<string, string> = {};
	for (const line of text.split("\n")) {
		const i = line.indexOf("=");
		if (i > 0) {
			fields[line.slice(0, i)] = line.slice(i + 1);
		}
	}
	return { colo: fields.colo ?? "", loc: fields.loc ?? "" };
}
