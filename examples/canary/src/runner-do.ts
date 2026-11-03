import { DurableObject } from "cloudflare:workers";
import { Metrics, PartitionTracker, fetchRunnerColo, measure } from "./metrics.js";
import { makeFokosDB, parseRunnerName, type DemoNames } from "./shared.js";
import { WORKLOADS, isWorkloadName, resolveParams, type RunScope, type Workload, type WorkloadParams } from "./workloads/index.js";

/** The argument of the three phase RPCs. `workload` must equal the one the object name encodes. */
export type PhaseRequest = {
	workload?: unknown;
	params?: Partial<WorkloadParams>;
};

/** The result of a phase RPC: `ok:false` with `reason:"busy"` while another phase runs. */
export type PhaseResult = {
	ok: boolean;
	workload: string;
	reason?: string;
	alreadySeeded?: boolean;
	seededItems?: number;
	iterationsDone?: number;
	ops?: number;
	errors?: number;
	elapsedMs?: number;
	droppedPoints?: number;
};

async function runIterative(params: WorkloadParams, fn: (i: number) => Promise<void>): Promise<number> {
	const deadline = Date.now() + params.budgetMs;
	let next = 0;
	let done = 0;
	await Promise.all(
		Array.from({ length: params.concurrency }, async () => {
			for (;;) {
				const i = next++;
				if (i >= params.iterations || Date.now() >= deadline) {
					return;
				}
				await fn(i);
				done += 1;
			}
		}),
	);
	return done;
}

/**
 * The workload runner: one Durable Object per `wl:<workload>:<placement>:<instance>` name, placed in
 * the placement the name encodes. `setup` seeds the table of the workload once, `tick` runs one
 * metered run (seeding lazily when needed), and `cleanup` destroys the table. Every measured call
 * emits Workers Analytics Engine points from inside this object.
 */
export class WorkloadRunnerDO extends DurableObject<Env> {
	#names: DemoNames;
	#bootedAt = Date.now();
	#busy = false;
	#tracker?: PartitionTracker;
	#colo = "";
	#loc = "";

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		const names = parseRunnerName(ctx.id.name ?? "");
		if (!names) {
			throw new Error(`the runner object needs a name of the form wl:<workload>:<placement>:<instance>, got ${ctx.id.name}`);
		}
		this.#names = names;
		// Best-effort, non-blocking: record the colo this isolate lives in for placement verification.
		setTimeout(() => {
			void fetchRunnerColo()
				.then(({ colo, loc }) => {
					this.#colo = colo;
					this.#loc = loc;
				})
				.catch(() => {});
		}, 0);
	}

	async setup(req: PhaseRequest): Promise<PhaseResult> {
		const { workload, name } = this.#resolve(req);
		if (this.#busy) {
			return { ok: false, workload: name, reason: "busy" };
		}
		this.#busy = true;
		try {
			const { scope } = this.#scope(workload, req.params);
			const seeded = await this.#ensureSeeded(workload, scope);
			this.#tracker?.save();
			return { ok: true, workload: name, alreadySeeded: seeded.already, seededItems: seeded.items };
		} finally {
			this.#busy = false;
		}
	}

	async tick(req: PhaseRequest): Promise<PhaseResult> {
		const { workload, name } = this.#resolve(req);
		if (this.#busy) {
			new Metrics(this.env, this.#names).point("busy", []);
			return { ok: false, workload: name, reason: "busy" };
		}
		this.#busy = true;
		const metrics = new Metrics(this.env, this.#names);
		try {
			metrics.point("runner", [Date.now() - this.#bootedAt], [this.#colo, this.#loc]);
			const { scope, counts } = this.#scope(workload, req.params, metrics);
			await this.#ensureSeeded(workload, scope);
			const start = Date.now();
			const { iterationsDone } = await workload.tick(scope);
			const elapsedMs = Date.now() - start;
			this.#tracker?.save();
			metrics.point("tick", [iterationsDone, elapsedMs, counts.ops, counts.errors, metrics.dropped]);
			return {
				ok: true,
				workload: name,
				iterationsDone,
				ops: counts.ops,
				errors: counts.errors,
				elapsedMs,
				droppedPoints: metrics.dropped,
			};
		} finally {
			this.#busy = false;
		}
	}

	async cleanup(req: PhaseRequest): Promise<PhaseResult> {
		const { name } = this.#resolve(req);
		if (this.#busy) {
			return { ok: false, workload: name, reason: "busy" };
		}
		this.#busy = true;
		const metrics = new Metrics(this.env, this.#names);
		try {
			const start = Date.now();
			await makeFokosDB(this.#names).destroy();
			this.ctx.storage.kv.delete(`seeded:${name}`);
			this.#tracker?.reset();
			metrics.point("cleanup", [Date.now() - start]);
			return { ok: true, workload: name, elapsedMs: Date.now() - start };
		} finally {
			this.#busy = false;
		}
	}

	/** The workload the request names, checked against the one the object name encodes. */
	#resolve(req: PhaseRequest): { workload: Workload; name: string } {
		const name = req?.workload;
		if (typeof name !== "string" || !isWorkloadName(name)) {
			throw new Error(`unknown workload: ${String(name)}`);
		}
		if (name !== this.#names.workload) {
			throw new Error(`workload mismatch: ${this.#names.doName} runs ${this.#names.workload}, not ${name}`);
		}
		return { workload: WORKLOADS[name], name };
	}

	#scope(
		workload: Workload,
		params: Partial<WorkloadParams> | undefined,
		metrics?: Metrics,
	): { scope: RunScope; counts: { ops: number; errors: number } } {
		metrics ??= new Metrics(this.env, this.#names);
		this.#tracker ??= new PartitionTracker(this.ctx.storage.kv);
		const tracker = this.#tracker;
		const resolved = resolveParams(workload.defaults, params);
		const counts = { ops: 0, errors: 0 };
		const scope: RunScope = {
			db: makeFokosDB(this.#names),
			names: this.#names,
			params: resolved,
			kv: this.ctx.storage.kv,
			metrics,
			op: async (fn) => {
				counts.ops++;
				const call = await measure(fn);
				if (call.outcome !== "ok") {
					counts.errors++;
				}
				for (const actor of call.stats.actorNames) {
					if (tracker.mark(actor)) {
						metrics.point("cold", [call.ms, call.stats.databaseSize], [actor]);
					}
				}
				return call;
			},
			iter: (doubles, outcome = "ok", blobs = []) => metrics.point("iter", doubles, blobs, outcome),
			event: (kind, doubles, blobs = []) => metrics.point(kind, doubles, blobs),
			runIterations: (fn, overrides) => runIterative({ ...resolved, ...overrides }, fn),
		};
		return { scope, counts };
	}

	/** Seeds the workload when its flag is absent; `tick` calls this lazily on every run. */
	async #ensureSeeded(workload: Workload, scope: RunScope): Promise<{ already: boolean; items: number }> {
		const key = `seeded:${this.#names.workload}`;
		if (this.ctx.storage.kv.get<number>(key) !== undefined) {
			return { already: true, items: 0 };
		}
		const start = Date.now();
		const seeded = (await workload.setup?.(scope)) ?? {};
		this.ctx.storage.kv.put(key, Date.now());
		scope.metrics.point("seed", [seeded.seededItems ?? 0, Date.now() - start]);
		return { already: false, items: seeded.seededItems ?? 0 };
	}
}
