import { demoNames, isPlacement, runnerStub } from "./shared.js";
import { isWorkloadName } from "./workloads/index.js";

/** What a sweep runs. Each field defaults to the matching env var the cron uses. */
export type SweepOptions = {
	placements?: readonly string[];
	workloads?: readonly string[];
	instance?: string;
};

/** The outcome of one (workload, placement) run: `reason` for a runner answer, `error` for a thrown RPC. */
export type SweepRun = {
	workload: string;
	placement: string;
	ok: boolean;
	reason?: string;
	error?: string;
};

/**
 * The fan-out the cron does: one `tick` per (workload, placement) pair, all in parallel.
 * A run that throws becomes `{ok: false, error}` instead of rejecting the sweep.
 */
export async function runTickSweep(env: Env, opts: SweepOptions = {}): Promise<SweepRun[]> {
	const placements = (opts.placements ?? env.DEMO_PLACEMENTS.split(",")).map((p) => p.trim()).filter(isPlacement);
	const workloads = (opts.workloads ?? env.DEMO_CRON_WORKLOADS.split(",")).map((w) => w.trim()).filter(isWorkloadName);
	const instance = opts.instance?.trim() || env.DEMO_CRON_INSTANCE?.trim() || "0";
	return Promise.all(
		placements.flatMap((placement) =>
			workloads.map(async (workload): Promise<SweepRun> => {
				try {
					const result = await runnerStub(env, demoNames(workload, placement, instance)).tick({ workload });
					return { workload, placement, ok: result.ok, ...(result.ok ? {} : { reason: result.reason ?? "unknown" }) };
				} catch (error) {
					return { workload, placement, ok: false, error: String(error) };
				}
			}),
		),
	);
}
