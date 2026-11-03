import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import type { Context } from "hono";
import * as v from "valibot";
import { ALL_PLACEMENTS, demoNames, isPlacement, runnerStub } from "./shared.js";
import { runTickSweep } from "./sweep.js";
import { WORKLOADS, isWorkloadName } from "./workloads/index.js";

// Wrangler resolves Durable Object bindings against this module, so the classes must be re-exported
// from the worker entry even though the implementations live in the library and in ./demo-dos.js.
export { FokosDBTopologyCache } from "fokosdb/server";
export { DemoAutoAdminDO, DemoPartitionDO, DemoTransactionCoordinatorDO } from "./demo-dos.js";
export { WorkloadRunnerDO } from "./runner-do.js";

const api = new Hono<{ Bindings: Env }>();

const PhaseBodySchema = v.object({
	workload: v.string(),
	placement: v.string(),
	instance: v.optional(v.string()),
	params: v.optional(
		v.object({
			iterations: v.optional(v.number()),
			concurrency: v.optional(v.number()),
			budgetMs: v.optional(v.number()),
		}),
	),
});

const SweepBodySchema = v.object({
	placements: v.optional(v.array(v.string())),
	workloads: v.optional(v.array(v.string())),
	instance: v.optional(v.string()),
});

const PHASES = ["setup", "tick", "cleanup"] as const;
type Phase = (typeof PHASES)[number];

api.onError((err, c) => {
	if (err instanceof HTTPException) {
		return err.getResponse();
	}
	console.error({ message: "Unexpected error in catch-all", error: String(err), errorProps: err });
	return c.json({ error: "Internal Server Error" }, 500);
});

const assertToken = (c: Context<{ Bindings: Env }>): void => {
	if (c.env.DEMO_API_TOKEN && c.req.header("x-fokos-secret-token") !== c.env.DEMO_API_TOKEN) {
		throw new HTTPException(401, { message: "Invalid token" });
	}
};

api.get("/", (c) => c.json({ phases: PHASES, sweep: "POST /sweep", workloads: Object.keys(WORKLOADS), placements: ALL_PLACEMENTS }));

api.post("/sweep", async (c) => {
	assertToken(c);
	const parsed = v.safeParse(SweepBodySchema, await c.req.json().catch(() => ({})));
	if (!parsed.success) {
		throw new HTTPException(400, { message: JSON.stringify({ error: "Validation failed", issues: v.flatten(parsed.issues) }) });
	}
	const bad = [
		...(parsed.output.placements ?? []).filter((p) => !isPlacement(p)).map((p) => `placement:${p}`),
		...(parsed.output.workloads ?? []).filter((w) => !isWorkloadName(w)).map((w) => `workload:${w}`),
	];
	if (bad.length > 0) {
		throw new HTTPException(400, { message: `unknown: ${bad.join(", ")}` });
	}
	const results = await runTickSweep(c.env, parsed.output);
	return c.json({ ok: results.every((r) => r.ok), results });
});

api.post("/:phase", async (c) => {
	const phase = c.req.param("phase") as Phase;
	if (!PHASES.includes(phase)) {
		throw new HTTPException(404, { message: `unknown phase: ${phase}` });
	}
	assertToken(c);
	const parsed = v.safeParse(PhaseBodySchema, await c.req.json());
	if (!parsed.success) {
		throw new HTTPException(400, { message: JSON.stringify({ error: "Validation failed", issues: v.flatten(parsed.issues) }) });
	}
	const body = parsed.output;
	if (!isWorkloadName(body.workload)) {
		throw new HTTPException(400, { message: `unknown workload: ${body.workload}` });
	}
	if (!isPlacement(body.placement)) {
		throw new HTTPException(400, { message: `unknown placement: ${body.placement}` });
	}
	const names = demoNames(body.workload, body.placement, body.instance ?? "0");
	const result = await runnerStub(c.env, names)[phase]({ workload: body.workload, params: body.params });
	return c.json(result);
});

export default {
	fetch: api.fetch,
	async scheduled(_event, env, _ctx): Promise<void> {
		for (const run of await runTickSweep(env)) {
			if (!run.ok) {
				console.warn({ message: "tick sweep run failed", ...run });
			}
		}
	},
} satisfies ExportedHandler<Env>;
