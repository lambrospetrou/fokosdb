import { runRepro } from "./run.js";

export { ReproPartitionDO } from "./repro-partition-do.js";
export { TransactionCoordinatorDO } from "fokosdb/server";

export default {
	async fetch(request, env): Promise<Response> {
		if (request.method !== "POST" || new URL(request.url).pathname !== "/run") {
			return new Response("POST /run", { status: 404 });
		}
		try {
			const outcome = await runRepro(env);
			return Response.json(outcome, { status: outcome.status === "read_conflict" ? 200 : 409 });
		} catch (error) {
			console.error("TTL read repro failed before it could check the result", error);
			return Response.json({ status: "error", message: String(error) }, { status: 500 });
		}
	},
} satisfies ExportedHandler<Env>;
