import { DurableObject } from "cloudflare:workers";
import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import { FokosDB } from "./lib/db.js";
import { PartitionContextCreator, PartitionTopologyRouterImpl } from "./lib/partition-topology/partition-topology.js";

export { PartitionDO } from "./lib/do-partition.js";
export { TransactionCoordinatorDO } from "./lib/do-transaction-coordinator.js";

export class MyDurableObject extends DurableObject<Env> {
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
	}

	async sayHello(name: string): Promise<string> {
		return `Hello, ${name}!`;
	}
}

const topologyOptions = PartitionContextCreator.create({
	ns: "PARTITION_DO",
	databaseName: "fokos",
	rootTreesN: 10,
	hashSplitConditions: { splitN: 2, maxSizeMb: 256 },
	rangeSplitConditions: { splitN: 2, maxSizeMb: 256 },
});

const api = new Hono<{ Bindings: Env }>().basePath("/api");

api.onError((err, c) => {
	if (err instanceof HTTPException) {
		return err.getResponse();
	}
	console.error(err);
	return c.json({ error: "Internal Server Error" }, 500);
});

api.get("/hello/:name", async (c) => {
	const name = c.req.param("name");
	return c.json({ message: `Hello, ${name}!` });
});

export default {
	async fetch(request, env, ctx): Promise<Response> {
		return api.fetch(request, env, ctx);
	},
} satisfies ExportedHandler<Env>;
