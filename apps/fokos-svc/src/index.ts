import { DurableObject } from "cloudflare:workers";
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
	nsPrefix: "fokos",
	rootTreesN: 10,
	hashSplitConditions: { splitN: 2, maxSizeMb: 256 },
	rangeSplitConditions: { splitN: 2, maxSizeMb: 256 },
});

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const db = new FokosDB({
			ns: env.PARTITION_DO,
			topology: new PartitionTopologyRouterImpl("encoded-topology", topologyOptions),
			transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
		});

		return new Response("Hello, world!");
	},
} satisfies ExportedHandler<Env>;
