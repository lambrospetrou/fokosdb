import { GetItemOptions, PutItemOptions } from "./types.js";
import { PartitionDO } from "./do-partition.js";
import { PartitionTopologyRouter } from "./partition-topology.js";

export type FokosDBOptions = {
	ns: DurableObjectNamespace<PartitionDO>;

	topology: PartitionTopologyRouter;

	// TODO Add location hints.
};

export class FokosDB {
	constructor(public readonly options: FokosDBOptions) {}

	async putItem(opts: PutItemOptions) {
		const { doId, partitionContext } = this.options.topology.pickPartition(opts.hashKey, opts.sortKey);
		const ns = this.options.ns;
		const stub = ns.get(doId);
		return await stub.putItem(partitionContext, opts);
	}

	async getItem(opts: GetItemOptions) {
		const { doId, partitionContext } = this.options.topology.pickPartition(opts.hashKey, opts.sortKey);
		const ns = this.options.ns;
		const stub = ns.get(doId);
		return await stub.getItem(partitionContext, opts);
	}
}
