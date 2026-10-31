/**
 * The cached entrypoint that serves the encoded topology of the shard groups of FokosDB. The Worker
 * re-exports it from its main module and enables its cache. A caller reaches it through
 * `exports.FokosDBTopologyCache({ props }).fetch(...)`, with the admin address of one shard group as the
 * props. Workers Cache keys an entry also by the props, so all the callers of one shard group share one
 * entry for each path.
 */
import { WorkerEntrypoint } from "cloudflare:workers";
import { topologyAdminStub, type TopologyAdminAddress } from "../shared/do-stubs.js";
import { NAMESPACE_FINGERPRINT_LENGTH, topologyAdminName } from "../shared/partition-context.js";
import { fokosTopologyEntrypointFetch } from "../sharding/topology-keeper.js";

/** `max-age` of a topology answer, in seconds. */
const TOPOLOGY_MAX_AGE_S = 10;
/** `stale-while-revalidate` of a topology answer, in seconds. */
const TOPOLOGY_STALE_WHILE_REVALIDATE_S = 600;

export class FokosDBTopologyCache extends WorkerEntrypoint<Env, TopologyAdminAddress> {
	async fetch(request: Request): Promise<Response> {
		const address = this.ctx.props;
		return await fokosTopologyEntrypointFetch(request, {
			admin: (shardGroup) => {
				// The props name one admin Durable Object, and it holds the topology of one shard group only.
				if (topologyAdminName(shardGroup, address.name.slice(-NAMESPACE_FINGERPRINT_LENGTH)) !== address.name) {
					throw new Error(`the shard group ${shardGroup} does not belong to the admin Durable Object ${address.name}`);
				}
				return topologyAdminStub(this.env, address);
			},
			maxAgeS: TOPOLOGY_MAX_AGE_S,
			staleWhileRevalidateS: TOPOLOGY_STALE_WHILE_REVALIDATE_S,
		});
	}
}
