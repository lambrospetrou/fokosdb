/**
 * The cached entrypoint that serves the encoded topology of the shard groups of FokosDB. The Worker
 * re-exports it from its main module and enables its cache. A caller reaches it through
 * `exports.FokosDBTopologyCache({ props }).fetch(...)`, with the admin address of one shard group as the
 * props. A range read uses the admin address of its routingHash. Workers Cache keys an entry also by
 * the props, so all the callers of one shard group share one entry for each path.
 */
import { WorkerEntrypoint } from "cloudflare:workers";
import { topologyAdminStub, type TopologyAdminAddress } from "../shared/do-stubs.js";
import { routingHashTopologyAdminName, NAMESPACE_FINGERPRINT_LENGTH, topologyAdminName } from "../shared/partition-context.js";
import { fokosTopologyEntrypointFetch } from "../sharding/topology-keeper.js";

/** `max-age` of a topology answer, in seconds. */
const TOPOLOGY_MAX_AGE_S = 10;
/** `stale-while-revalidate` of a topology answer, in seconds. */
const TOPOLOGY_STALE_WHILE_REVALIDATE_S = 600;
/** The length of `.rh.` and the 16 hex characters at the end of the admin name of a routingHash. */
const ROUTING_HASH_NAME_SUFFIX_LENGTH = 20;

export class FokosDBTopologyCache extends WorkerEntrypoint<Env, TopologyAdminAddress> {
	async fetch(request: Request): Promise<Response> {
		const address = this.ctx.props;
		return await fokosTopologyEntrypointFetch(request, {
			admin: (shardGroup, routingHash) => {
				// The props name one admin Durable Object. It holds the hash topology of one shard group, or
				// the range topology of one routingHash of one shard group.
				const fpEnd = address.name.length - (routingHash === undefined ? 0 : ROUTING_HASH_NAME_SUFFIX_LENGTH);
				const fp = address.name.slice(fpEnd - NAMESPACE_FINGERPRINT_LENGTH, fpEnd);
				const name =
					routingHash === undefined ? topologyAdminName(shardGroup, fp) : routingHashTopologyAdminName(shardGroup, fp, routingHash);
				if (name !== address.name) {
					throw new Error(`the topology that the request reads does not belong to the admin Durable Object ${address.name}`);
				}
				return topologyAdminStub(this.env, address);
			},
			maxAgeS: TOPOLOGY_MAX_AGE_S,
			staleWhileRevalidateS: TOPOLOGY_STALE_WHILE_REVALIDATE_S,
		});
	}
}
