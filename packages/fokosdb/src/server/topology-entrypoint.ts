/**
 * The cached entrypoint that serves the encoded topology of the shard groups of FokosDB. The Worker
 * re-exports it from its main module and enables its cache. A caller reaches it through
 * `exports.FokosTopologyCache({ props }).fetch(...)`, with the admin address of its table as the
 * props. Workers Cache keys an entry also by the props, so all the callers of one table share one
 * entry for each path.
 */
import { WorkerEntrypoint } from "cloudflare:workers";
import { topologyAdminStub, type TopologyAdminAddress } from "../shared/do-stubs.js";
import { fokosTopologyEntrypointFetch } from "../sharding/topology-keeper.js";

/** `max-age` of a topology answer, in seconds. */
const TOPOLOGY_MAX_AGE_S = 10;
/** `stale-while-revalidate` of a topology answer, in seconds. */
const TOPOLOGY_STALE_WHILE_REVALIDATE_S = 600;

export class FokosTopologyCache extends WorkerEntrypoint<Env, TopologyAdminAddress> {
	async fetch(request: Request): Promise<Response> {
		const address = this.ctx.props;
		return await fokosTopologyEntrypointFetch(request, {
			admin: (shardGroup) => topologyAdminStub(this.env, address, shardGroup),
			maxAgeS: TOPOLOGY_MAX_AGE_S,
			staleWhileRevalidateS: TOPOLOGY_STALE_WHILE_REVALIDATE_S,
		});
	}
}
