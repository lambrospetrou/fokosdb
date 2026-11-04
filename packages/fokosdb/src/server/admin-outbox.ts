/**
 * The admin outbox of the partitions and the coordinators. Only the Durable Objects import it, so the
 * client does not load the topology event codec.
 */
import { env } from "cloudflare:workers";
import { topologyAdminAddress, topologyAdminStub, type DataNamespaceKey } from "../shared/do-stubs.js";
import type { FokosDBStubContext } from "../shared/partition-context.js";
import type { FokosAdminOutbox } from "../sharding/runtime-types.js";
import { topologyEventFromWire } from "../sharding/topology-encode.js";
import invariant from "../shared/invariant.js";

/**
 * The admin outbox of a partition or a coordinator of `ctx`, or undefined when the table has no `nsAdmin`.
 * `binding` is the namespace that holds the shard group of `ctx`.
 *
 * The outbox holds one event type now: the report of a completed repartition of this shard group.
 * - A `hash_split` goes to the admin of the shard group.
 * - A `key_promotion` goes to the admin of its routingHash, which stores the range root, and then
 *   to the admin of the shard group, which adds the routingHash to the promoted set.
 * - A `range_split` goes to the admin of its routingHash only.
 * Each keeper ignores a report that it has, so a retry that sends to both admins again is safe.
 */
export function adminOutboxFor(ctx: FokosDBStubContext, binding: DataNamespaceKey): FokosAdminOutbox | undefined {
	const address = topologyAdminAddress(env, ctx, binding);
	if (address === undefined) {
		return undefined;
	}
	return {
		send: async (entry) => {
			const event = topologyEventFromWire(entry.event);
			const slice = event.targets[0].slice;
			if (slice.kind !== "hash_child") {
				// `address` exists, so the table has `nsAdmin`.
				const routingHashAddress = topologyAdminAddress(env, ctx, binding, slice.routingHash);
				invariant(routingHashAddress !== undefined, "routingHashAddress must exist");
				await topologyAdminStub(env, routingHashAddress).registerRepartition(entry.event);
			}
			if (event.kind !== "range_split") {
				await topologyAdminStub(env, address).registerRepartition(entry.event);
			}
		},
	};
}
