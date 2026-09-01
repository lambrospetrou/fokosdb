/**
 * Entrypoint for the library test worker (root wrangler.jsonc).
 *
 * Durable Object classes must be exported from the worker `main` module, so this
 * file re-exports the library classes for the bindings the tests use. It is not
 * deployed; the fetch handler exists only because wrangler expects a default export.
 */
import { PartitionDO } from "../src/server/do-partition.js";

export { PartitionDO } from "../src/server/do-partition.js";
export { TransactionCoordinatorDO } from "../src/server/do-transaction-coordinator.js";

// db.test.ts runs its whole suite over CUSTOM_PARTITION_DO as well, so that a
// subclassed PartitionDO stays covered.
export class CustomPartitionDO extends PartitionDO {}

export default {
	async fetch(): Promise<Response> {
		return new Response("fokos library test worker", { status: 404 });
	},
} satisfies ExportedHandler<Env>;
