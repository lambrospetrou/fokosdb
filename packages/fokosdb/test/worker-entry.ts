/**
 * Entrypoint for the library test worker (root wrangler.jsonc).
 *
 * Durable Object classes must be exported from the worker `main` module, so this
 * file re-exports the library classes for the bindings the tests use. It is not
 * deployed; the fetch handler exists only because wrangler expects a default export.
 */
import { DurableObject } from "cloudflare:workers";
import { PartitionDO } from "../src/server/do-partition.js";
import { FOKOS_ERROR_CATEGORIES, FOKOS_ERROR_REGISTRY, FokosError, FokosInternalError, type FokosErrorCode } from "../src/shared/errors.js";

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

/**
 * Probe used by `test/tagged-error-rpc.test.ts`. It raises errors so the test can observe what a
 * Workers RPC hop keeps and what it drops. It stores nothing.
 */
export class ErrorProbeDO extends DurableObject<Env> {
	async raise(code: FokosErrorCode, attributes: Record<string, unknown>): Promise<never> {
		const Category = FOKOS_ERROR_CATEGORIES.get(FOKOS_ERROR_REGISTRY[code].tag)!;
		throw new Category({ code, message: "probe failed", attributes });
	}

	async raiseWithCause(): Promise<never> {
		throw new FokosInternalError({ code: "partition_fanout_failed", message: "outer", cause: new Error("inner") });
	}

	async raiseForeign(): Promise<never> {
		throw Object.assign(new Error("platform fault"), { retryable: true, overloaded: false });
	}

	/** A second hop: calls another probe and rethrows what it receives, the way a forwarding partition does. */
	async relay(target: string, code: FokosErrorCode): Promise<never> {
		try {
			await this.env.ERROR_PROBE_DO.getByName(target).raise(code, {});
		} catch (e) {
			throw FokosError.wrap(e);
		}
		throw new Error("the probe did not throw");
	}
}
