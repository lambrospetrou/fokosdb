/**
 * Entrypoint for the library test worker (root wrangler.jsonc).
 *
 * Durable Object classes must be exported from the worker `main` module, so this
 * file re-exports the library classes for the bindings the tests use. It is not
 * deployed; the fetch handler exists only because wrangler expects a default export.
 */
import { DurableObject } from "cloudflare:workers";
import { PartitionDO } from "../src/server/do-partition.js";

/**
 * A minimal tagged error with the shape the error handling design uses: the tag and every field are
 * own properties, assigned in the constructor, and the only prototype member holds no data.
 */
export class ProbeError extends Error {
	static readonly tag = "ProbeError";
	readonly _tag: string;
	readonly code: string;
	readonly errorId: string;

	constructor(fields: { message: string; code: string; errorId: string; cause?: unknown }) {
		super(fields.message, fields.cause === undefined ? undefined : { cause: fields.cause });
		Object.setPrototypeOf(this, new.target.prototype);
		this.name = ProbeError.tag;
		this._tag = ProbeError.tag;
		this.code = fields.code;
		this.errorId = fields.errorId;
	}

	toWire(): { name: string; code: string; errorId: string } {
		return { name: this.name, code: this.code, errorId: this.errorId };
	}
}

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
 * Probe used by `test/tagged-error-rpc.test.ts`. It raises a `TaggedError` so the test can observe
 * what a Workers RPC hop keeps and what it drops. It stores nothing.
 */
export class ErrorProbeDO extends DurableObject {
	async raiseTagged(): Promise<never> {
		throw new ProbeError({ message: "probe failed", code: "probe_code", errorId: "e_abc123_deadbeef" });
	}

	async raiseWithNestedCause(): Promise<never> {
		throw new ProbeError({ message: "outer", code: "outer_code", errorId: "e_zzz999_cafe", cause: new Error("inner") });
	}
}
