/**
 * The outcome of the items that one error stopped, in a batch operation. A partition and `db.ts` both
 * classify with this function, so an error gets the same outcome wherever it lands.
 */
import { routedError } from "../sharding/envelope.js";
import type { BatchUnprocessedCode } from "./batch-api-types.js";
import { FokosError, FokosExpressionError, FokosInternalError, FokosValidationError, VALIDATION_CODES } from "./errors.js";
import { WORKERS_RPC_MAX_BYTES } from "../sharding/rpc-limits.js";

export type BatchFailureOutcome = "rejected" | "unprocessed" | "unknown";

export type BatchFailureClass = { outcome: BatchFailureOutcome; code: BatchUnprocessedCode; error_id: string };

/**
 * The start of the message of the error that workerd raises for an RPC value above
 * `WORKERS_RPC_MAX_BYTES`. The text is the text of workerd. The full message continues with the size
 * of the value.
 */
const RPC_TOO_LARGE_PREFIX = "Serialized RPC arguments or return values are limited to 32MiB";

/**
 * The outcome, the code, and the `error_id` of `error`. `write` is true for a batch write. `local`
 * is true for an error of the local part of this partition. The rules apply in this order:
 *
 * 1. An RPC value above `WORKERS_RPC_MAX_BYTES` is `rejected` with `batch_request_too_large`. In a batch, that value is
 *    the arguments of a request, which the caller serializes, so the request never left the caller.
 * 2. A validation or an expression error is `rejected`: the same item fails the same way again.
 * 3. An error of the local part is `unprocessed`. A local batch handler throws only before its first
 *    write, and a write block that throws rolls back its own writes.
 * 4. A `FokosError` with routing is `unprocessed`: partition code raised it before any handler ran on
 *    that part. For a write, a `FokosInternalError` is the exception, because runtime code can raise
 *    one after a handler wrote.
 * 5. Any other error is `unknown` for a write, and `unprocessed` for a read, which applies nothing.
 *
 * It never throws.
 */
export function classifyBatchFailure(error: unknown, opts: { write: boolean; local: boolean }): BatchFailureClass {
	if (isRpcTooLarge(error)) {
		const err = new FokosValidationError(VALIDATION_CODES.batch_request_too_large, {
			message: `the batch request is above the RPC limit of ${WORKERS_RPC_MAX_BYTES / (1024 * 1024)} MiB, send its items in smaller batches`,
			cause: error,
		});
		return { outcome: "rejected", code: err.code, error_id: err.error_id };
	}
	const err = FokosError.wrap(error);
	const code = err.code as BatchUnprocessedCode;
	if (FokosValidationError.is(err) || FokosExpressionError.is(err)) {
		return { outcome: "rejected", code, error_id: err.error_id };
	}
	// Rule 3, and rules 4 and 5 for a read.
	if (opts.local || !opts.write) {
		return { outcome: "unprocessed", code, error_id: err.error_id };
	}
	if (routedError(err) && !FokosInternalError.is(err)) {
		return { outcome: "unprocessed", code, error_id: err.error_id };
	}
	return { outcome: "unknown", code, error_id: err.error_id };
}

/** True for the error of workerd, or for an error that has it as its cause. */
function isRpcTooLarge(error: unknown): boolean {
	const cause = error instanceof Error ? error.cause : undefined;
	return [error, cause].some((e) => e instanceof Error && e.message.startsWith(RPC_TOO_LARGE_PREFIX));
}
