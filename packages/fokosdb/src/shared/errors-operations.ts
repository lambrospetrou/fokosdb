/**
 * The errors of the FokosDB operations: the errors with fields of the FokosDB data model, and the union
 * of every error the library raises.
 *
 * `errors.ts` holds the machinery and imports nothing. This module adds what depends on the types of
 * the library.
 */

import {
	CONFLICT_CODES,
	EXPRESSION_CODES,
	FOKOS_CODE_TABLES,
	FokosConditionCheckError,
	FokosConflictError,
	FokosExpressionError,
	FokosInternalError,
	FokosRoutingError,
	FokosTransactionCancelledError,
	FokosTransactionPendingError,
	FokosUnavailableError,
	FokosValidationError,
	INTERNAL_CODES,
	ROUTING_CODES,
	TRANSACTION_CANCELLED_CODES,
	TRANSACTION_PENDING_CODES,
	UNAVAILABLE_CODES,
	VALIDATION_CODES,
	defineErrorGuard,
	type FokosCodesOf,
	type FokosCodeDef,
	type FokosErrorOptions,
} from "./errors.js";
import type { ExpressionError } from "./expression/errors.js";
import type { ConditionFailedReason } from "./transaction-types.js";
import type { OperationMetrics, PartitionInfo } from "./types.js";

/** The `FokosConditionCheckError` that `putItem` and `deleteItem` raise. Its `name` and `_tag` are those of its category. */
export class FokosItemConditionCheckError extends FokosConditionCheckError<"condition_failed"> {
	/**
	 * The same record that a rejected transaction result holds. `reason.item` is the old item image when
	 * the caller asked for one and the item exists.
	 */
	readonly reason: ConditionFailedReason;
	/** The metrics and the partition info of the request that evaluated the condition. */
	readonly meta: OperationMetrics & PartitionInfo;

	constructor(
		code: FokosCodeDef<"FokosConditionCheckError", "condition_failed">,
		options: FokosErrorOptions & { reason: ConditionFailedReason; meta: OperationMetrics & PartitionInfo },
	) {
		super(code, options);
		this.reason = options.reason;
		this.meta = options.meta;
	}
}

/** Every error the library raises. `_tag` and `code` are literals here, so a switch narrows and stays exhaustive. */
export type FokosAnyError =
	| FokosValidationError<FokosCodesOf<typeof VALIDATION_CODES>>
	| FokosExpressionError<FokosCodesOf<typeof EXPRESSION_CODES>>
	| FokosItemConditionCheckError
	| FokosConflictError<FokosCodesOf<typeof CONFLICT_CODES>>
	| FokosTransactionCancelledError<FokosCodesOf<typeof TRANSACTION_CANCELLED_CODES>>
	| FokosTransactionPendingError<FokosCodesOf<typeof TRANSACTION_PENDING_CODES>>
	| FokosUnavailableError<FokosCodesOf<typeof UNAVAILABLE_CODES>>
	| FokosRoutingError<FokosCodesOf<typeof ROUTING_CODES>>
	| FokosInternalError<FokosCodesOf<typeof INTERNAL_CODES>>;

export type FokosErrorCode = FokosAnyError["code"];

/** True for an error of `FokosAnyError`, after any number of hops. */
export const isFokosAnyError = defineErrorGuard<FokosAnyError>(...FOKOS_CODE_TABLES);

/**
 * Runs `fn`, and raises an `ExpressionError` from it as a `FokosExpressionError`. The original error is
 * the `cause`, and its `ExpressionErrorCode` is `attributes.expressionCode`.
 */
export function withExpressionErrors<T>(fn: () => T): T {
	try {
		return fn();
	} catch (e) {
		const expressionError = e as Partial<ExpressionError> | null;
		if (expressionError?.name !== "ExpressionError") throw e;
		throw new FokosExpressionError(EXPRESSION_CODES.expression_invalid, {
			message: "expression is not valid",
			cause: e,
			attributes: { expressionCode: expressionError.code },
		});
	}
}
