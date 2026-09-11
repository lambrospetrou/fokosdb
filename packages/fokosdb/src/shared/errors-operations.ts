/**
 * The errors of the FokosDB operations: the categories whose fields hold types of the FokosDB data
 * model, and the union of every error the library raises.
 *
 * `errors.ts` holds the machinery and the generic categories, and imports nothing. This module adds
 * what depends on the types of the library, in the same way that another package extends `errors.ts`.
 */

import {
	CONFLICT_CODES,
	EXPRESSION_CODES,
	FOKOS_CODE_TABLES,
	FokosConflictError,
	FokosError,
	FokosExpressionError,
	FokosInternalError,
	FokosRoutingError,
	FokosTransactionPendingError,
	FokosUnavailableError,
	FokosValidationError,
	INTERNAL_CODES,
	ROUTING_CODES,
	TRANSACTION_PENDING_CODES,
	UNAVAILABLE_CODES,
	VALIDATION_CODES,
	defineCodes,
	defineErrorGuard,
	type FokosCodeDef,
	type FokosCodesOf,
	type FokosErrorOptions,
} from "./errors.js";
import type { ExpressionError } from "./expression/errors.js";
import type { ConditionFailedReason, TransactWriteOperationResult } from "./transaction-types.js";
import type { OperationMetrics, PartitionInfo } from "./types.js";

export const CONDITION_CHECK_CODES = defineCodes("FokosConditionCheckError", "caller", 409, {
	condition_failed: "usbs9w",
});

export const TRANSACTION_CANCELLED_CODES = defineCodes("FokosTransactionCancelledError", "caller", 409, {
	transaction_cancelled: "zd7rzd",
});

/** `putItem` and `deleteItem` raise it when their condition fails. */
export class FokosConditionCheckError<C extends string = string> extends FokosError<"FokosConditionCheckError", C> {
	static readonly tag = "FokosConditionCheckError";
	/**
	 * The same record that a rejected transaction result holds. `reason.item` is the old item image when
	 * the caller asked for one and the item exists.
	 */
	readonly reason: ConditionFailedReason;
	/** The metrics and the partition info of the request that evaluated the condition. */
	readonly meta: OperationMetrics & PartitionInfo;

	constructor(
		code: FokosCodeDef<"FokosConditionCheckError", C>,
		options: FokosErrorOptions & { reason: ConditionFailedReason; meta: OperationMetrics & PartitionInfo },
	) {
		super(code, options);
		this.reason = options.reason;
		this.meta = options.meta;
	}
}

/**
 * `transactWriteItems` raises it when the transaction cancelled. The transaction applied nothing.
 *
 * Its origin and hint come from the codes of the rejected entries, unless the call site passes others.
 * The first origin in the order `caller`, `internal`, `service` wins: a premise that must change comes
 * before a defect, and only a cancel whose every failure clears on its own is a service condition.
 */
export class FokosTransactionCancelledError<C extends string = string> extends FokosError<"FokosTransactionCancelledError", C> {
	static readonly tag = "FokosTransactionCancelledError";
	/**
	 * One entry for each operation, in request order. A rejected entry carries the reason of that
	 * operation: a premise that did not hold, or the error of the partition that owns it.
	 */
	readonly results: TransactWriteOperationResult[];

	constructor(
		code: FokosCodeDef<"FokosTransactionCancelledError", C>,
		options: FokosErrorOptions & { results: TransactWriteOperationResult[] },
	) {
		// The first rejected entry of each origin, in request order. A caller entry wins, so the loop stops at the first one.
		let caller: FokosCodeDef | undefined;
		let internal: FokosCodeDef | undefined;
		let service: FokosCodeDef | undefined;
		for (const r of options.results) {
			if (r.outcome !== "rejected") continue;
			const def = FOKOS_LIBRARY_CODES.get(r.reason.code);
			if (def?.origin === "caller") {
				caller = def;
				break;
			}
			if (def?.origin === "internal") internal ??= def;
			else if (def?.origin === "service") service ??= def;
		}
		const decisive = caller ?? internal ?? service;
		super(code, { origin: decisive?.origin, httpStatusHint: decisive?.httpStatusHint, ...options });
		this.results = options.results;
	}
}

/** Every error the library raises. `_tag` and `code` are literals here, so a switch narrows and stays exhaustive. */
export type FokosAnyError =
	| FokosValidationError<FokosCodesOf<typeof VALIDATION_CODES>>
	| FokosExpressionError<FokosCodesOf<typeof EXPRESSION_CODES>>
	| FokosConditionCheckError<FokosCodesOf<typeof CONDITION_CHECK_CODES>>
	| FokosConflictError<FokosCodesOf<typeof CONFLICT_CODES>>
	| FokosTransactionCancelledError<FokosCodesOf<typeof TRANSACTION_CANCELLED_CODES>>
	| FokosTransactionPendingError<FokosCodesOf<typeof TRANSACTION_PENDING_CODES>>
	| FokosUnavailableError<FokosCodesOf<typeof UNAVAILABLE_CODES>>
	| FokosRoutingError<FokosCodesOf<typeof ROUTING_CODES>>
	| FokosInternalError<FokosCodesOf<typeof INTERNAL_CODES>>;

export type FokosErrorCode = FokosAnyError["code"];

/** Every code table of the library. */
export const FOKOS_LIBRARY_CODE_TABLES = [...FOKOS_CODE_TABLES, CONDITION_CHECK_CODES, TRANSACTION_CANCELLED_CODES] as const;

/** Every code definition of the library by its code. */
const FOKOS_LIBRARY_CODES: ReadonlyMap<string, FokosCodeDef> = new Map(
	FOKOS_LIBRARY_CODE_TABLES.flatMap((table) => Object.values(table).map((def) => [def.code, def] as const)),
);

/** True for an error of `FokosAnyError`, after any number of hops. */
export const isFokosAnyError = defineErrorGuard<FokosAnyError>(...FOKOS_LIBRARY_CODE_TABLES);

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

/** The error that says the stored or reported answers of a transaction cannot be read back. */
export function unexpectedTransactionStateError(detail: string, attributes: Record<string, unknown> = {}): FokosInternalError {
	return new FokosInternalError(INTERNAL_CODES.unexpected_transaction_state, {
		message: "unexpected transaction state",
		attributes: { detail, ...attributes },
	});
}
