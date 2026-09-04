import type { JsonPrimitive } from "../json-types.js";
import type { ExpressionRequiredColumn } from "./semantic.js";

export const CONDITION_PLAN_VERSION = 1 as const;
export const CONDITION_FIXED_BINDING_COUNT = 2;

export const UPDATE_PLAN_VERSION = 1 as const;

/**
 * Parameters every statement that runs an update plan binds BEFORE the plan's own, in this order:
 * the hash key and the sort key. The compiler offsets the plan's parameters past them, exactly as it
 * does for a condition, so every such statement shares one numbering and one binding order.
 */
export const UPDATE_FIXED_BINDING_COUNT = 2;

/**
 * The widest statement-local tail any statement appends AFTER an update plan's parameters — today
 * `PartitionStore.insertPendingUpdateLock`, with the transaction id, its timestamp, the created-at
 * stamp, the coordinator id, the condition JSON, and the TTL of an operation that sets one.
 *
 * The compiler charges it to every plan, because one plan is embedded by every statement and its
 * parameter numbering is fixed when it compiles. Raising a statement's tail therefore lowers the
 * budget for every update expression, which is why the number lives here and not at the call site.
 */
export const UPDATE_MAX_TRAILING_BINDING_COUNT = 6;

export function composeConditionStatement(predicateSql: string): string {
	return `WITH requested(requested_hk, requested_sk) AS (VALUES (?, ?))
SELECT i.hk IS NOT NULL AS item_present,
       CASE WHEN (${predicateSql}) THEN 1 ELSE 0 END AS condition_ok,
       i.last_transaction_ts
FROM requested
LEFT JOIN items AS i ON i.hk = requested.requested_hk AND i.sk = requested.requested_sk`;
}

export type ExpressionBindingDescriptor =
	| { kind: "val"; value: JsonPrimitive }
	| { kind: "keyText"; value: string }
	| { kind: "keyB64"; value: string }
	| { kind: "b64"; value: string }
	| { kind: "path"; value: string };

/**
 * Compiled SQL plan for evaluating a write condition expression.
 * The plan is JSON-serializable and survives coordinator persistence.
 */
export type CompiledConditionPlan = {
	/** Plan schema version number. */
	version: typeof CONDITION_PLAN_VERSION;
	/** Discriminant for condition expression plans. */
	kind: "condition";
	/** Compiled SQL predicate expression evaluating the condition. */
	sql: string;
	/** Descriptors for expression values bound to SQL statement parameters. */
	bindings: readonly ExpressionBindingDescriptor[];
	/** Number of parameter bindings in this plan. */
	bindingCount: number;
	/** Total bindings including fixed statement parameters (requested_hk, requested_sk). */
	completeBindingCount: number;
	/** Storage columns required to execute the condition statement. */
	requiredColumns: readonly ExpressionRequiredColumn[];
	/** Item data dependencies needed by the condition. */
	dataDependencies: {
		/** True if the condition accesses the complete data column rather than specific paths. */
		completeData: boolean;
		/** List of distinct JSON paths accessed in item data. */
		paths: readonly string[];
	};
	/** Static type analysis guarantees for the condition result. */
	result: {
		nativeTypes: readonly ["boolean"];
		canBeMissing: false;
	};
	/** Canonical deterministic fingerprint used for transaction idempotency. */
	identity: string;
};

/**
 * Compiled SQL plan for applying an update expression to a JSON item.
 * The plan is JSON-serializable and survives coordinator persistence.
 */
export type CompiledUpdatePlan = {
	/** Plan schema version number. */
	version: typeof UPDATE_PLAN_VERSION;
	/** Discriminant for update expression plans. */
	kind: "update";
	/**
	 * SQL expression computing the complete new JSONB document from stored `i.data`.
	 * Actions wrap accumulator starting at `i.data`; values evaluate against pre-image `i.data`.
	 */
	documentSql: string;
	/**
	 * SQL boolean expression checking if the update can apply to the target item.
	 * Evaluates to 1 when item exists, data is JSON, targets exist, operands are present,
	 * and result remains a valid document. Evaluates to 0 otherwise.
	 */
	applicableSql: string;
	/**
	 * SQL boolean expression checking that every `set` value has a type a JSON document can hold.
	 * Evaluates to 1 when no value is bytes, and to 0 when one is — a key reference over a binary
	 * key, or a SQLite function that returned a blob for this item.
	 *
	 * It is also a term of `applicableSql`, so it never decides on its own whether an update applies.
	 * It exists so the probe can separate this ONE cause from the rest and report it to the caller,
	 * which a single applicability bit cannot do. `1` when no value needs the test.
	 */
	valueTypeSql: string;
	/** Descriptors for expression values bound to SQL statement parameters. */
	bindings: readonly ExpressionBindingDescriptor[];
	/** Number of parameter bindings in this plan. */
	bindingCount: number;
	/**
	 * `UPDATE_FIXED_BINDING_COUNT + bindingCount`: the keys plus the plan's own parameters, which
	 * together occupy `?1` to here. A statement appends its own tail starting at the next number.
	 */
	completeBindingCount: number;
	/** Storage columns required to execute the update statement. */
	requiredColumns: readonly ExpressionRequiredColumn[];
	/** Item data dependencies needed by the update. */
	dataDependencies: {
		/** True if the update accesses the complete data column rather than specific paths. */
		completeData: boolean;
		/** List of distinct JSON paths modified or accessed in item data. */
		paths: readonly string[];
	};
	/** Canonical deterministic fingerprint used for transaction idempotency. */
	identity: string;
};
