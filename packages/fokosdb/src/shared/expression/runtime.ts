import { type KeyBytes } from "../../sharding/key-codec.js";
import { materializedPlanBindings } from "./bindings.js";
import { ExpressionError } from "./errors.js";
import { decodeProjectedRow, type ProjectedWireRow } from "./projection.js";
import { estRowBytesExpr, JSON_KIND_CODE } from "../partition/item-size.js";
import { tryOne } from "../sql-cursor.js";
import {
	composeConditionStatement,
	composeProjectionStatement,
	type CompiledConditionPlan,
	type CompiledProjectionPlan,
	type CompiledUpdatePlan,
} from "./plan.js";

export type ConditionEvaluationResult = {
	itemPresent: boolean;
	conditionOk: boolean;
	lastReadTs: number | null;
	lastWriteTs: number | null;
	rowsRead: number;
	rowsWritten: number;
};

export type ProjectedReadResult = {
	row?: { projected: ProjectedWireRow; version: number; ttlAt?: number };
	rowsRead: number;
	rowsWritten: number;
};

export type UpdateProbeResult = {
	itemPresent: boolean;
	applicable: boolean;
	/** False when a `set` value evaluated to bytes for this item, which a JSON document cannot hold. */
	valueTypeOk: boolean;
	newSize: number | null;
	lastReadTs: number | null;
	lastWriteTs: number | null;
	rowsRead: number;
	rowsWritten: number;
};

export function evaluateConditionPlan(
	storage: DurableObjectStorage,
	plan: CompiledConditionPlan,
	hashKey: KeyBytes,
	sortKey: KeyBytes,
): ConditionEvaluationResult {
	const statement = composeConditionStatement(plan.sql);
	try {
		const cursor = storage.sql.exec<{
			item_present: number;
			condition_ok: number;
			last_read_ts: number | null;
			last_write_ts: number | null;
		}>(statement, hashKey, sortKey, ...materializedPlanBindings(plan));
		const row = cursor.one();
		return {
			itemPresent: row.item_present === 1,
			conditionOk: row.condition_ok === 1,
			lastReadTs: row.last_read_ts,
			lastWriteTs: row.last_write_ts,
			rowsRead: cursor.rowsRead,
			rowsWritten: cursor.rowsWritten,
		};
	} catch (error) {
		if (error instanceof ExpressionError) {
			throw error;
		}
		throw new ExpressionError("runtime_capability", "Workers SQLite could not evaluate the compiled expression", { cause: error });
	}
}

/**
 * Runs a projected point read: one row of the items table through the plan's value and type
 * columns. The pool is `?1`, `hk` is `?2`, and `sk` is `?3` of the composed statement. An absent row
 * is a `found: false` read. A projection has nothing to return for one, so no LEFT JOIN is needed.
 */
export function readProjectedItem(
	storage: DurableObjectStorage,
	plan: CompiledProjectionPlan,
	hashKey: KeyBytes,
	sortKey: KeyBytes,
): ProjectedReadResult {
	const statement = composeProjectionStatement(plan);
	try {
		const cursor = storage.sql.exec<Record<string, SqlStorageValue>>(
			statement,
			...materializedPlanBindings(plan, "pool"),
			hashKey,
			sortKey,
		);
		const row = tryOne(cursor);
		if (row === undefined) {
			return { row: undefined, rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
		}
		return {
			row: {
				projected: decodeProjectedRow(row, plan.names.length),
				version: row.v as number,
				ttlAt: (row.ttl_epoch_utc_seconds as number | null) ?? undefined,
			},
			rowsRead: cursor.rowsRead,
			rowsWritten: cursor.rowsWritten,
		};
	} catch (error) {
		if (error instanceof ExpressionError) {
			throw error;
		}
		throw new ExpressionError("runtime_capability", "Workers SQLite could not evaluate the compiled expression", { cause: error });
	}
}

export function composeUpdateProbeStatement(plan: CompiledUpdatePlan): string {
	// ?1 and ?2 are the keys, as they are in every statement that runs an update plan.
	const hkParam = "?1";
	const skParam = "?2";
	// value_type_ok names ONE cause of an inapplicable update, so a caller learns that its value was
	// bytes for this item instead of only that the update did not apply. It runs over a JSON pre-image
	// only: the fragment can read a JSON path, and json_type over a text or bytes row raises. An absent
	// row has a JSON pre-image, because an update creates the item.
	return `WITH requested(requested_hk, requested_sk) AS (VALUES (${hkParam}, ${skParam}))
SELECT i.hk IS NOT NULL AS item_present,
       (${plan.applicableSql}) AS applicable,
       CASE WHEN i.hk IS NULL OR i.data_kind = ${JSON_KIND_CODE} THEN (${plan.valueTypeSql}) ELSE 1 END AS value_type_ok,
       CASE WHEN (${plan.applicableSql}) = 1 THEN (${estRowBytesExpr(plan.documentSql, hkParam, skParam)}) ELSE NULL END AS new_size,
       i.last_read_ts,
       i.last_write_ts
FROM requested
LEFT JOIN items AS i ON i.hk = requested.requested_hk AND i.sk = requested.requested_sk`;
}

export function probeUpdatePlan(
	storage: DurableObjectStorage,
	plan: CompiledUpdatePlan,
	hashKey: KeyBytes,
	sortKey: KeyBytes,
): UpdateProbeResult {
	const statement = composeUpdateProbeStatement(plan);
	try {
		const cursor = storage.sql.exec<{
			item_present: number;
			applicable: number;
			value_type_ok: number;
			new_size: number | null;
			last_read_ts: number | null;
			last_write_ts: number | null;
		}>(statement, hashKey, sortKey, ...materializedPlanBindings(plan));
		const row = cursor.one();
		return {
			itemPresent: row.item_present === 1,
			applicable: row.applicable === 1,
			valueTypeOk: row.value_type_ok === 1,
			newSize: row.new_size,
			lastReadTs: row.last_read_ts,
			lastWriteTs: row.last_write_ts,
			rowsRead: cursor.rowsRead,
			rowsWritten: cursor.rowsWritten,
		};
	} catch (error) {
		if (error instanceof ExpressionError) {
			throw error;
		}
		throw new ExpressionError("runtime_capability", "Workers SQLite could not evaluate the compiled expression", { cause: error });
	}
}
