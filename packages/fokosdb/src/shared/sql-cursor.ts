import invariant from "./invariant.js";

/**
 * The first row of a cursor, or `undefined` when the query returned no row. It reads one row and
 * stops, so it builds no intermediate array. Use it for a query whose row can be absent, such as a
 * primary-key lookup.
 */
export function tryOne<T>(cursor: Iterable<T>): T | undefined {
	for (const row of cursor) return row;
	return undefined;
}

/**
 * The first row of a cursor that must return a row, such as an aggregate without GROUP BY. Raises
 * `invariant_failed` when the query returned no row.
 */
export function one<T>(cursor: Iterable<T>, message?: string): T {
	for (const row of cursor) return row;
	invariant(false, message ?? "expected at least one row from query");
}

/** True when the query returned at least one row. It reads one row and stops. */
export function exists(cursor: Iterable<unknown>): boolean {
	return tryOne(cursor) !== undefined;
}
