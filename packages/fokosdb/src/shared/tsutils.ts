import invariant from "./invariant.js";

export function assertExists<T>(val: T | undefined | null): asserts val is T {
	invariant(val !== undefined && val !== null, "Value is missing");
}

/**
 * Parses JSON that this code wrote itself, for example rows in Durable Object storage.
 * This function does not validate the result. Do not use it for client input.
 */
export function parseJSONTrusted<T>(json: string): T {
	return JSON.parse(json) as T;
}

/**
 * Same as `Array.isArray`, but it narrows to `readonly unknown[]`.
 * `Array.isArray` narrows to `any[]`, and this changes a typed array into an array of `any`.
 */
export function isArray(value: unknown): value is readonly unknown[] {
	return Array.isArray(value);
}
