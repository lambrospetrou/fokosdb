import invariant from "./invariant.js";

export function assertExists<T>(val: T | undefined | null): asserts val is T {
	invariant(val !== undefined && val !== null, "Value is missing");
}
