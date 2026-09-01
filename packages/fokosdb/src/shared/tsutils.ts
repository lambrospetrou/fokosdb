export function assertExists<T>(val: T | undefined | null): asserts val is T {
	if (val === undefined || val === null) throw new Error("Value is missing");
}
