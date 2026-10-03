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

/**
 * Calls `fn` at most once per interval, at the end of the interval.
 *
 * The first `schedule(ms)` starts a timer of `ms`. The calls to `schedule` before the timer fires do
 * nothing. `fn` takes no arguments and reads the current state when it runs, so it sees all the changes
 * of the interval. `fn` runs in a timer task of its own. A `schedule` while `fn` runs, or after it,
 * starts a new interval.
 *
 * It does not read the clock, because the Workers runtime does not advance `Date.now()` while code runs.
 */
export function throttleTrailing(fn: () => void): { schedule(ms: number): void; cancel(): void; forceRun(): void } {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const run = () => {
		timer = undefined;
		fn();
	};
	return {
		forceRun() {
			fn();
		},
		schedule(ms: number) {
			if (timer === undefined) {
				timer = setTimeout(run, ms);
			}
		},
		cancel() {
			clearTimeout(timer);
			timer = undefined;
		},
	};
}
