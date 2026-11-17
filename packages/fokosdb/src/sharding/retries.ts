import type { FokosPlatform } from "../shared/platform.js";

/** A random delay from 0 up to `min(2^attempt * baseDelayMs, maxDelayMs)`, in milliseconds. */
export function jitterBackoff(platform: FokosPlatform, attempt: number, baseDelayMs: number, maxDelayMs: number): number {
	const attemptUpperBoundMs = Math.min(2 ** attempt * baseDelayMs, maxDelayMs);
	return Math.floor(platform.random() * attemptUpperBoundMs);
}

/**
 * Calls `fn` until it succeeds or `isRetryable` returns false, and waits a jittered backoff between
 * two attempts. `nextAttempt` is 2 for the first retry.
 */
export async function tryWhile<T>(
	platform: FokosPlatform,
	fn: (attempt: number) => Promise<T>,
	isRetryable: (err: unknown, nextAttempt: number) => boolean,
	options: { baseDelayMs: number; maxDelayMs: number },
): Promise<T> {
	const baseDelayMs = Math.floor(options.baseDelayMs);
	const maxDelayMs = Math.floor(options.maxDelayMs);
	if (baseDelayMs <= 0 || maxDelayMs <= 0) {
		throw new Error("baseDelayMs and maxDelayMs must be greater than 0");
	}
	if (baseDelayMs >= maxDelayMs) {
		throw new Error("baseDelayMs must be less than maxDelayMs");
	}
	for (let attempt = 1; ; ) {
		try {
			return await fn(attempt);
		} catch (err) {
			attempt += 1;
			if (!isRetryable(err, attempt)) {
				throw err;
			}
			await platform.wait(jitterBackoff(platform, attempt, baseDelayMs, maxDelayMs));
		}
	}
}
