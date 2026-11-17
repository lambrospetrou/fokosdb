/**
 * The clock, the timers, and the random values of one Durable Object or of one client. The library
 * reads each of them only through this object, so a caller can give a platform with its own clock.
 */
export type FokosTimer = number;

export interface FokosPlatform {
	/** Epoch milliseconds. */
	now(): number;
	setTimeout(fn: () => void, ms: number): FokosTimer;
	clearTimeout(timer: FokosTimer | undefined): void;
	wait(ms: number): Promise<void>;
	/** A new random ID of 32 hex characters. */
	randomId(): string;
	/** A random number from 0 (included) to 1 (not included). */
	random(): number;
}

/** The platform of the Workers runtime. */
export const systemPlatform: FokosPlatform = {
	now: () => Date.now(),
	// The Workers runtime returns a number.
	setTimeout: (fn, ms) => setTimeout(fn, ms) as unknown as FokosTimer,
	clearTimeout: (timer) => clearTimeout(timer),
	wait: async (ms) => await scheduler.wait(ms),
	randomId: () => crypto.randomUUID().replaceAll("-", ""),
	random: () => Math.random(),
};
