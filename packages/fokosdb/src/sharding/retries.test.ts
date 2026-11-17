import { describe, expect, it } from "vitest";
import { systemPlatform, type FokosPlatform } from "../shared/platform.js";
import { jitterBackoff, tryWhile } from "./retries.js";

/** A platform whose random value is fixed and whose wait only records the delay. */
function recordingPlatform(random: number): FokosPlatform & { waits: number[] } {
	const waits: number[] = [];
	return { ...systemPlatform, random: () => random, wait: async (ms) => void waits.push(ms), waits };
}

describe("retries", () => {
	it("takes the jitter from the random value of the platform", () => {
		expect(jitterBackoff(recordingPlatform(0.5), 3, 10, 1_000)).toBe(40);
		expect(jitterBackoff(recordingPlatform(0.5), 20, 10, 1_000)).toBe(500);
		expect(jitterBackoff(recordingPlatform(0), 3, 10, 1_000)).toBe(0);
	});

	it("waits through the platform between two attempts and stops when the error is not retryable", async () => {
		const platform = recordingPlatform(0.5);
		const attempts: number[] = [];
		await expect(
			tryWhile(
				platform,
				async (attempt) => {
					attempts.push(attempt);
					throw new Error("fail");
				},
				(_err, nextAttempt) => nextAttempt <= 3,
				{ baseDelayMs: 10, maxDelayMs: 1_000 },
			),
		).rejects.toThrow("fail");
		expect(attempts).toEqual([1, 2, 3]);
		expect(platform.waits).toEqual([20, 40]);
	});

	it("returns the first answer that succeeds", async () => {
		const platform = recordingPlatform(0);
		let calls = 0;
		const answer = await tryWhile(
			platform,
			async () => {
				if (++calls < 2) {
					throw new Error("fail");
				}
				return "ok";
			},
			() => true,
			{ baseDelayMs: 10, maxDelayMs: 1_000 },
		);
		expect(answer).toBe("ok");
		expect(platform.waits).toEqual([0]);
	});
});
