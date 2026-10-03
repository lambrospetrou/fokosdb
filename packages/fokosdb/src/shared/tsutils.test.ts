import { describe, expect, it } from "vitest";
import { throttleTrailing } from "./tsutils.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("throttleTrailing", () => {
	it("calls fn once at the end of the interval for many calls of schedule", async () => {
		let calls = 0;
		const throttle = throttleTrailing(() => calls++);
		throttle.schedule(5);
		throttle.schedule(5);
		throttle.schedule(5);
		expect(calls).toBe(0);
		await sleep(30);
		expect(calls).toBe(1);
	});

	it("starts a new interval for a schedule while fn runs", async () => {
		let calls = 0;
		const throttle = throttleTrailing(() => {
			calls++;
			if (calls === 1) {
				throttle.schedule(5);
			}
		});
		throttle.schedule(5);
		await sleep(50);
		expect(calls).toBe(2);
	});

	it("does not call fn after cancel", async () => {
		let calls = 0;
		const throttle = throttleTrailing(() => calls++);
		throttle.schedule(5);
		throttle.cancel();
		await sleep(30);
		expect(calls).toBe(0);
		throttle.schedule(5);
		await sleep(30);
		expect(calls).toBe(1);
	});
});
