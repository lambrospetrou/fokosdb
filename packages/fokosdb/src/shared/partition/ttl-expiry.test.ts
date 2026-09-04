import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { PartitionDO } from "../../server/do-partition.js";
import { KeyCodec } from "../partition-topology/key-codec.js";
import { PartitionStore } from "./partition-store.js";
import { EST_ROW_BYTES_K } from "./item-size.js";
import { TtlExpiry, type TtlSweepConfig } from "./ttl-expiry.js";

const kb = (value: string) => KeyCodec.encode(value);
const NOW_SECONDS = 100;

async function withStore(fn: (store: PartitionStore, state: DurableObjectState) => void | Promise<void>): Promise<void> {
	const stub = PartitionDO.getByName(env.PARTITION_DO, `ttl-expiry-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		await fn(new PartitionStore(state.storage), state);
	});
}

function config(overrides: Partial<TtlSweepConfig> = {}): TtlSweepConfig {
	return {
		chunkSize: 2,
		sleepMs: 7,
		maxRowsBeforeSleep: 100,
		maxBytesBeforeSleep: 1_000_000,
		maxRowsPerCycle: 10,
		initialDelayMs: 100,
		...overrides,
	};
}

function putExpired(store: PartitionStore, hk: string, ttlAt: number, data = "d"): number {
	store.upsertItem({ hk: kb(hk), sk: kb("s"), data, kind: "text", ttlAt, lastTransactionTs: 1 });
	return new TextEncoder().encode(data).length + kb(hk).byteLength + kb("s").byteLength + EST_ROW_BYTES_K;
}

function itemCount(state: DurableObjectState): number {
	return state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items`).toArray()[0].n;
}

function makeExpiry(
	store: PartitionStore,
	opts: {
		config?: TtlSweepConfig;
		canSweep?: () => boolean;
		wait?: (ms: number) => Promise<void>;
	} = {},
): TtlExpiry {
	return new TtlExpiry({
		store,
		canSweep: opts.canSweep ?? (() => true),
		logParams: () => ({ partition: "test" }),
		config: () => opts.config ?? config(),
		nowSec: () => NOW_SECONDS,
		wait: opts.wait ?? (async () => {}),
	});
}

describe("TtlExpiry", () => {
	it("runs multiple chunks and reports cap, partial, and empty outcomes", async () => {
		await withStore(async (store, state) => {
			const sizes = Array.from({ length: 5 }, (_, index) => putExpired(store, `k${index}`, index + 1, "x".repeat(index + 1)));
			const waits: number[] = [];
			const expiry = makeExpiry(store, {
				config: config({ chunkSize: 2, maxRowsPerCycle: 4 }),
				wait: async (ms) => {
					waits.push(ms);
				},
			});

			expect(await expiry.runCycle()).toEqual({
				deletedRows: 4,
				deletedBytes: sizes.slice(0, 4).reduce((sum, size) => sum + size, 0),
				more: true,
			});
			expect(itemCount(state)).toBe(1);
			expect(waits).toEqual([0]);

			expect(await expiry.runCycle()).toEqual({ deletedRows: 1, deletedBytes: sizes[4], more: false });
			expect(await expiry.runCycle()).toEqual({ deletedRows: 0, deletedBytes: 0, more: false });
			expect(itemCount(state)).toBe(0);
		});
	});

	it("applies row and byte sleep budgets through wait", async () => {
		await withStore(async (store) => {
			for (let index = 0; index < 3; index++) putExpired(store, `row-${index}`, index + 1);
			const rowWaits: number[] = [];
			const rowBudget = makeExpiry(store, {
				config: config({ chunkSize: 1, maxRowsBeforeSleep: 2 }),
				wait: async (ms) => {
					rowWaits.push(ms);
				},
			});
			expect(await rowBudget.runCycle()).toMatchObject({ deletedRows: 3, more: false });
			expect(rowWaits).toEqual([0, 7, 0]);

			putExpired(store, "byte-0", 1);
			putExpired(store, "byte-1", 2);
			const byteWaits: number[] = [];
			const byteBudget = makeExpiry(store, {
				config: config({ chunkSize: 1, maxRowsBeforeSleep: 100, maxBytesBeforeSleep: 1 }),
				wait: async (ms) => {
					byteWaits.push(ms);
				},
			});
			expect(await byteBudget.runCycle()).toMatchObject({ deletedRows: 2, more: false });
			expect(byteWaits).toEqual([7, 7]);
		});
	});

	it("checks sweep permission before the first chunk and between chunks", async () => {
		await withStore(async (store, state) => {
			for (let index = 0; index < 3; index++) putExpired(store, `k${index}`, index + 1);
			let blockedChecks = 0;
			const blocked = makeExpiry(store, {
				canSweep: () => {
					blockedChecks++;
					return false;
				},
			});
			expect(await blocked.runCycle()).toEqual({ deletedRows: 0, deletedBytes: 0, more: false });
			expect(blockedChecks).toBe(1);
			expect(itemCount(state)).toBe(3);

			let checks = 0;
			const waits: number[] = [];
			const stopped = makeExpiry(store, {
				config: config({ chunkSize: 1 }),
				canSweep: () => ++checks === 1,
				wait: async (ms) => {
					waits.push(ms);
				},
			});
			expect(await stopped.runCycle()).toMatchObject({ deletedRows: 1, more: false });
			expect(checks).toBe(2);
			expect(waits).toEqual([0]);
			expect(itemCount(state)).toBe(2);
		});
	});

	it("rejects reentry while a cycle waits", async () => {
		await withStore(async (store) => {
			putExpired(store, "a", 1);
			putExpired(store, "b", 2);
			let release!: () => void;
			const gate = new Promise<void>((resolve) => {
				release = resolve;
			});
			let waitCalls = 0;
			const expiry = makeExpiry(store, {
				config: config({ chunkSize: 1 }),
				wait: async () => {
					waitCalls++;
					if (waitCalls === 1) await gate;
				},
			});

			const first = expiry.runCycle();
			expect(waitCalls).toBe(1);
			expect(await expiry.runCycle()).toEqual({ deletedRows: 0, deletedBytes: 0, more: false });
			release();
			expect(await first).toMatchObject({ deletedRows: 2, more: false });
		});
	});

	it("deduplicates arm calls and disarms a timer", async () => {
		await withStore(async (store) => {
			let checks = 0;
			const expiry = makeExpiry(store, { canSweep: () => (++checks, true) });

			expiry.arm(20);
			expiry.arm(0);
			expect(expiry.armed).toBe(true);
			expiry.disarm();
			expect(expiry.armed).toBe(false);
			await scheduler.wait(30);
			expect(checks).toBe(0);

			expiry.arm(0);
			expiry.arm(0);
			await scheduler.wait(10);
			expect(checks).toBe(1);
			expect(expiry.armed).toBe(false);
		});
	});

	it("validates every config field before deletion", async () => {
		await withStore(async (store) => {
			putExpired(store, "kept", 1);
			const invalid: Array<[keyof TtlSweepConfig, number]> = [];
			for (const name of ["chunkSize", "maxRowsBeforeSleep", "maxBytesBeforeSleep", "maxRowsPerCycle"] as const) {
				for (const value of [0, -1, 1.5]) invalid.push([name, value]);
			}
			for (const name of ["sleepMs", "initialDelayMs"] as const) {
				for (const value of [-1, 0.5]) invalid.push([name, value]);
			}
			let checks = 0;

			for (const [name, value] of invalid) {
				const expiry = makeExpiry(store, {
					config: config({ [name]: value }),
					canSweep: () => (++checks, true),
				});
				await expect(expiry.runCycle()).rejects.toThrow(name);
			}
			expect(checks).toBe(0);
			expect(store.getItem(kb("kept"), kb("s")).row).toBeDefined();
		});
	});

	it("lets direct errors throw but catches and logs timer errors", async () => {
		await withStore(async (store) => {
			const error = new Error("config failed");
			const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
			try {
				const expiry = new TtlExpiry({
					store,
					canSweep: () => true,
					logParams: () => ({ partition: "test" }),
					config: () => {
						throw error;
					},
				});
				await expect(expiry.runCycle()).rejects.toBe(error);
				expect(errorSpy).not.toHaveBeenCalled();

				expiry.arm(0);
				expect(expiry.armed).toBe(true);
				await scheduler.wait(10);
				expect(expiry.armed).toBe(false);
				expect(errorSpy).toHaveBeenCalledOnce();
				expect(errorSpy).toHaveBeenCalledWith({
					partition: "test",
					message: "fokos/partition: TTL expiry cycle failed.",
					error: "Error: config failed",
					errorProps: error,
				});
			} finally {
				errorSpy.mockRestore();
			}
		});
	});
});
