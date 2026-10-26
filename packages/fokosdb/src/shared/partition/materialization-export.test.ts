import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { FokosDBPartitionDO } from "../../server/do-partition.js";
import { testPartitionStub } from "../../../test/stub-helpers.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { PartitionStore } from "./partition-store.js";
import {
	MaterializationExporter,
	type ExportCall,
	type ExportItemOutcome,
	type MaterializationExportConfig,
} from "./materialization-export.js";

const kb = (value: string) => KeyCodec.encode(value);
const label = (key: KeyBytes) => KeyCodec.decode(key) as string;

const CONFIG: MaterializationExportConfig = {
	maxCalls: 2,
	maxItems: 100,
	maxBytes: 1024 * 1024,
	maxScannedRows: 1_000,
	unownedRetryMs: 30_000,
	unprocessedRetryMs: 5_000,
	rejectedRetryMs: 30_000,
	failureBackoffMinMs: 1_000,
	failureBackoffMaxMs: 30_000,
	callTimeoutMs: 30_000,
	fallbackAlarmMs: 60_000,
};

type Harness = {
	store: PartitionStore;
	state: DurableObjectState;
	exporter: MaterializationExporter;
	calls: ExportCall[];
	put: (hk: string, data?: string) => number;
	del: (hk: string) => void;
	outbox: () => Array<{ hk: string; revision: number; is_deleted: number; is_local_only: number; next_attempt_at: number }>;
	exportState: () => { epoch: number; isCallOpen: boolean };
	/** Waits until the loop stops. */
	drain: () => Promise<void>;
	/** Moves the clock of the exporter forward. */
	advance: (ms: number) => void;
	/** The times that the exporter gave to `scheduleRetry`. */
	retries: number[];
};

/**
 * A store with capture on and an exporter over it. `answer` gives the outcomes of each call; the
 * default acknowledges each image. The clock stays at `now` unless the test moves it.
 */
async function withExporter(
	opts: {
		answer?: (call: ExportCall) => Promise<ExportItemOutcome[]> | ExportItemOutcome[];
		owns?: (hk: string) => boolean;
		config?: Partial<MaterializationExportConfig>;
	},
	fn: (h: Harness) => Promise<void>,
): Promise<void> {
	const stub = testPartitionStub(`export-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
		let exporter: MaterializationExporter | undefined;
		const store = new PartitionStore(state.storage, { captureChanges: () => true, onChangeCaptured: () => exporter?.wake() });
		const calls: ExportCall[] = [];
		let now = 1_000;
		const retries: number[] = [];
		exporter = new MaterializationExporter({
			store,
			canRun: () => true,
			ownerCheck: () => (row) => opts.owns?.(label(row.hk)) ?? true,
			send: async (call) => {
				calls.push(call);
				return await (opts.answer ?? ((c: ExportCall) => c.images.map(() => ({ outcome: "acknowledged" as const }))))(call);
			},
			config: () => ({ ...CONFIG, ...opts.config }),
			scheduleRetry: (runAt) => retries.push(runAt),
			now: () => now,
			logParams: () => ({ partition: "test" }),
			// The exporter waits only to yield, so the wait returns at once.
			wait: async () => {},
		});
		const h: Harness = {
			store,
			state,
			exporter,
			calls,
			put: (hk, data = "d") => store.upsertItem({ hk: kb(hk), sk: kb("s"), data, kind: "text", ttlAt: null, txOrderTs: 1 }).version,
			del: (hk) => void store.deleteItem({ hk: kb(hk), sk: kb("s"), txOrderTs: 1 }),
			outbox: () =>
				state.storage.sql
					.exec<{ hk: ArrayBuffer; revision: number; is_deleted: number; is_local_only: number; next_attempt_at: number }>(
						`SELECT hk, revision, is_deleted, is_local_only, next_attempt_at FROM materialization_outbox ORDER BY hk`,
					)
					.toArray()
					.map((row) => ({ ...row, hk: label(new Uint8Array(row.hk) as KeyBytes) })),
			exportState: () => store.exportState(),
			drain: async () => {
				await vi.waitFor(() => expect(exporter!.running).toBe(false));
			},
			advance: (ms) => {
				now += ms;
			},
			retries,
		};
		await fn(h);
	});
}

describe("MaterializationExporter — claim and acknowledgement", () => {
	it("exports the live image and the tombstone of each changed item, and deletes the acknowledged rows", async () => {
		await withExporter({}, async (h) => {
			h.put("a", "one");
			h.put("b");
			h.store.markOutboxRowExported(kb("b"), kb("s"));
			h.del("b");
			await h.drain();

			expect(h.calls).toHaveLength(1);
			const [call] = h.calls;
			expect(call.epoch).toBe(1);
			expect(call.ancestorFences).toEqual([]);
			expect(call.images.map((image) => ({ ...image, hashKey: label(image.hashKey) }))).toEqual([
				{ opIndex: 0, hashKey: "a", sortKey: kb("s"), revision: 1, deleted: false, data: "one", kind: "text", ttlAt: null },
				{ opIndex: 1, hashKey: "b", sortKey: kb("s"), revision: 2, deleted: true },
			]);
			expect(h.outbox()).toEqual([]);
			expect(h.exportState()).toEqual({ epoch: 1, isCallOpen: false });
		});
	});

	it("marks a claimed row as not local only, so a delete during the call writes a tombstone", async () => {
		let release!: () => void;
		const blocked = new Promise<void>((resolve) => (release = resolve));
		await withExporter(
			{
				answer: async (call) => {
					await blocked;
					return call.images.map(() => ({ outcome: "acknowledged" as const }));
				},
			},
			async (h) => {
				h.put("a");
				await vi.waitFor(() => expect(h.calls).toHaveLength(1));
				expect(h.exportState().isCallOpen).toBe(true);
				// The create left the partition, so the delete must reach the receiver.
				h.del("a");
				expect(h.outbox()).toMatchObject([{ hk: "a", revision: 2, is_deleted: 1, is_local_only: 0 }]);
				release();
				await h.drain();

				// The acknowledgement of revision 1 does not delete the tombstone. The second call sends it.
				expect(h.calls.map((c) => c.images.map((i) => [label(i.hashKey), i.revision, i.deleted]))).toEqual([
					[["a", 1, false]],
					[["a", 2, true]],
				]);
				expect(h.outbox()).toEqual([]);
			},
		);
	});

	it("never sends one key in two calls in flight at the same time", async () => {
		const releases: Array<() => void> = [];
		await withExporter(
			{
				answer: async (call) => {
					await new Promise<void>((resolve) => releases.push(resolve));
					return call.images.map(() => ({ outcome: "acknowledged" as const }));
				},
			},
			async (h) => {
				h.put("a");
				await vi.waitFor(() => expect(h.calls).toHaveLength(1));
				h.put("a");
				h.put("b");
				// The second worker claims b, and skips a while the first call holds it.
				await vi.waitFor(() => expect(h.calls).toHaveLength(2));
				expect(h.calls[1].images.map((i) => label(i.hashKey))).toEqual(["b"]);
				while (h.exporter.running) {
					releases.shift()?.();
					await scheduler.wait(1);
				}
				expect(h.calls.map((c) => c.images.map((i) => `${label(i.hashKey)}@${i.revision}`))).toEqual([["a@1"], ["b@1"], ["a@2"]]);
				expect(h.outbox()).toEqual([]);
			},
		);
	});
});

describe("MaterializationExporter — epochs", () => {
	it("an unknown answer raises the epoch, keeps the row, stops the loop for the backoff, and the next call goes with the new epoch", async () => {
		let failures = 1;
		await withExporter(
			{
				answer: (call) => {
					if (failures-- > 0) {
						throw new Error("the transport failed");
					}
					return call.images.map(() => ({ outcome: "acknowledged" as const }));
				},
			},
			async (h) => {
				vi.spyOn(console, "error").mockImplementation(() => {});
				h.put("a");
				await h.drain();
				expect(h.calls.map((c) => c.epoch)).toEqual([1]);
				expect(h.exportState()).toEqual({ epoch: 2, isCallOpen: false });
				expect(h.outbox()).toMatchObject([{ hk: "a", next_attempt_at: 0 }]);
				// The alarm starts the loop at the end of the backoff.
				expect(h.exporter.deadline()).toBe(1_000 + CONFIG.failureBackoffMinMs);
				expect(h.retries).toEqual([1_000 + CONFIG.failureBackoffMinMs]);
				h.advance(CONFIG.failureBackoffMinMs);
				h.exporter.wake();
				await h.drain();
				expect(h.calls.map((c) => c.epoch)).toEqual([1, 2]);
				expect(h.outbox()).toEqual([]);
			},
		);
	});

	it("a fenced item raises the epoch, and an unknown item of a call raises it one time", async () => {
		let round = 0;
		await withExporter(
			{
				answer: (call) =>
					call.images.map((image) => {
						if (round++ === 0) {
							return { outcome: "unprocessed" as const, code: "producer_epoch_fenced" };
						}
						return label(image.hashKey) === "b" && round < 3
							? { outcome: "unknown" as const, code: "x" }
							: { outcome: "acknowledged" as const };
					}),
			},
			async (h) => {
				h.put("a");
				h.put("b");
				await h.drain();
				// Both rows stay after the first call, and the raise of its completion applies to the next call.
				expect(h.calls.map((c) => [c.epoch, c.images.map((i) => label(i.hashKey))])).toEqual([
					[1, ["a", "b"]],
					[2, ["a", "b"]],
				]);
				expect(h.exportState().epoch).toBe(2);
				expect(h.outbox()).toEqual([]);
			},
		);
	});

	it("discards the answer of a call whose epoch is below the current epoch", async () => {
		let release!: () => void;
		const blocked = new Promise<void>((resolve) => (release = resolve));
		let first = true;
		await withExporter(
			{
				answer: async (call) => {
					if (first) {
						first = false;
						await blocked;
					}
					return call.images.map(() => ({ outcome: "acknowledged" as const }));
				},
			},
			async (h) => {
				h.put("a");
				await vi.waitFor(() => expect(h.calls).toHaveLength(1));
				// A split raises the epoch while the call is in flight.
				expect(h.store.raiseExportEpoch()).toBe(2);
				release();
				await h.drain();
				// The old answer deleted nothing. The row went again with epoch 2.
				expect(h.calls.map((c) => c.epoch)).toEqual([1, 2]);
				expect(h.outbox()).toEqual([]);
			},
		);
	});

	it("an answer that it discards for its old epoch starts no backoff", async () => {
		let release!: () => void;
		const blocked = new Promise<void>((resolve) => (release = resolve));
		let first = true;
		await withExporter(
			{
				answer: async (call) => {
					if (first) {
						first = false;
						await blocked;
						throw new Error("the transport failed");
					}
					return call.images.map(() => ({ outcome: "acknowledged" as const }));
				},
			},
			async (h) => {
				vi.spyOn(console, "error").mockImplementation(() => {});
				h.put("a");
				await vi.waitFor(() => expect(h.calls).toHaveLength(1));
				h.store.raiseExportEpoch();
				release();
				await h.drain();
				// The failed call had an old epoch, so the row went again at once with the current epoch.
				expect(h.retries).toEqual([]);
				expect(h.calls.map((c) => c.epoch)).toEqual([1, 2]);
				expect(h.outbox()).toEqual([]);
			},
		);
	});

	it("the first claim after a restart with a call open raises the epoch", async () => {
		await withExporter({}, async (h) => {
			h.store.updateExportState({ callOpen: true });
			h.put("a");
			await h.drain();
			expect(h.calls.map((c) => c.epoch)).toEqual([2]);
			expect(h.exportState()).toEqual({ epoch: 2, isCallOpen: false });
		});
	});

	it("each call carries the export fences while a row of the inherited work remains", async () => {
		await withExporter({}, async (h) => {
			h.store.insertMigratedOutboxRow({ hk: kb("inherited"), sk: kb("s"), revision: 1, is_deleted: 1 });
			h.store.importExportFences("parent", 7, [{ ancestorId: "grandparent", minEpoch: 3 }]);
			expect(h.store.exportFences()).toEqual([
				{ ancestorId: "grandparent", minEpoch: 3 },
				{ ancestorId: "parent", minEpoch: 7 },
			]);
			h.put("own");
			await h.drain();
			expect(h.calls[0].ancestorFences).toHaveLength(2);
			// The acknowledgement of the inherited row deleted the fences.
			expect(h.store.exportFences()).toEqual([]);
		});
	});

	it("a target with no copied row stores no fence", async () => {
		await withExporter({}, async (h) => {
			h.store.importExportFences("parent", 7, []);
			expect(h.store.exportFences()).toEqual([]);
		});
	});
});

describe("MaterializationExporter — failures and ownership", () => {
	it("delays an item that was not applied when other items applied, and backs off when each item failed", async () => {
		let round = 0;
		await withExporter(
			{
				answer: (call) => {
					round++;
					return call.images.map((image) =>
						round === 1 && label(image.hashKey) === "b"
							? { outcome: "unprocessed" as const, code: "partition_migrating" }
							: { outcome: "acknowledged" as const },
					);
				},
			},
			async (h) => {
				h.put("a");
				h.put("b");
				await h.drain();
				expect(h.outbox()).toMatchObject([{ hk: "b", next_attempt_at: 1_000 + CONFIG.unprocessedRetryMs }]);
				expect(h.exporter.deadline()).toBe(1_000 + CONFIG.unprocessedRetryMs);
				// Some items applied, so the call did not fail as a whole, and no backoff starts.
				expect(h.retries).toEqual([]);
			},
		);
		await withExporter(
			{ answer: (call) => call.images.map(() => ({ outcome: "unprocessed" as const, code: "partition_over_size" })) },
			async (h) => {
				h.put("a");
				await h.drain();
				// A call in which each item failed writes no row. The row stays due, and the loop stops until
				// the end of the backoff.
				expect(h.calls).toHaveLength(1);
				expect(h.outbox()).toMatchObject([{ hk: "a", next_attempt_at: 0 }]);
				expect(h.exporter.deadline()).toBe(1_000 + CONFIG.failureBackoffMinMs);
				expect(h.retries).toEqual([1_000 + CONFIG.failureBackoffMinMs]);
			},
		);
	});

	it("defers an unowned row, and drops a row that does not match its item", async () => {
		await withExporter({ owns: (hk) => hk !== "moved" }, async (h) => {
			const error = vi.spyOn(console, "error").mockImplementation(() => {});
			h.put("moved");
			h.put("broken");
			h.state.storage.sql.exec(`UPDATE materialization_outbox SET revision = 9 WHERE hk = ?`, kb("broken"));
			await h.drain();
			expect(h.calls).toEqual([]);
			expect(h.outbox()).toEqual([
				{ hk: "moved", revision: 1, is_deleted: 0, is_local_only: 1, next_attempt_at: 1_000 + CONFIG.unownedRetryMs },
			]);
			expect(error).toHaveBeenCalledWith(expect.objectContaining({ message: expect.stringContaining("does not match its item") }));
		});
	});
});
