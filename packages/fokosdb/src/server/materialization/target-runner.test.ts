import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { FokosDBMaterializationDO } from "./do-materialization.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { MaterializationCleanup } from "./materialization-cleanup.js";
import { MaterializationStore, type LatestImage, type TargetRow } from "./materialization-store.js";
import { isMissingRpcMethod, TargetRunner, type TargetRunnerConfig } from "./target-runner.js";
import type { FokosTargetDefinition, PushTargetBatch, PushTargetResult } from "./targets.js";
import type { GsiWriteEntriesRequest } from "../do-partition.js";
import { compileGsiPlans, desiredGsiEntry, type GsiDesiredEntry } from "./gsi-target.js";
import type { GsiEntryOutcome } from "./target-runner.js";
import type { TargetImage } from "./materialization-store.js";
import invariant from "../../shared/invariant.js";
import type { ExpressionValue } from "../../shared/expression/types.js";
import { resolveLimits } from "../../shared/transaction-limits.js";

const kb = (value: string) => KeyCodec.encode(value);
const NO_SK = KeyCodec.encodeOptional(undefined);

const CONFIG: TargetRunnerConfig = {
	concurrency: 2,
	maxItemsPerStep: 100,
	maxBytesPerStep: 16 * 1024 * 1024,
	maxScannedPositions: 5_000,
	retryBackoffMinMs: 1_000,
	retryBackoffMaxMs: 10_000,
	breakerFailures: 3,
	probeIntervalMs: 5_000,
	callTimeoutMs: 30_000,
	fallbackAlarmMs: 60_000,
};

type Call = { targetId: string; batch: PushTargetBatch };

/** The answer of a push call: the results, or a throw for an unknown answer. */
type Answer = (call: Call) => unknown;

type Harness = {
	store: MaterializationStore;
	runner: TargetRunner;
	calls: Call[];
	/** Applies an image as `applyLatestImages` does: only a newer revision applies, and it wakes the runner. */
	ingest: (hk: string, revision: number, opts?: { deleted?: boolean; data?: string }) => void;
	target: (targetId: string) => TargetRow;
	retryRows: () => Array<{ target_id: string; hk: string; kind: string; not_before: number; attempts: number; last_error: string | null }>;
	count: (table: "latest_items" | "change_positions" | "key_size_estimates") => number;
	/** Waits until each worker stopped. */
	drain: () => Promise<void>;
	advance: (ms: number) => void;
	now: () => number;
	/** The times that the runner gave to `scheduleRetry`. */
	scheduled: number[];
	sql: SqlStorage;
	gsiCalls: GsiWriteEntriesRequest[];
};

/** The items of a call as `hashKey@revision`, with a `-` for a tombstone. */
const sent = (call: Call) => call.batch.items.map((item) => `${String(item.baseHashKey)}@${item.revision}${item.deleted ? "-" : ""}`);

const applyAll = ({ batch }: Call): PushTargetResult[] => batch.items.map(() => ({ outcome: "applied" }));

async function withRunner(
	opts: {
		targets?: string[];
		answer?: Answer;
		config?: Partial<TargetRunnerConfig>;
		owns?: (hk: string) => boolean;
		/** Runs when a step checks a push target, and throws when this instance cannot call the target. */
		checkPushTarget?: (targetId: string) => void;
		/**
		 * Makes each target a GSI target. `answer` gives the outcomes of each GSI call, or throws. `plans`
		 * runs when a step gets the evaluator of a target, and throws when the target cannot evaluate.
		 */
		gsi?: {
			answer: (request: GsiWriteEntriesRequest) => GsiEntryOutcome[];
			desired: (image: TargetImage) => GsiDesiredEntry;
			plans?: () => void;
		};
	},
	fn: (h: Harness) => Promise<void>,
): Promise<void> {
	const stub = env.MATERIALIZATION_DO.getByName(`runner-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: FokosDBMaterializationDO, state: DurableObjectState) => {
		const store = new MaterializationStore(state.storage);
		const targets: FokosTargetDefinition[] = (opts.targets ?? ["t1"]).map((targetId) =>
			opts.gsi ? { kind: "gsi", targetId, hashKey: { ref: "data" } } : { kind: "push_sink", targetId, binding: "SINK", method: "push" },
		);
		store.storeTargets(targets);
		const cleanup = new MaterializationCleanup({
			store,
			config: () => ({ deleteRows: 100, scanRows: 100 }),
		});
		const calls: Call[] = [];
		const gsiCalls: GsiWriteEntriesRequest[] = [];
		const scheduled: number[] = [];
		let now = 1_000;
		const runner = new TargetRunner({
			store,
			cleanup,
			targets: () => targets,
			canRun: () => true,
			ownerCheck: () => (row) => opts.owns?.(KeyCodec.decode(row.hk) as string) ?? true,
			producerId: () => "aa".repeat(32),
			checkPushTarget: (target) => opts.checkPushTarget?.(target.targetId),
			push: async (target, batch) => {
				const call = { targetId: target.targetId, batch };
				calls.push(call);
				return await (opts.answer ?? applyAll)(call);
			},
			writeGsi: async (_target, request) => {
				gsiCalls.push(request);
				invariant(opts.gsi, "this harness has no GSI target");
				return opts.gsi.answer(request);
			},
			gsiEvaluator: () => {
				invariant(opts.gsi, "this harness has no GSI target");
				opts.gsi.plans?.();
				return opts.gsi.desired;
			},
			config: () => ({ ...CONFIG, ...opts.config }),
			scheduleRetry: (runAt) => scheduled.push(runAt),
			now: () => now,
			logParams: () => ({ partition: "test" }),
			// The runner waits only to yield, so the wait returns at once.
			wait: async () => {},
		});
		const image = (hk: string, revision: number, deleted: boolean, data: string): LatestImage =>
			deleted
				? { opIndex: 0, hashKey: kb(hk), sortKey: NO_SK, revision, deleted: true }
				: { opIndex: 0, hashKey: kb(hk), sortKey: NO_SK, revision, deleted: false, data, kind: "text", ttlAt: null };
		await fn({
			store,
			runner,
			calls,
			ingest: (hk, revision, { deleted = false, data = `v${revision}` } = {}) => {
				const current = store.storedImage(kb(hk), NO_SK);
				if (current !== undefined && revision <= current.revision) {
					return;
				}
				const positions = store.targetRows().map((row) => ({
					cursor: row.cursorSeq,
					read: Math.max(row.cursorSeq, runner.readPosition(row.targetId) ?? row.cursorSeq),
				}));
				store.putLatestImage(image(hk, revision, deleted, data), now, current, positions);
				if (deleted && positions.length === 0) {
					cleanup.tombstoneUnblocked();
				}
				runner.wake();
			},
			target: (targetId) => store.targetRow(targetId),
			retryRows: () =>
				state.storage.sql
					.exec<{ target_id: string; hk: ArrayBuffer; kind: string; not_before: number; attempts: number; last_error: string | null }>(
						`SELECT r.target_id, li.base_hk AS hk, r.kind, r.not_before, r.attempts, r.last_error
						 FROM target_retry r JOIN latest_items li ON li.item_id = r.item_id ORDER BY r.target_id, li.base_hk`,
					)
					.toArray()
					.map((row) => ({ ...row, hk: KeyCodec.decode(new Uint8Array(row.hk) as KeyBytes) as string })),
			count: (table) => state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table}`).one().n,
			drain: async () => {
				await vi.waitFor(() => expect(runner.running).toBe(false));
			},
			advance: (ms) => {
				now += ms;
			},
			now: () => now,
			scheduled,
			sql: state.storage.sql,
			gsiCalls,
		});
	});
}

/** A promise that the test resolves later. */
function gate(): { wait: Promise<void>; open: () => void } {
	let open!: () => void;
	const wait = new Promise<void>((resolve) => (open = resolve));
	return { wait, open };
}

describe("TargetRunner — batches and cursors", () => {
	it("sends the latest image of each changed item, moves the cursor, and closes the batch", async () => {
		await withRunner({}, async (h) => {
			h.ingest("a", 1);
			h.ingest("b", 1);
			h.ingest("b", 2, { deleted: true });
			await h.drain();

			expect(h.calls).toHaveLength(1);
			expect(h.calls[0].batch).toEqual({
				producerId: "aa".repeat(32),
				epoch: 1,
				ancestorFences: [],
				items: [
					{ baseHashKey: "a", revision: 1, deleted: false, value: { kind: "text", data: "v1" } },
					{ baseHashKey: "b", revision: 2, deleted: true },
				],
			});
			expect(h.target("t1")).toMatchObject({ cursorSeq: 2, epoch: 1, isBatchOpen: false });
		});
	});

	it("decodes a json item, and sends its sort key and its TTL", async () => {
		await withRunner({}, async (h) => {
			const jsonb = new Uint8Array(h.sql.exec<{ b: ArrayBuffer }>(`SELECT jsonb(?) AS b`, `{"n":1}`).one().b);
			const position = h.store.targetRows().map((row) => ({ cursor: row.cursorSeq, read: row.cursorSeq }));
			h.store.putLatestImage(
				{ opIndex: 0, hashKey: kb("j"), sortKey: kb("s"), revision: 3, deleted: false, data: jsonb, kind: "json", ttlAt: 99 },
				h.now(),
				undefined,
				position,
			);
			h.runner.wake();
			await h.drain();
			expect(h.calls[0].batch.items).toEqual([
				{ baseHashKey: "j", baseSortKey: "s", revision: 3, deleted: false, value: { kind: "json", data: { n: 1 } }, ttlAt: 99 },
			]);
		});
	});

	it("a slow target holds one worker, and the other worker drains the other target", async () => {
		const slow = gate();
		await withRunner(
			{
				targets: ["slow", "fast"],
				answer: async (call) => {
					if (call.targetId === "slow") {
						await slow.wait;
					}
					return applyAll(call);
				},
			},
			async (h) => {
				h.ingest("a", 1);
				await vi.waitFor(() => expect(h.target("fast").cursorSeq).toBe(1));
				h.ingest("b", 1);
				await vi.waitFor(() => expect(h.target("fast").cursorSeq).toBe(2));
				expect(h.calls.filter((c) => c.targetId === "slow")).toHaveLength(1);

				slow.open();
				await h.drain();
				expect(h.calls.filter((c) => c.targetId === "slow").map(sent)).toEqual([["a@1"], ["b@1"]]);
				expect(h.target("slow").cursorSeq).toBe(2);
			},
		);
	});

	it("a target that lags reads the oldest position of an item, and sends the item again from a newer position in a later batch", async () => {
		const lake = gate();
		let lakeCalls = 0;
		await withRunner(
			{
				targets: ["fast", "lake"],
				config: { maxItemsPerStep: 1 },
				answer: async (call) => {
					if (call.targetId === "lake" && lakeCalls++ === 0) {
						await lake.wait;
					}
					return applyAll(call);
				},
			},
			async (h) => {
				// lake holds a batch of z in flight, and fast reads each change at once.
				h.ingest("z", 1);
				await vi.waitFor(() => expect(h.target("fast").cursorSeq).toBe(1));
				h.ingest("x", 1);
				h.ingest("y", 1);
				await vi.waitFor(() => expect(h.target("fast").cursorSeq).toBe(3));
				// fast read the position of x, so x gets a second position.
				h.ingest("x", 2);
				await vi.waitFor(() => expect(h.target("fast").cursorSeq).toBe(4));
				// lake still needs the first position of x, so the second position of x moves.
				h.ingest("x", 3);
				await vi.waitFor(() => expect(h.target("fast").cursorSeq).toBe(5));
				expect(
					h.sql
						.exec<{ seq: number }>(`SELECT seq FROM change_positions ORDER BY seq`)
						.toArray()
						.map((r) => r.seq),
				).toEqual([1, 2, 3, 5]);

				lake.open();
				await h.drain();
				// lake sends the latest revision of x from the first position of x. A batch holds 1 item, so
				// the second position of x is in a later batch, and lake sends x again. The receiver drops it.
				expect(h.calls.filter((c) => c.targetId === "lake").map(sent)).toEqual([["z@1"], ["x@3"], ["y@1"], ["x@3"]]);
				expect(h.target("lake").cursorSeq).toBe(5);
			},
		);
	});
});

describe("TargetRunner — failed items", () => {
	it("a failed item gets a retry row with a backoff, the cursor moves past it, and a newer write sends it at once", async () => {
		await withRunner(
			{
				answer: ({ batch }) =>
					batch.items.map(
						(item): PushTargetResult =>
							item.baseHashKey === "bad" && item.revision === 1 ? { outcome: "retry", code: "busy" } : { outcome: "applied" },
					),
			},
			async (h) => {
				h.ingest("ok", 1);
				h.ingest("bad", 1);
				await h.drain();
				expect(h.target("t1")).toMatchObject({ cursorSeq: 2, isBatchOpen: false });
				expect(h.retryRows()).toEqual([
					{ target_id: "t1", hk: "bad", kind: "retry", not_before: h.now() + 1_000, attempts: 1, last_error: "busy" },
				]);
				// The last worker sets the alarm for the end of the backoff.
				expect(h.scheduled.at(-1)).toBe(h.now() + 1_000);

				// After the backoff, the retry row sends the item again, and the backoff doubles. The retry
				// row comes first in the batch. "ok2" applies in the same call, so the call did not fail as
				// a whole.
				h.advance(1_000);
				h.ingest("ok2", 1);
				await h.drain();
				expect(h.calls.map(sent)).toEqual([
					["ok@1", "bad@1"],
					["bad@1", "ok2@1"],
				]);
				expect(h.retryRows()).toMatchObject([{ attempts: 2, not_before: h.now() + 2_000 }]);

				// A newer write makes the retry row due. The cursor skips the new position, because the
				// retry row owns the item, and the retry row sends the new image.
				h.ingest("bad", 2);
				await h.drain();
				expect(h.calls.at(-1)!.batch.items.map((i) => i.revision)).toEqual([2]);
				expect(h.target("t1").cursorSeq).toBe(4);
				expect(h.retryRows()).toEqual([]);
			},
		);
	});

	it("due retry rows take at most half of the bytes of a batch, so the cursor still moves", async () => {
		const config: Partial<TargetRunnerConfig> = {};
		const data = "x".repeat(600);
		await withRunner(
			{
				config,
				answer: ({ batch }) =>
					batch.items.map(
						(item): PushTargetResult => (String(item.baseHashKey).startsWith("r") ? { outcome: "retry" } : { outcome: "applied" }),
					),
			},
			async (h) => {
				// Each item has the same size. A batch holds 2 items and a half by bytes.
				h.ingest("r1", 1, { data });
				config.maxBytesPerStep = Math.floor(2.5 * h.store.storedImage(kb("r1"), NO_SK)!.estRowBytes);
				h.ingest("o1", 1, { data });
				await h.drain();
				h.ingest("r2", 1, { data });
				h.ingest("o2", 1, { data });
				await h.drain();
				expect(h.retryRows().map((row) => row.hk)).toEqual(["r1", "r2"]);

				// r1 and r2 are due and fail each time. They take at most half of the bytes, so o3 goes
				// into the same batch, and the call does not fail as a whole.
				h.advance(10_000);
				h.ingest("o3", 1, { data });
				await h.drain();
				expect(h.calls.map(sent)).toContainEqual(["r1@1", "o3@1"]);
				expect(h.target("t1").cursorSeq).toBe(5);
			},
		);
	});

	it("a rejected item becomes a violation, a rejected tombstone keeps no row, and a newer write clears the violation", async () => {
		let reject = true;
		await withRunner(
			{
				answer: ({ batch }) =>
					batch.items.map((): PushTargetResult => (reject ? { outcome: "rejected", code: "bad_value" } : { outcome: "applied" })),
			},
			async (h) => {
				h.ingest("live", 1);
				h.ingest("gone", 1);
				await h.drain();
				h.ingest("gone", 2, { deleted: true });
				// The two rows are a violation and the one of the item before its delete.
				await h.drain();
				expect(h.retryRows()).toEqual([
					{ target_id: "t1", hk: "live", kind: "violation", not_before: 0, attempts: 0, last_error: "bad_value" },
				]);
				// A violation row costs the runner nothing: no position and no due row is left.
				expect(h.runner.deadline()).toBeNull();

				reject = false;
				h.ingest("live", 2);
				await h.drain();
				expect(h.retryRows()).toEqual([]);
			},
		);
	});

	it("a call that fails as a whole writes no row and keeps the cursor, and the breaker opens until its probe", async () => {
		let down = true;
		await withRunner(
			{
				answer: ({ batch }) => batch.items.map((): PushTargetResult => (down ? { outcome: "retry" } : { outcome: "applied" })),
			},
			async (h) => {
				h.ingest("a", 1);
				h.ingest("b", 1);
				await h.drain();
				// Three calls fail, and then the breaker stops the calls until the probe.
				expect(h.calls).toHaveLength(3);
				expect(h.retryRows()).toEqual([]);
				expect(h.target("t1")).toMatchObject({ cursorSeq: 0, isBatchOpen: true });
				expect(h.runner.deadline()).toBe(h.now() + 5_000);
				expect(h.scheduled.at(-1)).toBe(h.now() + 5_000);

				// A wake before the probe time sends nothing.
				h.ingest("c", 1);
				await h.drain();
				expect(h.calls).toHaveLength(3);

				down = false;
				h.advance(5_000);
				h.runner.wake();
				await h.drain();
				expect(h.calls.map(sent).at(-1)).toEqual(["a@1", "b@1", "c@1"]);
				expect(h.target("t1")).toMatchObject({ cursorSeq: 3, isBatchOpen: false });
			},
		);
	});

	it("a push target that this instance cannot call is stopped before its claim, and is never due", async () => {
		const errors = vi.spyOn(console, "error").mockImplementation(() => {});
		const stopped = () =>
			errors.mock.calls.filter(
				([fields]) => (fields as { message?: string }).message === "fokos/mat: this instance cannot call a push target, and stops it",
			);
		try {
			await withRunner(
				{
					targets: ["bad", "good"],
					checkPushTarget: (targetId) => {
						if (targetId === "bad") {
							throw new Error("the Worker env has no binding SINK");
						}
					},
				},
				async (h) => {
					h.ingest("a", 1);
					await h.drain();
					expect(h.calls.map((c) => [c.targetId, sent(c)])).toEqual([["good", ["a@1"]]]);
					// No claim: the batch marker, the epoch, and the cursor of the stopped target stay.
					expect(h.target("bad")).toMatchObject({ epoch: 1, cursorSeq: 0, isBatchOpen: false });
					expect(h.retryRows()).toEqual([]);
					// The position above its cursor does not make the stopped target due.
					expect(h.runner.deadline()).toBeNull();

					h.ingest("b", 1);
					await h.drain();
					expect(h.calls.map((c) => [c.targetId, sent(c)])).toEqual([
						["good", ["a@1"]],
						["good", ["b@1"]],
					]);
					expect(stopped()).toEqual([[expect.objectContaining({ targetId: "bad", binding: "SINK" })]]);
				},
			);
		} finally {
			errors.mockRestore();
		}
	});

	it("a push target that does not implement its method fails the call as a whole, and keeps the epoch", async () => {
		let missing = true;
		const errors = vi.spyOn(console, "error").mockImplementation(() => {});
		try {
			await withRunner(
				{
					answer: (call) => {
						if (missing) {
							throw Object.assign(new TypeError('The RPC receiver does not implement "push".'), { remote: true });
						}
						return applyAll(call);
					},
				},
				async (h) => {
					h.ingest("a", 1);
					await h.drain();
					// Three calls fail, and then the breaker waits for its probe.
					expect(h.calls.map((c) => c.batch.epoch)).toEqual([1, 1, 1]);
					expect(h.retryRows()).toEqual([]);
					expect(h.target("t1")).toMatchObject({ epoch: 1, cursorSeq: 0 });
					expect(h.runner.deadline()).toBe(h.now() + 5_000);

					// The target deploys the method, and the probe sends the same item with the same epoch.
					missing = false;
					h.advance(5_000);
					h.runner.wake();
					await h.drain();
					expect(h.calls.map((c) => [c.batch.epoch, sent(c)]).at(-1)).toEqual([1, ["a@1"]]);
					expect(h.target("t1")).toMatchObject({ epoch: 1, cursorSeq: 1, isBatchOpen: false });
				},
			);
		} finally {
			errors.mockRestore();
		}
	});

	it("isMissingRpcMethod matches only a remote TypeError with the message of workerd", () => {
		const remote = (error: Error) => Object.assign(error, { remote: true });
		expect(isMissingRpcMethod(remote(new TypeError('The RPC receiver does not implement "push".')))).toBe(true);
		expect(isMissingRpcMethod(remote(new TypeError('The RPC receiver does not implement the method "push".')))).toBe(true);
		// A local error, another type, or another message is an unknown answer.
		expect(isMissingRpcMethod(new TypeError('The RPC receiver does not implement "push".'))).toBe(false);
		expect(isMissingRpcMethod(remote(new Error('The RPC receiver does not implement "push".')))).toBe(false);
		expect(isMissingRpcMethod(remote(new TypeError("Network connection lost.")))).toBe(false);
	});

	it("an unknown answer raises the epoch and sends the same items again at once", async () => {
		let answers = 0;
		await withRunner(
			{
				answer: (call) => {
					answers += 1;
					if (answers === 1) {
						throw new Error("connection lost");
					}
					// A wrong number of results is an unknown answer too.
					return answers === 2 ? [] : applyAll(call);
				},
			},
			async (h) => {
				h.ingest("a", 1);
				await h.drain();
				expect(h.calls.map((c) => [c.batch.epoch, sent(c)])).toEqual([
					[1, ["a@1"]],
					[2, ["a@1"]],
					[3, ["a@1"]],
				]);
				expect(h.target("t1")).toMatchObject({ epoch: 3, cursorSeq: 1, isBatchOpen: false });
			},
		);
	});

	it("raises the epoch at the first claim after a restart while a batch was open", async () => {
		await withRunner({}, async (h) => {
			h.store.openTargetBatch("t1");
			h.ingest("a", 1);
			await h.drain();
			expect(h.calls.map((c) => c.batch.epoch)).toEqual([2]);
			expect(h.target("t1")).toMatchObject({ epoch: 2, isBatchOpen: false });

			// A later claim of the same instance keeps the epoch.
			h.ingest("b", 1);
			await h.drain();
			expect(h.calls.map((c) => c.batch.epoch)).toEqual([2, 2]);
		});
	});
});

describe("TargetRunner — cleanup", () => {
	it("deletes the positions that each target finished, and the tombstones that each target read", async () => {
		await withRunner({ targets: ["t1", "t2"] }, async (h) => {
			h.ingest("a", 1);
			h.ingest("b", 1);
			await h.drain();
			h.ingest("b", 2, { deleted: true });
			await h.drain();
			expect(h.count("change_positions")).toBe(0);
			expect(h.count("latest_items")).toBe(1);
			// The estimate of the deleted hash key goes with its last row.
			expect(h.count("key_size_estimates")).toBe(1);
		});
	});

	it("keeps a tombstone while a target has a retry row for it, and deletes it after the row goes", async () => {
		let fail = true;
		await withRunner(
			{
				targets: ["t1", "t2"],
				answer: (call) =>
					call.batch.items.map(
						(item): PushTargetResult =>
							call.targetId === "t2" && item.baseHashKey === "x" && fail ? { outcome: "retry" } : { outcome: "applied" },
					),
			},
			async (h) => {
				h.ingest("x", 1);
				h.ingest("y", 1);
				await h.drain();
				// y applies in the same batch, so the failure of x is not a failure of the whole call.
				h.ingest("x", 2, { deleted: true });
				h.ingest("y", 2);
				await h.drain();
				// Each cursor read the tombstone, but t2 has a retry row for it.
				expect(h.count("change_positions")).toBe(0);
				expect(h.count("latest_items")).toBe(2);

				fail = false;
				h.advance(10_000);
				h.runner.wake();
				await h.drain();
				expect(h.retryRows()).toEqual([]);
				expect(h.count("latest_items")).toBe(1);
			},
		);
	});

	it("keeps each position and tombstone while a target has not read it", async () => {
		const down = gate();
		await withRunner(
			{
				targets: ["t1", "t2"],
				answer: async (call) => {
					if (call.targetId === "t2") {
						await down.wait;
					}
					return applyAll(call);
				},
			},
			async (h) => {
				h.ingest("x", 1);
				await vi.waitFor(() => expect(h.target("t1").cursorSeq).toBe(1));
				h.ingest("x", 2, { deleted: true });
				await vi.waitFor(() => expect(h.target("t1").cursorSeq).toBe(2));
				expect(h.count("latest_items")).toBe(1);
				expect(h.count("change_positions")).toBe(2);

				down.open();
				await h.drain();
				expect(h.count("latest_items")).toBe(0);
				expect(h.count("change_positions")).toBe(0);
			},
		);
	});

	it("with no target, deletes each tombstone and writes no position", async () => {
		await withRunner({ targets: [] }, async (h) => {
			h.ingest("x", 1);
			h.ingest("x", 2, { deleted: true });
			await h.drain();
			expect(h.count("change_positions")).toBe(0);
			expect(h.count("latest_items")).toBe(0);
			expect(h.runner.deadline()).toBeNull();

			// A later tombstone moves no cursor, and the cleanup still deletes it.
			h.ingest("y", 1);
			h.ingest("y", 2, { deleted: true });
			await h.drain();
			expect(h.count("latest_items")).toBe(0);
		});
	});
});

describe("TargetRunner — splits", () => {
	const ANCESTOR = "bb".repeat(32);

	it("sends the ancestor fences of a target until its cursor reaches them and no inherited retry row is left", async () => {
		let failA = true;
		const answer = ({ batch }: Call): PushTargetResult[] =>
			batch.items.map((item) => (item.baseHashKey === "a" && failA ? { outcome: "retry" } : { outcome: "applied" }));
		await withRunner({ answer }, async (h) => {
			const fences = () => h.sql.exec(`SELECT ancestor_id, min_epoch, until_id FROM ancestor_fences`).toArray();
			const addFence = () =>
				h.sql.exec(`INSERT INTO ancestor_fences (target_id, ancestor_id, min_epoch, until_id) VALUES ('t1', ?, 3, 2)`, ANCESTOR);
			addFence();
			h.ingest("a", 1);
			h.ingest("b", 1);
			await h.drain();
			expect(h.calls[0].batch.ancestorFences).toEqual([{ ancestorId: ANCESTOR, minEpoch: 3 }]);
			// The cursor reached `until_id`, and the `retry` row of a is not inherited.
			expect(h.target("t1").cursorSeq).toBe(2);
			expect(fences()).toEqual([]);

			// An inherited `retry` row keeps the fences after the cursor reached them.
			h.sql.exec(`UPDATE target_retry SET is_inherited = 1`);
			addFence();
			failA = false;
			h.advance(CONFIG.retryBackoffMaxMs);
			h.runner.wake();
			await h.drain();
			expect(h.calls.map(sent)).toEqual([["a@1", "b@1"], ["a@1"]]);
			expect(h.calls[1].batch.ancestorFences).toEqual([{ ancestorId: ANCESTOR, minEpoch: 3 }]);
			expect(h.retryRows()).toEqual([]);
			expect(fences()).toEqual([]);
		});
	});

	it("keeps the ancestor fences while a promotion snapshot cursor is below them", async () => {
		await withRunner({}, async (h) => {
			const fences = () => h.sql.exec(`SELECT ancestor_id FROM ancestor_fences`).toArray();
			h.sql.exec(`INSERT INTO ancestor_fences (target_id, ancestor_id, min_epoch, until_id) VALUES ('t1', ?, 3, 2)`, ANCESTOR);
			// The promotion child starts from the snapshot cursor 0, so it needs the fence up to 2.
			h.store.storeCursorSnapshot(kb("p"));
			h.ingest("a", 1);
			h.ingest("b", 1);
			await h.drain();
			expect(h.target("t1").cursorSeq).toBe(2);
			expect(fences()).toEqual([{ ancestor_id: ANCESTOR }]);

			h.store.deleteCursorSnapshot(kb("p"));
			h.runner.wake();
			await h.drain();
			expect(fences()).toEqual([]);
		});
	});

	it("moves the backoff of the retry row of a moved item while a promotion runs, and deletes the row after", async () => {
		let moved = false;
		const answer = ({ batch }: Call): PushTargetResult[] =>
			batch.items.map((item) => (item.baseHashKey === "a" ? { outcome: "retry" } : { outcome: "applied" }));
		await withRunner({ answer, owns: (hk) => !(moved && hk === "a") }, async (h) => {
			h.ingest("a", 1);
			h.ingest("b", 1);
			await h.drain();
			expect(h.retryRows()).toMatchObject([{ hk: "a", kind: "retry" }]);
			const calls = h.calls.length;

			moved = true;
			h.store.storeCursorSnapshot(kb("a"));
			h.advance(CONFIG.retryBackoffMaxMs);
			h.runner.wake();
			await h.drain();
			expect(h.calls).toHaveLength(calls);
			expect(h.retryRows()).toMatchObject([{ hk: "a", not_before: h.now() + CONFIG.retryBackoffMaxMs }]);

			// After the completion, the child has the row, so the source deletes it.
			h.store.deleteCursorSnapshot(kb("a"));
			h.advance(CONFIG.retryBackoffMaxMs);
			h.runner.wake();
			await h.drain();
			expect(h.retryRows()).toEqual([]);
			expect(h.calls).toHaveLength(calls);
		});
	});

	it("keeps the positions above a promotion snapshot until the promotion completes", async () => {
		await withRunner({}, async (h) => {
			h.store.storeCursorSnapshot(kb("p"));
			h.ingest("a", 1);
			h.ingest("b", 1);
			await h.drain();
			expect(h.target("t1").cursorSeq).toBe(2);
			expect(h.count("change_positions")).toBe(2);

			h.store.deleteCursorSnapshot(kb("p"));
			h.runner.wake();
			await h.drain();
			expect(h.count("change_positions")).toBe(0);
		});
	});
});

describe("TargetRunner — GSI targets", () => {
	/** The data of an image is its GSI hash key, and "none" gives no entry. */
	const desired = (image: TargetImage): GsiDesiredEntry =>
		image.deleted || image.data === "none"
			? { kind: "absent" }
			: { kind: "present", key: { hk: kb(image.data as string), sk: NO_SK }, data: "d", dataKind: "text", ttlAt: null };
	const applied = (request: GsiWriteEntriesRequest): GsiEntryOutcome[] => request.items.map(() => ({ outcome: "applied" }));
	/** Each call as its entries, `put:<gsiHK>` or `delete:<gsiHK>`. */
	const entries = (calls: GsiWriteEntriesRequest[]) =>
		calls.map((call) => call.items.map((item) => `${item.op}:${String(KeyCodec.decode(item.hashKey))}`));
	const keySet = (h: Harness, hk: string) => {
		const itemId = h.store.storedImage(kb(hk), NO_SK)?.itemId;
		return itemId === undefined ? [] : h.store.indexKeys("t1", itemId).map((key) => String(KeyCodec.decode(key.hk)));
	};

	const sqliteFailures: Array<{ name: string; expr: ExpressionValue; good: object; bad: object }> = [
		{
			name: "integer overflow",
			expr: { fn: "sqlite.abs", args: [{ fn: "*", args: [{ ref: "data", path: "$.n" }, { val: 4_294_967_296 }] }] },
			good: { n: 1 },
			bad: { n: -2_147_483_648 },
		},
		{
			// Four hex encodings make the value 16 times longer (9.6 MB). That is above the string limit of
			// SQLite, which is 2 MB on Cloudflare and between 4 MB and 8 MB in the local runtime.
			name: "string or blob too big",
			expr: [1, 2, 3, 4].reduce<ExpressionValue>((value) => ({ fn: "sqlite.hex", args: [value] }), { ref: "data", path: "$.s" }),
			good: { s: "a" },
			bad: { s: "a".repeat(600_000) },
		},
	];
	for (const field of ["hashKey", "projection"] as const) {
		it.each(sqliteFailures)(
			`a ${field} evaluation with $name parks only that item, cleans up its old entry, and recovers on a newer image`,
			async ({ expr, good, bad }) => {
				const plans = compileGsiPlans({
					kind: "gsi",
					targetId: "t1",
					hashKey: field === "hashKey" ? { fn: "sqlite.concat", args: [{ val: "key" }, expr] } : { ref: "hashKey" },
					...(field === "projection" ? { projection: [{ expr, as: "value" }] } : {}),
				});
				let store!: MaterializationStore;
				await withRunner(
					{
						config: { breakerFailures: 1 },
						gsi: { answer: applied, desired: (image) => desiredGsiEntry(store, plans, image, resolveLimits(undefined)) },
					},
					async (h) => {
						store = h.store;
						const ingest = (hk: string, revision: number, data: object) => {
							const jsonb = new Uint8Array(h.sql.exec<{ b: ArrayBuffer }>(`SELECT jsonb(?) AS b`, JSON.stringify(data)).one().b);
							const positions = store.targetRows().map((row) => ({
								cursor: row.cursorSeq,
								read: Math.max(row.cursorSeq, h.runner.readPosition(row.targetId) ?? row.cursorSeq),
							}));
							store.putLatestImage(
								{ opIndex: 0, hashKey: kb(hk), sortKey: NO_SK, revision, deleted: false, data: jsonb, kind: "json", ttlAt: null },
								h.now(),
								store.storedImage(kb(hk), NO_SK),
								positions,
							);
							h.runner.wake();
						};
						ingest("bad", 1, good);
						await h.drain();
						expect(keySet(h, "bad")).toHaveLength(1);
						ingest("bad", 2, bad);
						ingest("healthy", 1, good);
						await h.drain();
						expect(h.target("t1")).toMatchObject({ cursorSeq: 3, epoch: 1, isBatchOpen: false });
						expect(h.retryRows()).toMatchObject([{ hk: "bad", kind: "violation", last_error: "gsi_sqlite_unrecoverable_error" }]);
						expect(keySet(h, "bad")).toEqual([]);
						expect(keySet(h, "healthy")).toHaveLength(1);
						expect(h.gsiCalls.slice(1).some((call) => call.items.some((item) => item.op === "put"))).toBe(true);
						ingest("bad", 3, good);
						await h.drain();
						expect(h.retryRows()).toEqual([]);
						expect(keySet(h, "bad")).toHaveLength(1);
					},
				);
			},
		);
	}

	it.each(["unexpected evaluator failure", "database or disk is full: SQLITE_FULL"])(
		"an evaluation error that is not a violation (%s) retries only that item and keeps its old entry",
		async (message) => {
			let fail = false;
			await withRunner(
				{
					config: { breakerFailures: 1 },
					gsi: {
						answer: applied,
						desired: (image) => {
							if (fail && KeyCodec.decode(image.hk) === "bad") {
								throw new Error(message);
							}
							return desired(image);
						},
					},
				},
				async (h) => {
					h.ingest("bad", 1, { data: "A" });
					await h.drain();
					fail = true;
					h.ingest("bad", 2, { data: "B" });
					h.ingest("healthy", 1, { data: "C" });
					await h.drain();
					expect(h.target("t1")).toMatchObject({ cursorSeq: 3, epoch: 1, isBatchOpen: false });
					expect(h.retryRows()).toMatchObject([{ hk: "bad", kind: "retry", attempts: 1, last_error: "gsi_evaluation_failed" }]);
					expect(keySet(h, "bad")).toEqual(["A"]);
					expect(keySet(h, "healthy")).toEqual(["C"]);
					h.advance(CONFIG.retryBackoffMinMs);
					h.runner.wake();
					await h.drain();
					expect(h.retryRows()).toMatchObject([{ hk: "bad", kind: "retry", attempts: 2 }]);
					expect(keySet(h, "bad")).toEqual(["A"]);
					fail = false;
					h.advance(CONFIG.retryBackoffMaxMs);
					h.runner.wake();
					await h.drain();
					expect(h.retryRows()).toEqual([]);
					expect(keySet(h, "bad")).toEqual(["B"]);
				},
			);
		},
	);

	it("a full batch of evaluation failures cannot stop a healthy item in a later batch", async () => {
		await withRunner(
			{
				config: { maxItemsPerStep: 1, breakerFailures: 1 },
				gsi: {
					answer: applied,
					desired: (image) => {
						if (KeyCodec.decode(image.hk) !== "healthy") {
							throw new Error("unexpected evaluator failure");
						}
						return desired(image);
					},
				},
			},
			async (h) => {
				h.ingest("bad1", 1);
				h.ingest("bad2", 1);
				h.ingest("healthy", 1, { data: "C" });
				await h.drain();
				expect(h.target("t1")).toMatchObject({ cursorSeq: 3, epoch: 1, isBatchOpen: false });
				expect(h.retryRows()).toMatchObject([
					{ hk: "bad1", kind: "retry" },
					{ hk: "bad2", kind: "retry" },
				]);
				expect(entries(h.gsiCalls)).toEqual([["put:C"]]);
			},
		);
	});

	it("a target that cannot evaluate keeps its cursor, writes no retry row, opens its circuit breaker, and continues when it can", async () => {
		let checks = 0;
		let broken = true;
		await withRunner(
			{
				gsi: {
					answer: applied,
					desired,
					plans: () => {
						checks += 1;
						if (broken) {
							throw new Error("compiled SQL exceeds the SQL limit");
						}
					},
				},
			},
			async (h) => {
				h.ingest("a", 1, { data: "A" });
				h.ingest("b", 1, { data: "B" });
				await h.drain();
				expect(checks).toBe(CONFIG.breakerFailures);
				expect(h.gsiCalls).toEqual([]);
				expect(h.retryRows()).toEqual([]);
				expect(h.target("t1")).toMatchObject({ cursorSeq: 0, isBatchOpen: false });
				expect(h.runner.deadline()).toBe(h.now() + CONFIG.probeIntervalMs);

				// A wake before the probe does not check the target again.
				h.runner.wake();
				await h.drain();
				expect(checks).toBe(CONFIG.breakerFailures);

				broken = false;
				h.advance(CONFIG.probeIntervalMs);
				h.runner.wake();
				await h.drain();
				expect(entries(h.gsiCalls)).toEqual([["put:A", "put:B"]]);
				expect(h.target("t1")).toMatchObject({ cursorSeq: 2, isBatchOpen: false });
				expect(h.retryRows()).toEqual([]);
			},
		);
	});

	it("a key move puts the new entry in one call, and deletes the old entry in a second call", async () => {
		await withRunner({ gsi: { answer: applied, desired } }, async (h) => {
			h.ingest("x", 1, { data: "A" });
			await h.drain();
			expect(entries(h.gsiCalls)).toEqual([["put:A"]]);
			expect(keySet(h, "x")).toEqual(["A"]);

			h.ingest("x", 2, { data: "C" });
			await h.drain();
			expect(entries(h.gsiCalls)).toEqual([["put:A"], ["put:C"], ["delete:A"]]);
			expect(keySet(h, "x")).toEqual(["C"]);
			expect(h.gsiCalls[2].epoch).toBe(h.gsiCalls[1].epoch);
		});
	});

	it("an item with no entry and no key set sends no call", async () => {
		await withRunner({ gsi: { answer: applied, desired } }, async (h) => {
			h.ingest("x", 1, { data: "none" });
			await h.drain();
			expect(h.gsiCalls).toEqual([]);
			expect(h.target("t1").cursorSeq).toBeGreaterThan(0);
		});
	});

	it("a delete with an unknown outcome keeps its key, raises the epoch, and is sent again", async () => {
		let unknown = true;
		const answer = (request: GsiWriteEntriesRequest): GsiEntryOutcome[] => {
			if (unknown && request.items.some((item) => item.op === "delete")) {
				unknown = false;
				return request.items.map(() => ({ outcome: "unknown", code: "internal" }));
			}
			return applied(request);
		};
		await withRunner({ gsi: { answer, desired } }, async (h) => {
			h.ingest("x", 1, { data: "A" });
			await h.drain();
			h.ingest("x", 2, { deleted: true });
			await h.drain();
			expect(entries(h.gsiCalls)).toEqual([["put:A"], ["delete:A"], ["delete:A"]]);
			expect(h.gsiCalls[2].epoch).toBe(h.gsiCalls[1].epoch + 1);
			expect(keySet(h, "x")).toEqual([]);
		});
	});

	it("a put that the GSI rejects deletes each key of the item, and parks the item as a violation", async () => {
		const answer = (request: GsiWriteEntriesRequest): GsiEntryOutcome[] =>
			request.items.map((item) =>
				item.op === "put" && String(KeyCodec.decode(item.hashKey)) === "BAD"
					? { outcome: "rejected", code: "item_data_too_large" }
					: { outcome: "applied" },
			);
		await withRunner({ gsi: { answer, desired } }, async (h) => {
			h.ingest("x", 1, { data: "A" });
			await h.drain();
			h.ingest("x", 2, { data: "BAD" });
			await h.drain();
			expect(entries(h.gsiCalls)).toEqual([["put:A"], ["put:BAD"], ["delete:BAD", "delete:A"]]);
			expect(keySet(h, "x")).toEqual([]);
			expect(h.retryRows()).toMatchObject([{ target_id: "t1", hk: "x", kind: "violation", last_error: "item_data_too_large" }]);
		});
	});

	it("a call above the RPC limit goes again in two halves, and writes no row", async () => {
		const answer = (request: GsiWriteEntriesRequest): GsiEntryOutcome[] =>
			request.items.length > 1 ? request.items.map(() => ({ outcome: "rejected", code: "batch_request_too_large" })) : applied(request);
		await withRunner({ gsi: { answer, desired } }, async (h) => {
			h.ingest("x", 1, { data: "A" });
			h.ingest("y", 1, { data: "B" });
			h.ingest("z", 1, { data: "C" });
			await h.drain();
			expect(entries(h.gsiCalls)).toEqual([["put:A", "put:B", "put:C"], ["put:A", "put:B"], ["put:A"], ["put:B"], ["put:C"]]);
			expect(h.retryRows()).toEqual([]);
			expect(["x", "y", "z"].map((hk) => keySet(h, hk))).toEqual([["A"], ["B"], ["C"]]);
		});
	});

	it("a rejection of the whole call writes no row and keeps the cursor", async () => {
		let reject = true;
		const answer = (request: GsiWriteEntriesRequest): GsiEntryOutcome[] =>
			reject
				? request.items.map(() => ({ outcome: "rejected", code: "partition_context_options_invalid", wholeCall: true }))
				: applied(request);
		await withRunner({ gsi: { answer, desired }, config: { breakerFailures: 1 } }, async (h) => {
			h.ingest("x", 1, { data: "A" });
			await h.drain();
			expect(h.retryRows()).toEqual([]);
			expect(h.target("t1").cursorSeq).toBe(0);
			reject = false;
			h.advance(CONFIG.probeIntervalMs);
			h.runner.wake();
			await h.drain();
			expect(entries(h.gsiCalls).at(-1)).toEqual(["put:A"]);
			expect(h.target("t1").cursorSeq).toBeGreaterThan(0);
		});
	});

	it("a put that the GSI rejects with a code that the item data does not cause gets a retry row", async () => {
		const answer = (request: GsiWriteEntriesRequest): GsiEntryOutcome[] =>
			request.items.map((item) =>
				String(KeyCodec.decode(item.hashKey)) === "B" ? { outcome: "rejected", code: "compiled_plan_refused" } : { outcome: "applied" },
			);
		await withRunner({ gsi: { answer, desired } }, async (h) => {
			h.ingest("x", 1, { data: "A" });
			h.ingest("y", 1, { data: "B" });
			await h.drain();
			expect(h.retryRows()).toMatchObject([{ target_id: "t1", hk: "y", kind: "retry", last_error: "compiled_plan_refused" }]);
			expect(keySet(h, "y")).toEqual(["B"]);
		});
	});
});
