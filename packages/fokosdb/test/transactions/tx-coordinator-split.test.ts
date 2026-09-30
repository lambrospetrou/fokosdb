// A transaction coordinator is a host of the sharding runtime, keyed by the idempotency token. Its pool
// grows by hash splits: a split moves the ledger rows of each token to the child that owns the token,
// and the root forwards every later call for that token.
//
// A test forces a split of the one root coordinator of a `controlled` table: it reports a database
// size above the split threshold while it asks the runtime to evaluate a split.
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import type { TransactionCoordinatorDO } from "../../src/server/do-transaction-coordinator.js";
import { createTableConfig, type FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { DEFAULT_LIMITS, encodeHashKey, txOrderTimestampNow } from "../../src/shared/transaction-limits.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import type { FokosImportRecord } from "../../src/sharding/repartition-types.js";
import type { FokosPartitionRef } from "../../src/sharding/route-context.js";
import { FOKOS_KV_KEYS } from "../../src/sharding/sharding-store.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { captureConsoleError } from "../partition-do/helpers.js";
import {
	controlledPartition,
	coordinatorRouter,
	keysAcrossPartitions,
	makeDB,
	owningPartition,
	txCalls,
	writeOutcome,
} from "./tx-helpers.js";

const SPLIT_TIMEOUT_MS = 15_000;
const FLOOR_MESSAGE = "fokos/tc: over the split threshold, but a split cannot make this coordinator smaller";

function coordinatorStub(doName: string) {
	return env.CONTROLLED_TRANSACTION_COORDINATOR_DO.getByName(doName);
}

function tokenKey(token: string) {
	return { hashKey: encodeHashKey(token, DEFAULT_LIMITS), sortKey: KeyCodec.encodeOptional(undefined) };
}

/** Writes a committed ledger row under a new token, as a transaction that completed now writes it. */
function insertCommittedToken(state: DurableObjectState, token: string): void {
	state.storage.sql.exec(
		`INSERT INTO tc_state (transaction_id, idempotency_token, state, transaction_ts, created_at, completed_at, results_json, operations_hash)
		 VALUES (?, ?, 'COMMITTED', 1, ?, ?, '[]', '0000000000000000')`,
		`tx-${crypto.randomUUID()}`,
		token,
		Date.now(),
		Date.now(),
	);
}

/**
 * Queues a hash split of the coordinator. The coordinator must already hold its identity. A split
 * needs two tokens, so the helper adds a committed token of its own.
 */
async function queueCoordinatorSplit(coordinator: FokosDBRouteContext): Promise<void> {
	await runInDurableObject(coordinatorStub(coordinator.doName), async (instance: TransactionCoordinatorDO, state: DurableObjectState) => {
		insertCommittedToken(state, `tc-split-filler-${crypto.randomUUID()}`);
		const size = vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(Number.MAX_SAFE_INTEGER);
		try {
			instance.fokos.requestSplitEvaluation();
			await vi.waitFor(() => expect(instance.fokos.lifecycle().activeRepartition).not.toBeNull(), { timeout: 5_000, interval: 10 });
		} finally {
			size.mockRestore();
		}
	});
}

async function roleOf(coordinator: FokosDBRouteContext): Promise<string> {
	return await runInDurableObject(
		coordinatorStub(coordinator.doName),
		(instance: TransactionCoordinatorDO) => instance.fokos.lifecycle().role,
	);
}

/**
 * Waits until the split of `coordinator` is complete and every child has finished its import. Returns
 * the child that owns `token`.
 */
async function awaitSplitSettled(coordinator: FokosDBRouteContext, token: string): Promise<FokosPartitionRef> {
	const stub = coordinatorStub(coordinator.doName);
	let owner: FokosPartitionRef | null = null;
	await vi.waitFor(
		async () => {
			const { children, settled } = await runInDurableObject(stub, (instance: TransactionCoordinatorDO) => {
				const lifecycle = instance.fokos.lifecycle();
				const resolved = instance.fokos.resolveOwner(tokenKey(token));
				if (resolved.kind === "remote") {
					owner = resolved.target;
				}
				return {
					children: instance.fokos.children(),
					settled: lifecycle.role === "router" && lifecycle.activeRepartition === null,
				};
			});
			expect(settled).toBe(true);
			for (const child of children) {
				const importState = await runInDurableObject(
					coordinatorStub(child.ref.doName),
					(instance: TransactionCoordinatorDO) => instance.fokos.lifecycle().import?.state,
				);
				expect(importState).toBe("active");
			}
		},
		{ timeout: SPLIT_TIMEOUT_MS, interval: 20 },
	);
	expect(owner).not.toBeNull();
	return owner!;
}

describe("transactions - the coordinator pool grows by hash split", () => {
	it("moves a COMMITTED ledger row to the child, and a replay of its token answers from the child", async () => {
		const db = makeDB({ controlled: true });
		const items = keysAcrossPartitions(db, 2, "tc-split-ledger").map((key) => ({ ...key, operation: "put" as const, data: "v" }));
		const token = `tc-split-ledger-${crypto.randomUUID()}`;
		const first = await writeOutcome(db.transactWriteItems({ items, clientRequestToken: token }));
		expect(first.outcome).toBe("committed");

		const root = coordinatorRouter(db).allRoots()[0];
		await queueCoordinatorSplit(root);
		const owner = await awaitSplitSettled(root, token);

		await runInDurableObject(coordinatorStub(owner.doName), (_instance: TransactionCoordinatorDO, state: DurableObjectState) => {
			const rows = state.storage.sql.exec(`SELECT transaction_id, state FROM tc_state WHERE idempotency_token = ?`, token).toArray();
			expect(rows).toEqual([{ transaction_id: first.transactionId, state: "COMMITTED" }]);
		});

		const replay = await writeOutcome(db.transactWriteItems({ items, clientRequestToken: token }));
		expect(replay).toEqual(first);
		// The root forwarded the replay: the child received the call on its own RPC method.
		expect(await coordinatorStub(owner.doName).testInitiateWriteCalls()).toBe(1);
	});

	it("stops the source at the PREPARED transition after a cutover, and the child resumes the same transaction once", async () => {
		const db = makeDB({ controlled: true });
		const keys = keysAcrossPartitions(db, 2, "tc-split-prepare");
		const items = keys.map((key) => ({ ...key, operation: "put" as const, data: "v" }));
		const token = `tc-split-prepare-${crypto.randomUUID()}`;
		const root = coordinatorRouter(db).allRoots()[0];

		// The lock of one participant is written, and its answer waits: the coordinator is in PREPARING.
		const held = controlledPartition(db, keys[0]);
		await held.testHoldPrepare();
		try {
			const write = writeOutcome(db.transactWriteItems({ items, clientRequestToken: token }));
			// The caller gets a failure of the write from the `await` below.
			write.catch(() => {});
			await vi.waitFor(async () => expect(await held.testPrepareParked()).toBe(true), { timeout: 5_000, interval: 10 });

			await queueCoordinatorSplit(root);
			await vi.waitFor(async () => expect(await roleOf(root)).toBe("router"), { timeout: SPLIT_TIMEOUT_MS, interval: 20 });
			await held.testReleasePrepare();

			// The source answers `partition_migrating` at PREPARED, and db.ts retries with the same token
			// until the child has imported the ledger row and resumes it.
			const result = await write;
			expect(result.outcome).toBe("committed");

			// One transaction, committed once on each participant, under the id the source created.
			const commits = await txCalls(db, keys, "txCommit");
			expect(commits.map((commit) => commit.transactionId)).toEqual([result.transactionId, result.transactionId]);
			const owner = await awaitSplitSettled(root, token);
			await runInDurableObject(coordinatorStub(owner.doName), (_instance: TransactionCoordinatorDO, state: DurableObjectState) => {
				const rows = state.storage.sql.exec(`SELECT transaction_id, state FROM tc_state WHERE idempotency_token = ?`, token).toArray();
				expect(rows).toEqual([{ transaction_id: result.transactionId, state: "COMMITTED" }]);
			});
		} finally {
			await held.testReleasePrepare();
		}
	});

	it("drives a non-terminal transaction that a split moved to a child, with no request for it", async () => {
		const db = makeDB({ controlled: true });
		const items = keysAcrossPartitions(db, 2, "tc-split-job").map((key) => ({ ...key, operation: "put" as const, data: "v" }));
		const token = `tc-split-job-${crypto.randomUUID()}`;
		await writeOutcome(db.transactWriteItems({ items, clientRequestToken: `${token}-first` }));

		// A decided transaction whose driver stopped: it waits in CANCELLING, and no participant is left
		// to confirm, so one recovery step completes it.
		const root = coordinatorRouter(db).allRoots()[0];
		const transactionId = `tx-${crypto.randomUUID()}`;
		await runInDurableObject(coordinatorStub(root.doName), (_instance: TransactionCoordinatorDO, state: DurableObjectState) => {
			state.storage.sql.exec(
				`INSERT INTO tc_state (transaction_id, idempotency_token, state, transaction_ts, created_at, results_json, operations_hash)
				 VALUES (?, ?, 'CANCELLING', 1, ?, '[]', '0000000000000000')`,
				transactionId,
				token,
				Date.now() - 60_000,
			);
		});

		await queueCoordinatorSplit(root);
		const owner = await awaitSplitSettled(root, token);

		await vi.waitFor(
			async () => {
				const rows = await runInDurableObject(
					coordinatorStub(owner.doName),
					(_instance: TransactionCoordinatorDO, state: DurableObjectState) =>
						state.storage.sql.exec(`SELECT state FROM tc_state WHERE transaction_id = ?`, transactionId).toArray(),
				);
				expect(rows).toEqual([{ state: "CANCELLED" }]);
			},
			{ timeout: 10_000, interval: 50 },
		);
	});

	it("answers the stale recovery of a partition through a coordinator that became a router", async () => {
		const db = makeDB({ controlled: true });
		const items = keysAcrossPartitions(db, 2, "tc-split-recovery").map((key) => ({ ...key, operation: "put" as const, data: "v" }));
		const token = `tc-split-recovery-${crypto.randomUUID()}`;
		const first = await writeOutcome(db.transactWriteItems({ items, clientRequestToken: token }));
		expect(first.outcome).toBe("committed");

		const root = coordinatorRouter(db).allRoots()[0];
		await queueCoordinatorSplit(root);
		await awaitSplitSettled(root, token);

		// A stale lock of the committed transaction whose commit never reached its partition. The lock
		// names the root coordinator, as the prepare wrote it before the split.
		const lockKey = { hashKey: `tc-split-recovery-lock-${crypto.randomUUID()}`, sortKey: "sk" };
		const { stub, rpc, pCtx } = owningPartition(db, lockKey);
		await rpc.status(pCtx);
		await runInDurableObject(stub, async (_instance, state: DurableObjectState) => {
			new PartitionStore(state.storage).insertPendingLock({
				hk: KeyCodec.encode(lockKey.hashKey),
				sk: KeyCodec.encode(lockKey.sortKey),
				transaction_id: first.transactionId,
				transaction_ts: txOrderTimestampNow(),
				operation: "put",
				data: "recovered",
				kind: "text",
				ttl_epoch_utc_seconds: null,
				coordinator_json: JSON.stringify({ v: 1, doName: root.doName, idempotencyToken: token }),
				created_at: Date.now() - 60_000,
				guarded_at: null,
				next_recovery_at: Date.now() - 55_000,
			});
			await state.storage.setAlarm(Date.now());
		});

		await runDurableObjectAlarm(stub);
		await vi.waitFor(async () => expect(await db.getItem(lockKey)).toMatchObject({ found: true, item: { data: "recovered" } }), {
			timeout: 10_000,
			interval: 50,
		});
		expect(await roleOf(root)).toBe("router");
	});

	it("splits a coordinator with two tokens when it refuses a new transaction for size", async () => {
		const db = makeDB({ controlled: true });
		const items = keysAcrossPartitions(db, 2, "tc-split-refused").map((key) => ({ ...key, operation: "put" as const, data: "v" }));
		const token = `tc-split-refused-${crypto.randomUUID()}`;
		expect((await writeOutcome(db.transactWriteItems({ items, clientRequestToken: `${token}-first` }))).outcome).toBe("committed");

		const root = coordinatorRouter(db).allRoots()[0];
		await runInDurableObject(coordinatorStub(root.doName), async (instance: TransactionCoordinatorDO, state: DurableObjectState) => {
			insertCommittedToken(state, `${token}-second`);
			const size = vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(Number.MAX_SAFE_INTEGER);
			try {
				await expect(
					instance.initiateWrite(instance.fokos.routeContext(), {
						clientRequestToken: token,
						table: createTableConfig(db.options()),
						items: [],
					}),
				).rejects.toThrow(fokosErrorWith("coordinator_over_size"));
				await vi.waitFor(() => expect(instance.fokos.lifecycle().activeRepartition).not.toBeNull(), { timeout: 5_000, interval: 10 });
			} finally {
				size.mockRestore();
			}
		});
		await awaitSplitSettled(root, token);

		expect((await writeOutcome(db.transactWriteItems({ items, clientRequestToken: token }))).outcome).toBe("committed");
	});

	it("does not split a coordinator with one token above its cap, and logs the floor once", async () => {
		const db = makeDB({ controlled: true });
		const items = keysAcrossPartitions(db, 2, "tc-split-floor").map((key) => ({ ...key, operation: "put" as const, data: "v" }));
		const token = `tc-split-floor-${crypto.randomUUID()}`;
		expect((await writeOutcome(db.transactWriteItems({ items, clientRequestToken: token }))).outcome).toBe("committed");

		const root = coordinatorRouter(db).allRoots()[0];
		const logged = captureConsoleError();
		try {
			await runInDurableObject(coordinatorStub(root.doName), async (instance: TransactionCoordinatorDO, state: DurableObjectState) => {
				const floorLogs = () => logged.withMessage(FLOOR_MESSAGE);
				const size = vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(Number.MAX_SAFE_INTEGER);
				try {
					// Each refusal runs the decision before it throws.
					for (const attempt of [1, 2]) {
						await expect(
							instance.initiateWrite(instance.fokos.routeContext(), {
								clientRequestToken: `${token}-refused-${attempt}`,
								table: createTableConfig(db.options()),
								items: [],
							}),
						).rejects.toThrow(fokosErrorWith("coordinator_over_size"));
					}
					expect(floorLogs()).toEqual([expect.objectContaining({ doName: root.doName, maxSizeBytes: expect.any(Number) })]);

					// A decision under the cap clears the flag, so the next decision at the floor logs again.
					size.mockReturnValue(0);
					instance.fokos.requestSplitEvaluation();
					size.mockReturnValue(Number.MAX_SAFE_INTEGER);
					instance.fokos.requestSplitEvaluation();
					expect(floorLogs()).toHaveLength(2);
				} finally {
					size.mockRestore();
				}
				expect(instance.fokos.lifecycle()).toMatchObject({ role: "owner", activeRepartition: null });
			});
		} finally {
			logged.spy.mockRestore();
		}
	});

	it("refuses initiateWrite and recoverTransaction with partition_migrating while a coordinator imports", async () => {
		const db = makeDB({ controlled: true });
		const items = keysAcrossPartitions(db, 2, "tc-split-importing").map((key) => ({ ...key, operation: "put" as const, data: "v" }));
		const token = `tc-split-importing-${crypto.randomUUID()}`;
		const first = await writeOutcome(db.transactWriteItems({ items, clientRequestToken: token }));

		const root = coordinatorRouter(db).allRoots()[0];
		await queueCoordinatorSplit(root);
		const owner = await awaitSplitSettled(root, token);
		const child = coordinatorStub(owner.doName);

		const record = await runInDurableObject(child, (_instance: TransactionCoordinatorDO, state: DurableObjectState) => {
			const imported = state.storage.kv.get<FokosImportRecord>(FOKOS_KV_KEYS.IMPORT)!;
			state.storage.kv.put<FokosImportRecord>(FOKOS_KV_KEYS.IMPORT, { ...imported, state: "importing" });
			return imported;
		});
		try {
			// runInDurableObject keeps the caught rejection inside the DO's execution context, so it does not
			// leak as an unhandled rejection at the worker level.
			await runInDurableObject(child, async (instance: TransactionCoordinatorDO) => {
				const childCtx = instance.fokos.routeContext();
				await expect(
					instance.initiateWrite(childCtx, { clientRequestToken: token, table: createTableConfig(db.options()), items: [] }),
				).rejects.toThrow(fokosErrorWith("partition_migrating"));
				await expect(
					instance.recoverTransaction(childCtx, { transactionId: first.transactionId, idempotencyToken: token }),
				).rejects.toThrow(fokosErrorWith("partition_migrating"));
			});
		} finally {
			await runInDurableObject(child, (_instance: TransactionCoordinatorDO, state: DurableObjectState) => {
				state.storage.kv.put<FokosImportRecord>(FOKOS_KV_KEYS.IMPORT, record);
			});
		}
	});
});
