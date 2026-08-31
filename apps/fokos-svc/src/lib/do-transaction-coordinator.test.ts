import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import { isTransactionCommitPendingError, isTransactionUndecidedError, TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import { PartitionDO } from "./do-partition.js";
import { KeyCodec } from "./partition-topology/key-codec.js";
import { ALARM_RECOVERY_BUDGET_MS, IDEMPOTENCY_WINDOW_MS, SWEEP_BATCH_ROWS } from "./transaction-limits.js";
import type { InitiateWriteRequest, InitiateWriteResponse, RejectionReason, TCState } from "./transaction-types.js";

const kb = (s: string) => KeyCodec.encode(s);
const ABSENT_SK = KeyCodec.encodeOptional(undefined);

const TX_ID = "tx-1";
const TOKEN = "tok-1";
const BASE_TIME = 2_000_000_000_000;

afterEach(() => {
	vi.restoreAllMocks();
});

// TypeScript's `private` is compile-time only, so the running instance exposes the coordinator's
// transition helpers. The states under test are otherwise reachable only by exhausting participant
// retry budgets. Direct calls keep the tests deterministic and isolate each storage transition.
type CoordinatorInternals = {
	alarm(): Promise<void>;
	initiateWrite(request: InitiateWriteRequest): Promise<InitiateWriteResponse>;
	loadFinalResponse(transactionId: string, idempotencyToken: string): InitiateWriteResponse;
	drivePrepare(
		transactionId: string,
		idempotencyToken: string,
		coordinatorDoId: string,
		commitRequestBudgetMs?: number,
	): Promise<InitiateWriteResponse>;
	runPrepareRecovery(transactionId: string, idempotencyToken: string, commitRequestBudgetMs?: number): Promise<void>;
	runCommit(transactionId: string, idempotencyToken: string, requestBudgetMs?: number): Promise<void>;
	runCancel(transactionId: string, idempotencyToken: string): Promise<void>;
	stripPayload(transactionId: string): void;
};

function seed(state: DurableObjectState, tcState: TCState, reason?: RejectionReason): void {
	state.storage.sql.exec(`DELETE FROM tc_state`);
	state.storage.sql.exec(`DELETE FROM tc_participants`);
	state.storage.sql.exec(`DELETE FROM tc_items`);
	state.storage.sql.exec(
		`INSERT INTO tc_state (idempotency_token, transaction_id, state, transaction_ts, created_at, rejection_reason_json, operations_hash)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		TOKEN,
		TX_ID,
		tcState,
		1_000,
		1_000,
		reason === undefined ? null : JSON.stringify(reason),
		// loadFinalResponse never reads the fingerprint; any non-null value satisfies the column.
		"0000000000000000",
	);
	// A realistic item set, one row with a sort key and one without. The response must not depend on
	// it: a committed transaction reports no items.
	state.storage.sql.exec(
		`INSERT INTO tc_items (transaction_id, hk, sk, operation, data, data_kind, conditions_json, partition_do_name)
		 VALUES (?, ?, ?, 'put', 'v', 1, NULL, 'p1')`,
		TX_ID,
		kb("hk1"),
		kb("sk1"),
	);
	state.storage.sql.exec(
		`INSERT INTO tc_items (transaction_id, hk, sk, operation, data, data_kind, conditions_json, partition_do_name)
		 VALUES (?, ?, ?, 'delete', NULL, NULL, NULL, 'p1')`,
		TX_ID,
		kb("hk2"),
		ABSENT_SK,
	);
}

function countRows(state: DurableObjectState, table: string): number {
	return state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table}`).toArray()[0].n;
}

function insertState(
	state: DurableObjectState,
	options: {
		token: string;
		transactionId: string;
		state: TCState;
		createdAt: number;
		completedAt?: number | null;
		reason?: RejectionReason;
	},
): void {
	state.storage.sql.exec(
		`INSERT INTO tc_state
			(idempotency_token, transaction_id, state, transaction_ts, created_at, completed_at, rejection_reason_json, operations_hash)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		options.token,
		options.transactionId,
		options.state,
		options.createdAt,
		options.createdAt,
		options.completedAt ?? null,
		options.reason === undefined ? null : JSON.stringify(options.reason),
		"0000000000000000",
	);
}

function insertParticipant(state: DurableObjectState, outcome: { prepare?: string; commit?: string; cancel?: string }): void {
	state.storage.sql.exec(
		`INSERT INTO tc_participants
			(transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome)
		 VALUES (?, 'p1', '{}', ?, ?, ?)`,
		TX_ID,
		outcome.prepare ?? null,
		outcome.commit ?? null,
		outcome.cancel ?? null,
	);
}

// Every table this DO owns — the tc_* tables plus the migrations bookkeeping. The `_cf_*` tables are
// the platform's own and are excluded.
function tableNames(state: DurableObjectState): string[] {
	return state.storage.sql
		.exec<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' ORDER BY name`)
		.toArray()
		.map((r) => r.name);
}

async function withCoordinator(fn: (tc: CoordinatorInternals, state: DurableObjectState) => void | Promise<void>): Promise<void> {
	const stub = TransactionCoordinatorDO.getByName(env.TRANSACTION_COORDINATOR_DO, `tc-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (instance: TransactionCoordinatorDO, state: DurableObjectState) => {
		await fn(instance as unknown as CoordinatorInternals, state);
	});
}

describe("TransactionCoordinatorDO - loadFinalResponse: committed only after every participant confirmed", () => {
	it("reports committed in state COMMITTED", async () => {
		await withCoordinator((tc, state) => {
			seed(state, "COMMITTED");
			// toEqual, not toMatchObject: an item echo reappearing here is a failure, not an extra.
			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toEqual({
				outcome: "committed",
				transactionId: TX_ID,
				idempotencyToken: TOKEN,
			});
		});
	});

	// The decision is durable and PREPARED is final, so these transactions WILL commit — but a
	// straggling participant has not applied yet, and a caller that reads now could see a stale
	// value from it. The answer must be the retryable commit-pending error, never "committed".
	it.each(["PREPARED", "COMMITTING"] as const)("throws the commit-pending error in state %s", async (tcState) => {
		await withCoordinator((tc, state) => {
			seed(state, tcState);
			let err: unknown;
			try {
				tc.loadFinalResponse(TX_ID, TOKEN);
			} catch (e) {
				err = e;
			}
			expect(isTransactionCommitPendingError(err)).toBe(true);
			expect(isTransactionUndecidedError(err)).toBe(false);
			expect(String(err)).toMatch(/commit is pending/);
			expect(String(err)).toMatch(new RegExp(`state=${tcState}`));
		});
	});

	// A cancelled transaction applied nothing anywhere, so outstanding cancel cleanup cannot change
	// what the caller observes.
	it.each(["CANCELLING", "CANCELLED"] as const)("reports cancelled with its reason in state %s", async (tcState) => {
		await withCoordinator((tc, state) => {
			const reason: RejectionReason = { type: "condition_failed", hashKey: "hk1", sortKey: "sk1" };
			seed(state, tcState, reason);
			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toEqual({
				outcome: "cancelled",
				transactionId: TX_ID,
				idempotencyToken: TOKEN,
				reason,
			});
		});
	});

	it("falls back to transient_error when a CANCELLING row has no recorded reason", async () => {
		await withCoordinator((tc, state) => {
			seed(state, "CANCELLING");
			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toMatchObject({
				outcome: "cancelled",
				reason: { type: "transient_error" },
			});
		});
	});

	// The only states where the outcome can still go either way, and so the only other retryable answer.
	it.each(["CREATED", "PREPARING"] as const)("throws the undecided error in state %s", async (tcState) => {
		await withCoordinator((tc, state) => {
			seed(state, tcState);
			let err: unknown;
			try {
				tc.loadFinalResponse(TX_ID, TOKEN);
			} catch (e) {
				err = e;
			}
			expect(isTransactionUndecidedError(err)).toBe(true);
			expect(isTransactionCommitPendingError(err)).toBe(false);
			expect(String(err)).toMatch(/outcome is not yet decided/);
		});
	});
});

describe("TransactionCoordinatorDO - bounded transaction storage", () => {
	it("creates the completed-at column and partial sweep index", async () => {
		await withCoordinator((_tc, state) => {
			const columns = state.storage.sql.exec<{ name: string }>(`PRAGMA table_info(tc_state)`).toArray();
			expect(columns.map((column) => column.name)).toContain("completed_at");
			const index = state.storage.sql
				.exec<{ sql: string }>(`SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_tc_state_completed_at'`)
				.toArray()[0];
			expect(index.sql).toMatch(/WHERE completed_at IS NOT NULL/);
		});
	});

	it("strips payload in the PREPARED transition but retains routing keys", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(`UPDATE tc_items SET conditions_json = '{"op":"test"}' WHERE transaction_id = ?`, TX_ID);
			vi.spyOn(tc, "runCommit").mockResolvedValue();

			await expect(tc.drivePrepare(TX_ID, TOKEN, "coordinator-id", 0)).rejects.toThrow(/commit is pending/);

			const stateRow = state.storage.sql
				.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE idempotency_token = ?`, TOKEN)
				.toArray()[0];
			expect(stateRow.state).toBe("PREPARED");
			const items = state.storage.sql
				.exec<{
					hk: ArrayBuffer;
					sk: ArrayBuffer;
					operation: string;
					data: string | ArrayBuffer | null;
					data_kind: number | null;
					conditions_json: string | null;
				}>(`SELECT hk, sk, operation, data, data_kind, conditions_json FROM tc_items WHERE transaction_id = ?`, TX_ID)
				.toArray();
			expect(items).toHaveLength(2);
			expect(items.every((item) => item.data === null && item.data_kind === null && item.conditions_json === null)).toBe(true);
			expect(items.map((item) => item.operation)).toEqual(["put", "delete"]);
		});
	});

	it("alarm recovery keeps PREPARING payload until prepare receives it", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			insertParticipant(state, {});
			const txPrepare = vi.fn(async (_pCtx: unknown, request: { items: Array<{ data?: string | Uint8Array }> }) => {
				expect(request.items[0].data).toBe("v");
				return { outcome: "accepted" as const };
			});
			const txCommit = vi.fn(async () => ({ outcome: "committed" as const }));
			vi.spyOn(PartitionDO, "getByName").mockReturnValue({ txPrepare, txCommit } as unknown as DurableObjectStub<PartitionDO>);

			await tc.alarm();

			expect(txPrepare).toHaveBeenCalledTimes(1);
			expect(txCommit).toHaveBeenCalledTimes(1);
			expect(
				state.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE idempotency_token = ?`, TOKEN).toArray()[0].state,
			).toBe("COMMITTED");
		});
	});

	it("strips payload in the CANCELLING transition", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(`UPDATE tc_items SET conditions_json = '{"op":"test"}' WHERE transaction_id = ?`, TX_ID);
			insertParticipant(state, { prepare: "rejected" });
			vi.spyOn(tc, "runCancel").mockResolvedValue();

			await tc.runPrepareRecovery(TX_ID, TOKEN);

			expect(
				state.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE idempotency_token = ?`, TOKEN).toArray()[0].state,
			).toBe("CANCELLING");
			const payload = state.storage.sql
				.exec<{
					data: string | ArrayBuffer | null;
					data_kind: number | null;
					conditions_json: string | null;
				}>(`SELECT data, data_kind, conditions_json FROM tc_items WHERE transaction_id = ?`, TX_ID)
				.toArray();
			expect(payload.every((item) => item.data === null && item.data_kind === null && item.conditions_json === null)).toBe(true);
		});
	});

	it("sets completed_at, deletes per-transaction rows, and keeps the committed replay", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "COMMITTING");
			insertParticipant(state, { prepare: "accepted", commit: "committed" });

			await tc.runCommit(TX_ID, TOKEN);

			const row = state.storage.sql
				.exec<{
					state: TCState;
					completed_at: number | null;
				}>(`SELECT state, completed_at FROM tc_state WHERE idempotency_token = ?`, TOKEN)
				.toArray()[0];
			expect(row).toMatchObject({ state: "COMMITTED", completed_at: expect.any(Number) });
			expect(countRows(state, "tc_items")).toBe(0);
			expect(countRows(state, "tc_participants")).toBe(0);
			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toEqual({ outcome: "committed", transactionId: TX_ID, idempotencyToken: TOKEN });
		});
	});

	it("sets completed_at, deletes per-transaction rows, and keeps the cancelled replay", async () => {
		await withCoordinator(async (tc, state) => {
			const reason: RejectionReason = { type: "condition_failed", hashKey: "hk1" };
			seed(state, "CANCELLING", reason);
			insertParticipant(state, { prepare: "rejected", cancel: "cancelled" });

			await tc.runCancel(TX_ID, TOKEN);

			const row = state.storage.sql
				.exec<{
					state: TCState;
					completed_at: number | null;
				}>(`SELECT state, completed_at FROM tc_state WHERE idempotency_token = ?`, TOKEN)
				.toArray()[0];
			expect(row).toMatchObject({ state: "CANCELLED", completed_at: expect.any(Number) });
			expect(countRows(state, "tc_items")).toBe(0);
			expect(countRows(state, "tc_participants")).toBe(0);
			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toEqual({
				outcome: "cancelled",
				transactionId: TX_ID,
				idempotencyToken: TOKEN,
				reason,
			});
		});
	});

	it("retains item and participant rows while a commit is unconfirmed", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "COMMITTING");
			tc.stripPayload(TX_ID);
			insertParticipant(state, { prepare: "accepted" });

			await tc.runCommit(TX_ID, TOKEN, -1);

			const row = state.storage.sql
				.exec<{
					state: TCState;
					completed_at: number | null;
				}>(`SELECT state, completed_at FROM tc_state WHERE idempotency_token = ?`, TOKEN)
				.toArray()[0];
			expect(row).toEqual({ state: "COMMITTING", completed_at: null });
			expect(countRows(state, "tc_items")).toBe(2);
			expect(countRows(state, "tc_participants")).toBe(1);
		});
	});
});

describe("TransactionCoordinatorDO - idempotency sweep", () => {
	it("deletes one batch and re-arms immediately while expired rows remain", async () => {
		await withCoordinator(async (tc, state) => {
			vi.spyOn(Date, "now").mockReturnValue(BASE_TIME);
			for (let i = 0; i < SWEEP_BATCH_ROWS + 3; i++) {
				insertState(state, {
					token: `expired-${i}`,
					transactionId: `tx-expired-${i}`,
					state: "COMMITTED",
					createdAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
					completedAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
				});
			}

			await tc.alarm();

			expect(countRows(state, "tc_state")).toBe(3);
			expect(await state.storage.getAlarm()).toBe(BASE_TIME);
			await state.storage.deleteAlarm();
			await tc.alarm();
			expect(countRows(state, "tc_state")).toBe(0);
		});
	});

	it("re-arms an idle shard until its last completed row expires", async () => {
		await withCoordinator(async (tc, state) => {
			let now = BASE_TIME;
			vi.spyOn(Date, "now").mockImplementation(() => now);
			insertState(state, {
				token: "idle-token",
				transactionId: "idle-tx",
				state: "COMMITTED",
				createdAt: BASE_TIME,
				completedAt: BASE_TIME,
			});

			await tc.alarm();
			expect(countRows(state, "tc_state")).toBe(1);
			expect(await state.storage.getAlarm()).toBe(BASE_TIME + IDEMPOTENCY_WINDOW_MS + 1);

			now = BASE_TIME + IDEMPOTENCY_WINDOW_MS + 1;
			await state.storage.deleteAlarm();
			await tc.alarm();
			expect(countRows(state, "tc_state")).toBe(0);
			expect(await state.storage.getAlarm()).toBeNull();
		});
	});

	it("treats a token as a new transaction after its completed row expires", async () => {
		await withCoordinator(async (tc, state) => {
			vi.spyOn(Date, "now").mockReturnValue(BASE_TIME);
			const oldTransactionId = "expired-replay-tx";
			insertState(state, {
				token: TOKEN,
				transactionId: oldTransactionId,
				state: "COMMITTED",
				createdAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
				completedAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
			});

			await tc.alarm();
			const result = await tc.initiateWrite({ clientRequestToken: TOKEN, items: [] });

			expect(result.outcome).toBe("committed");
			expect(result.transactionId).not.toBe(oldTransactionId);
			expect(countRows(state, "tc_state")).toBe(1);
		});
	});

	it("runs the sweep after the recovery budget is exhausted", async () => {
		await withCoordinator(async (tc, state) => {
			let now = BASE_TIME;
			vi.spyOn(Date, "now").mockImplementation(() => now);
			insertState(state, {
				token: "recover-1",
				transactionId: "recover-tx-1",
				state: "PREPARING",
				createdAt: BASE_TIME - 10_000,
			});
			insertState(state, {
				token: "recover-2",
				transactionId: "recover-tx-2",
				state: "PREPARING",
				createdAt: BASE_TIME - 9_000,
			});
			insertState(state, {
				token: "expired-during-recovery",
				transactionId: "expired-during-recovery-tx",
				state: "COMMITTED",
				createdAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
				completedAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
			});
			const recover = vi.spyOn(tc, "runPrepareRecovery").mockImplementation(async () => {
				now += ALARM_RECOVERY_BUDGET_MS;
			});

			await tc.alarm();

			expect(recover).toHaveBeenCalledTimes(1);
			expect(
				state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM tc_state WHERE completed_at IS NOT NULL`).toArray()[0].n,
			).toBe(0);
			expect(countRows(state, "tc_state")).toBe(2);
		});
	});
});

describe("TransactionCoordinatorDO - destroyCoordinator", () => {
	// The idempotency window lives in tc_state. A coordinator that survives FokosDB.destroy() answers a
	// replayed clientRequestToken with the old transaction's outcome — "committed" for data that was
	// wiped with the partitions.
	it("wipes the idempotency window and the alarm, then evicts the instance", async () => {
		const stub = TransactionCoordinatorDO.getByName(env.TRANSACTION_COORDINATOR_DO, `tc-destroy.${crypto.randomUUID()}`);

		await runInDurableObject(stub, async (tc: TransactionCoordinatorDO, state: DurableObjectState) => {
			seed(state, "COMMITTED");
			await state.storage.setAlarm(Date.now() + 60_000);
			expect(countRows(state, "tc_state")).toBe(1);
			expect(countRows(state, "tc_items")).toBe(2);
			expect(tableNames(state)).toContain("tc_participants");

			// ctx.abort() genuinely evicts the instance, which hangs the workers pool — the same reason
			// test/destroy.test.ts is skipped. Stubbing it keeps the eviction assertable (it is what makes
			// the next caller re-run the migrations) without killing the run.
			const abort = vi.spyOn(state, "abort").mockImplementation(() => {});

			await tc.destroyCoordinator();

			expect(abort).toHaveBeenCalledWith("__special_destroy_sentinel");
			// deleteAll() drops the tables themselves, migration bookkeeping included — which is exactly
			// why the instance must be evicted: the next caller re-creates them from the migrations.
			expect(tableNames(state)).toEqual([]);
			// A surviving alarm would fire after the wipe and try to drive transactions whose rows are gone.
			expect(await state.storage.getAlarm()).toBeNull();
		});
	});
});
