import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import { PartitionDO } from "./do-partition.js";
import { FokosError, FokosUnavailableError, TRANSACTION_PENDING_CODES, UNAVAILABLE_CODES, type FokosErrorWire } from "../shared/errors.js";
import { KeyCodec } from "../shared/partition-topology/key-codec.js";
import { ALARM_RECOVERY_BUDGET_MS, IDEMPOTENCY_WINDOW_MS, MAX_TC_DATABASE_BYTES, SWEEP_BATCH_ROWS } from "../shared/transaction-limits.js";
import { hashTransactionOperations } from "../shared/transaction-idempotency.js";
import type {
	InitiateWriteRequest,
	InitiateWriteResponseEncoded,
	PrepareResponse,
	TCState,
	TransactWriteOperationResultEncoded,
} from "../shared/transaction-types.js";

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
	initiateWrite(request: InitiateWriteRequest): Promise<InitiateWriteResponseEncoded>;
	recoverTransaction(transactionId: string): Promise<unknown>;
	loadFinalResponse(transactionId: string, idempotencyToken: string): InitiateWriteResponseEncoded;
	cancelTransactionInStore(transactionId: string): void;
	drivePrepare(
		transactionId: string,
		idempotencyToken: string,
		coordinatorDoId: string,
		commitRequestBudgetMs?: number,
	): Promise<InitiateWriteResponseEncoded>;
	runPrepareRecovery(transactionId: string, idempotencyToken: string, commitRequestBudgetMs?: number): Promise<void>;
	runCommit(transactionId: string, idempotencyToken: string, requestBudgetMs?: number): Promise<void>;
	runCancel(transactionId: string, idempotencyToken: string): Promise<void>;
	stripPayload(transactionId: string): void;
};

function seed(state: DurableObjectState, tcState: TCState, results?: TransactWriteOperationResultEncoded[], createdAt?: number): void {
	state.storage.sql.exec(`DELETE FROM tc_state`);
	state.storage.sql.exec(`DELETE FROM tc_participants`);
	state.storage.sql.exec(`DELETE FROM tc_items`);
	const now = createdAt ?? Date.now() - 10_000;
	state.storage.sql.exec(
		`INSERT INTO tc_state (idempotency_token, transaction_id, state, transaction_ts, created_at, results_json, operations_hash)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		TOKEN,
		TX_ID,
		tcState,
		now,
		now,
		results === undefined ? null : JSON.stringify(results),
		// loadFinalResponse never reads the fingerprint; any non-null value satisfies the column.
		"0000000000000000",
	);
	// A realistic item set, one row with a sort key and one without. The response must not depend on
	// it: a committed transaction reports no items.
	state.storage.sql.exec(
		`INSERT INTO tc_items (transaction_id, hk, sk, op_index, operation, data, data_kind, conditions_json, partition_do_name)
		 VALUES (?, ?, ?, 0, 'put', 'v', 1, NULL, 'p1')`,
		TX_ID,
		kb("hk1"),
		kb("sk1"),
	);
	state.storage.sql.exec(
		`INSERT INTO tc_items (transaction_id, hk, sk, op_index, operation, data, data_kind, conditions_json, partition_do_name)
		 VALUES (?, ?, ?, 1, 'delete', NULL, NULL, NULL, 'p1')`,
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
		results?: TransactWriteOperationResultEncoded[];
		operationsHash?: string;
	},
): void {
	state.storage.sql.exec(
		`INSERT INTO tc_state
			(idempotency_token, transaction_id, state, transaction_ts, created_at, completed_at, results_json, operations_hash)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		options.token,
		options.transactionId,
		options.state,
		options.createdAt,
		options.createdAt,
		options.completedAt ?? null,
		options.results === undefined ? null : JSON.stringify(options.results),
		options.operationsHash ?? "0000000000000000",
	);
}

function insertParticipant(
	state: DurableObjectState,
	outcome: { prepare?: string; commit?: string; cancel?: string; name?: string; answer?: PrepareResponse; error?: FokosErrorWire },
): void {
	state.storage.sql.exec(
		`INSERT INTO tc_participants
			(transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome, answer_json, error_json)
		 VALUES (?, ?, '{}', ?, ?, ?, ?, ?)`,
		TX_ID,
		outcome.name ?? "p1",
		outcome.prepare ?? null,
		outcome.commit ?? null,
		outcome.cancel ?? null,
		outcome.answer === undefined ? null : JSON.stringify(outcome.answer),
		outcome.error === undefined ? null : JSON.stringify(outcome.error),
	);
}

function insertImage(state: DurableObjectState, transactionId: string, opIndex: number, data: string): void {
	state.storage.sql.exec(
		`INSERT INTO tc_results (transaction_id, op_index, image_kind, image_version, image_ttl_epoch_utc_seconds, image_data)
		 VALUES (?, ?, 1, 1, NULL, ?)`,
		transactionId,
		opIndex,
		data,
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
			expect(FokosError.isCode(err, TRANSACTION_PENDING_CODES.transaction_commit_pending)).toBe(true);
			expect(FokosError.isCode(err, TRANSACTION_PENDING_CODES.transaction_undecided)).toBe(false);
			expect(err).toMatchObject({ code: "transaction_commit_pending", attributes: { transactionId: TX_ID, state: tcState } });
		});
	});

	// A cancelled transaction applied nothing anywhere, so outstanding cancel cleanup cannot change
	// what the caller observes.
	it.each(["CANCELLING", "CANCELLED"] as const)("reports cancelled with its results in state %s", async (tcState) => {
		await withCoordinator((tc, state) => {
			const results: TransactWriteOperationResultEncoded[] = [
				{ outcome: "rejected", reason: { code: "condition_failed", hashKey: "hk1", sortKey: "sk1" } },
				{ outcome: "passed" },
			];
			seed(state, tcState, results);
			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toEqual({ outcome: "cancelled", transactionId: TX_ID, idempotencyToken: TOKEN, results });
		});
	});

	it("raises unexpected_transaction_state when a CANCELLING row has no stored results", async () => {
		await withCoordinator((tc, state) => {
			seed(state, "CANCELLING");
			expect(() => tc.loadFinalResponse(TX_ID, TOKEN)).toThrow(
				expect.objectContaining({ code: "unexpected_transaction_state", attributes: expect.objectContaining({ state: "CANCELLING" }) }),
			);
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
			expect(FokosError.isCode(err, TRANSACTION_PENDING_CODES.transaction_undecided)).toBe(true);
			expect(FokosError.isCode(err, TRANSACTION_PENDING_CODES.transaction_commit_pending)).toBe(false);
			expect(String(err)).toMatch(/outcome is not yet decided/);
		});
	});
});

describe("TransactionCoordinatorDO - bounded transaction storage", () => {
	it("refuses a new transaction when the coordinator is above its database size guard", async () => {
		await withCoordinator(async (tc, state) => {
			vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(MAX_TC_DATABASE_BYTES + 1);

			await expect(tc.initiateWrite({ clientRequestToken: TOKEN, items: [] })).rejects.toThrow(
				/transaction coordinator exceeded its storage limit, please retry later/,
			);
			expect(countRows(state, "tc_state")).toBe(0);
		});
	});

	it("answers a replay when the coordinator is above its database size guard", async () => {
		await withCoordinator(async (tc, state) => {
			const items: InitiateWriteRequest["items"] = [];
			insertState(state, {
				token: TOKEN,
				transactionId: TX_ID,
				state: "COMMITTED",
				createdAt: BASE_TIME,
				completedAt: BASE_TIME,
				operationsHash: hashTransactionOperations(items),
			});
			vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(MAX_TC_DATABASE_BYTES + 1);

			await expect(tc.initiateWrite({ clientRequestToken: TOKEN, items })).resolves.toEqual({
				outcome: "committed",
				transactionId: TX_ID,
				idempotencyToken: TOKEN,
			});
		});
	});

	it("still drives recovery and runs its alarm above the database size guard", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(MAX_TC_DATABASE_BYTES + 1);
			const recover = vi.spyOn(tc, "runPrepareRecovery").mockResolvedValue();

			await tc.recoverTransaction(TX_ID);
			await tc.alarm();

			expect(recover).toHaveBeenCalledTimes(2);
		});
	});

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

	it("keys tc_state by transaction_id and enforces unique idempotency_token", async () => {
		await withCoordinator((_tc, state) => {
			const columns = state.storage.sql.exec<{ name: string; pk: number }>(`PRAGMA table_info(tc_state)`).toArray();
			const pkColumn = columns.find((c) => c.pk > 0);
			expect(pkColumn?.name).toBe("transaction_id");

			const indexes = state.storage.sql.exec<{ name: string; unique: number }>(`PRAGMA index_list(tc_state)`).toArray();
			const tokenIndex = indexes.find((idx) => idx.name === "tc_state_idempotency_token");
			expect(tokenIndex).toBeDefined();
			expect(tokenIndex?.unique).toBe(1);

			insertState(state, {
				token: "unique-token",
				transactionId: "tx-unique-1",
				state: "CREATED",
				createdAt: 1_000,
			});

			expect(() => {
				insertState(state, {
					token: "unique-token",
					transactionId: "tx-unique-2",
					state: "CREATED",
					createdAt: 1_000,
				});
			}).toThrow(/UNIQUE constraint failed/);
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

	// A prepare that keeps throwing decides nothing: the transaction can still commit once the
	// participant answers. Cancelling on it would turn transient trouble into a lost transaction.
	it("leaves a transaction PREPARING when a participant still has no answer and none rejected", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			insertParticipant(state, { prepare: "accepted", name: "p1" });
			insertParticipant(state, { name: "p2" });
			const txPrepare = vi.fn(async () => {
				throw new FokosUnavailableError(UNAVAILABLE_CODES.partition_over_size, {
					message: "partition exceeded its limits, please retry later",
				});
			});
			vi.spyOn(PartitionDO, "getByName").mockReturnValue({ txPrepare } as unknown as DurableObjectStub<PartitionDO>);

			await tc.runPrepareRecovery(TX_ID, TOKEN);

			expect(
				state.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE idempotency_token = ?`, TOKEN).toArray()[0].state,
			).toBe("PREPARING");
			expect(() => tc.loadFinalResponse(TX_ID, TOKEN)).toThrow(/outcome is not yet decided/);
		});
	});

	// Every participant answers for its own operations, so the clock_skew of one partition does not hide
	// the condition rejection of another, and that rejection keeps its image.
	it("reports each participant's answer for its own operations, and keeps the image of a rejection", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(`UPDATE tc_items SET partition_do_name = 'p2' WHERE transaction_id = ? AND op_index = 1`, TX_ID);
			const clockSkew = { code: "clock_skew" as const, hashKey: "hk1", sortKey: "sk1", serverTimestampMs: 10, transactionTimestampMs: 99 };
			insertParticipant(state, {
				name: "p1",
				prepare: "rejected",
				answer: { outcome: "rejected", results: [{ opIndex: 0, outcome: "rejected", reason: clockSkew }] },
			});
			insertParticipant(state, {
				name: "p2",
				prepare: "rejected",
				answer: {
					outcome: "rejected",
					results: [{ opIndex: 1, outcome: "rejected", reason: { code: "condition_failed", hashKey: "hk2" }, imageBytes: 6 }],
				},
			});
			insertImage(state, TX_ID, 1, "image-1");

			tc.cancelTransactionInStore(TX_ID);

			const response = tc.loadFinalResponse(TX_ID, TOKEN);
			expect(response.outcome).toBe("cancelled");
			if (response.outcome === "cancelled") {
				expect(response.results).toEqual([
					{ outcome: "rejected", reason: clockSkew },
					{
						outcome: "rejected",
						reason: { code: "condition_failed", hashKey: "hk2", item: { hashKey: "hk2", data: "image-1", kind: "text", version: 1 } },
					},
				]);
			}
			expect(countRows(state, "tc_results")).toBe(1);
		});
	});

	// A prepare that throws after its retries has no answer. Its operations report the stored cause, and
	// the operations of the other participant keep what that participant answered.
	it("reports the stored cause of a thrown prepare on its own operations, and keeps the other answer", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(`UPDATE tc_items SET partition_do_name = 'p2' WHERE transaction_id = ? AND op_index = 1`, TX_ID);
			insertParticipant(state, { name: "p1" });
			insertParticipant(state, { name: "p2" });
			const throwingPrepare = vi.fn(async () => {
				throw new Error("partition unreachable");
			});
			const rejectingPrepare = vi.fn(async () => ({
				outcome: "rejected" as const,
				results: [
					{
						opIndex: 1,
						outcome: "rejected" as const,
						reason: {
							code: "condition_failed" as const,
							hashKey: "hk2",
							item: { hashKey: "hk2", data: "image-1", kind: "text" as const, version: 1 },
						},
						imageBytes: 7,
					},
				],
			}));
			vi.spyOn(PartitionDO, "getByName").mockImplementation(
				(_ns, name) => ({ txPrepare: name === "p1" ? throwingPrepare : rejectingPrepare }) as unknown as DurableObjectStub<PartitionDO>,
			);
			vi.spyOn(tc, "runCancel").mockResolvedValue();

			const response = await tc.drivePrepare(TX_ID, TOKEN, "coordinator-id");

			expect(response.outcome).toBe("cancelled");
			if (response.outcome === "cancelled") {
				// The raw error of the prepare is stored as the foreign_error that wraps it.
				expect(response.results).toEqual([
					{
						outcome: "rejected",
						reason: { code: "foreign_error", hashKey: "hk1", sortKey: "sk1", error_id: expect.stringMatching(/^e_jvufz5_/) },
					},
					{
						outcome: "rejected",
						reason: { code: "condition_failed", hashKey: "hk2", item: { hashKey: "hk2", data: "image-1", kind: "text", version: 1 } },
					},
				]);
			}
			expect(countRows(state, "tc_results")).toBe(1);
		});
	});

	// The recovery pass must not suppress images because a participant has not answered yet: that
	// participant is the one being re-prepared, and it can come back accepted, which leaves the
	// transaction on the merge path. A suppressed image would then reach the caller as a rejection
	// with no item and no itemOmitted, which reads as "the item does not exist".
	it("keeps the images of a recovered participant when another has not answered yet", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(`UPDATE tc_items SET partition_do_name = 'p2' WHERE transaction_id = ? AND op_index = 1`, TX_ID);
			insertParticipant(state, { name: "p1" });
			insertParticipant(state, { name: "p2" });
			const acceptingPrepare = vi.fn(async () => ({ outcome: "accepted" as const }));
			const rejectingPrepare = vi.fn(async () => ({
				outcome: "rejected" as const,
				results: [
					{
						opIndex: 1,
						outcome: "rejected" as const,
						reason: {
							code: "condition_failed" as const,
							hashKey: "hk2",
							item: { hashKey: "hk2", data: "image-1", kind: "text" as const, version: 1 },
						},
						imageBytes: 7,
					},
				],
			}));
			vi.spyOn(PartitionDO, "getByName").mockImplementation(
				(_ns, name) => ({ txPrepare: name === "p1" ? acceptingPrepare : rejectingPrepare }) as unknown as DurableObjectStub<PartitionDO>,
			);
			vi.spyOn(tc, "runCancel").mockResolvedValue();

			await tc.runPrepareRecovery(TX_ID, TOKEN);

			const response = tc.loadFinalResponse(TX_ID, TOKEN);
			expect(response.outcome).toBe("cancelled");
			if (response.outcome === "cancelled") {
				expect(response.results[0]).toEqual({ outcome: "passed" });
				expect(response.results[1]).toMatchObject({
					outcome: "rejected",
					reason: { code: "condition_failed", hashKey: "hk2", item: { data: "image-1", version: 1 } },
				});
			}
		});
	});

	// The answer is written with the prepare outcome it belongs to, so a coordinator evicted between a
	// participant's answer and the decision still reports what that participant actually said.
	it("recovers a persisted clock_skew as clock_skew", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			const clockSkew = { code: "clock_skew" as const, hashKey: "hk1", serverTimestampMs: 5, transactionTimestampMs: 500 };
			insertParticipant(state, {
				name: "p1",
				prepare: "rejected",
				answer: {
					outcome: "rejected",
					results: [0, 1].map((opIndex) => ({
						opIndex,
						outcome: "rejected" as const,
						reason: { ...clockSkew, hashKey: `hk${opIndex + 1}` },
					})),
				},
			});
			insertParticipant(state, { name: "p2", prepare: "accepted" });
			vi.spyOn(tc, "runCancel").mockResolvedValue();

			await tc.runPrepareRecovery(TX_ID, TOKEN);

			const response = tc.loadFinalResponse(TX_ID, TOKEN);
			expect(response.outcome).toBe("cancelled");
			if (response.outcome === "cancelled") {
				expect(response.results).toEqual([
					{ outcome: "rejected", reason: { ...clockSkew, hashKey: "hk1" } },
					{ outcome: "rejected", reason: { ...clockSkew, hashKey: "hk2" } },
				]);
			}
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
			const results: TransactWriteOperationResultEncoded[] = [
				{ outcome: "rejected", reason: { code: "condition_failed", hashKey: "hk1" } },
			];
			seed(state, "CANCELLING", results);
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
			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toMatchObject({
				outcome: "cancelled",
				transactionId: TX_ID,
				idempotencyToken: TOKEN,
				results,
			});
		});
	});

	it("stores images in tc_results only and keeps tc_state.results_json and answer_json free of item data", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			const largeData = "A".repeat(50_000);
			state.storage.sql.exec(
				`INSERT INTO tc_participants (transaction_id, partition_do_name, partition_context_json, prepare_outcome, answer_json)
				 VALUES (?, 'p1', '{}', 'rejected', ?)`,
				TX_ID,
				JSON.stringify({
					outcome: "rejected",
					reason: { code: "condition_failed", hashKey: "hk1", sortKey: "sk1" },
					// A participant answers for every operation it owns, not only the ones it rejected.
					results: [
						{
							opIndex: 0,
							outcome: "rejected",
							reason: { code: "condition_failed", hashKey: "hk1", sortKey: "sk1" },
							imageBytes: 50_000,
						},
						{ opIndex: 1, outcome: "passed" },
					],
				}),
			);
			state.storage.sql.exec(
				`INSERT INTO tc_results (transaction_id, op_index, image_kind, image_version, image_ttl_epoch_utc_seconds, image_data)
				 VALUES (?, 0, 1, 1, NULL, ?)`,
				TX_ID,
				largeData,
			);

			tc.cancelTransactionInStore(TX_ID);

			const stateRow = state.storage.sql
				.exec<{ results_json: string }>(`SELECT results_json FROM tc_state WHERE transaction_id = ?`, TX_ID)
				.toArray()[0];

			expect(stateRow.results_json).not.toContain(largeData);
			expect(stateRow.results_json).not.toContain('"item"');

			const response = tc.loadFinalResponse(TX_ID, TOKEN);
			expect(response.outcome).toBe("cancelled");
			if (response.outcome === "cancelled") {
				expect(response.results).toHaveLength(2);
				expect(response.results[1]).toEqual({ outcome: "passed" });
				expect(response.results[0].outcome).toBe("rejected");
				if (response.results[0].outcome === "rejected" && response.results[0].reason.code === "condition_failed") {
					expect(response.results[0].reason.item?.data).toBe(largeData);
				}
			}
		});
	});

	// The array is positional to the request, so a gap must stay a gap. Filling it by push order would
	// shift every later operation onto its neighbour's outcome, and its neighbour's image with it.
	it("reports an operation no participant answered as not_evaluated, without shifting the array", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(
				`INSERT INTO tc_participants (transaction_id, partition_do_name, partition_context_json, prepare_outcome, answer_json)
				 VALUES (?, 'p1', '{}', 'rejected', ?)`,
				TX_ID,
				JSON.stringify({
					outcome: "rejected",
					reason: { code: "condition_failed", hashKey: "hk2" },
					results: [{ opIndex: 1, outcome: "rejected", reason: { code: "condition_failed", hashKey: "hk2" } }],
				}),
			);

			tc.cancelTransactionInStore(TX_ID);

			const response = tc.loadFinalResponse(TX_ID, TOKEN);
			expect(response.outcome).toBe("cancelled");
			if (response.outcome === "cancelled") {
				expect(response.results).toHaveLength(2);
				expect(response.results[0]).toEqual({ outcome: "not_evaluated" });
				expect(response.results[1]).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed", hashKey: "hk2" } });
			}
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

	// tc_results is keyed by transaction_id, and the sweep selects one batch of ids and deletes both
	// tables by it. A tc_results row that outlived its tc_state row would be unreachable and unswept.
	it("deletes the images of every transaction it sweeps, and leaves the rest alone", async () => {
		await withCoordinator(async (tc, state) => {
			vi.spyOn(Date, "now").mockReturnValue(BASE_TIME);
			insertState(state, {
				token: "expired-token",
				transactionId: "tx-expired",
				state: "CANCELLED",
				createdAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
				completedAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
			});
			insertImage(state, "tx-expired", 0, "expired-image-0");
			insertImage(state, "tx-expired", 1, "expired-image-1");
			insertState(state, {
				token: "live-token",
				transactionId: "tx-live",
				state: "CANCELLED",
				createdAt: BASE_TIME,
				completedAt: BASE_TIME,
			});
			insertImage(state, "tx-live", 0, "live-image-0");

			await tc.alarm();

			const remaining = state.storage.sql
				.exec<{ transaction_id: string }>(`SELECT transaction_id FROM tc_results ORDER BY op_index`)
				.toArray()
				.map((r) => r.transaction_id);
			expect(remaining).toEqual(["tx-live"]);
			expect(countRows(state, "tc_state")).toBe(1);
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

describe("TransactionCoordinatorDO - bounded preparing hold", () => {
	it("cancels with the stored cause and marks every operation not_evaluated when older than MAX_PREPARING_HOLD_MS and participant throws", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING", undefined, Date.now() - 30_000);
			insertParticipant(state, { name: "p1" });
			const txPrepare = vi.fn(async () => {
				throw new Error("partition unreachable");
			});
			const txCancel = vi.fn(async () => {});
			vi.spyOn(PartitionDO, "getByName").mockReturnValue({ txPrepare, txCancel } as unknown as DurableObjectStub<PartitionDO>);

			await tc.runPrepareRecovery(TX_ID, TOKEN);

			const row = state.storage.sql
				.exec<{ state: TCState; completed_at: number | null }>(`SELECT state, completed_at FROM tc_state WHERE transaction_id = ?`, TX_ID)
				.toArray()[0];
			expect(row.state).toBe("CANCELLED");
			expect(row.completed_at).toBeTypeOf("number");

			const response = tc.loadFinalResponse(TX_ID, TOKEN);
			expect(response.outcome).toBe("cancelled");
			if (response.outcome === "cancelled") {
				// p1 owns both operations, so both report its one error.
				expect(response.results).toMatchObject([
					{ outcome: "rejected", reason: { code: "foreign_error", hashKey: "hk1", sortKey: "sk1" } },
					{ outcome: "rejected", reason: { code: "foreign_error", hashKey: "hk2" } },
				]);
				const [first, second] = response.results;
				if (
					first.outcome !== "rejected" ||
					second.outcome !== "rejected" ||
					!("error_id" in first.reason) ||
					!("error_id" in second.reason)
				) {
					throw new Error("unreachable");
				}
				expect(first.reason.error_id).toBe(second.reason.error_id);
			}
		});
	});

	it("stays in PREPARING and writes no transition when younger than MAX_PREPARING_HOLD_MS", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING", undefined, Date.now() - 10_000);
			insertParticipant(state, { name: "p1" });
			const txPrepare = vi.fn(async () => {
				throw new Error("partition unreachable");
			});
			vi.spyOn(PartitionDO, "getByName").mockReturnValue({ txPrepare } as unknown as DurableObjectStub<PartitionDO>);

			await tc.runPrepareRecovery(TX_ID, TOKEN);

			const row = state.storage.sql
				.exec<{ state: TCState; completed_at: number | null }>(`SELECT state, completed_at FROM tc_state WHERE transaction_id = ?`, TX_ID)
				.toArray()[0];
			expect(row.state).toBe("PREPARING");
			expect(row.completed_at).toBeNull();
			expect(() => tc.loadFinalResponse(TX_ID, TOKEN)).toThrow(/outcome is not yet decided/);
		});
	});

	it("commits the transaction when a participant answers accepted on a pass that crosses the bound", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING", undefined, Date.now() - 30_000);
			insertParticipant(state, { name: "p1" });
			const txPrepare = vi.fn(async () => ({ outcome: "accepted" as const }));
			const txCommit = vi.fn(async () => ({ outcome: "committed" as const }));
			vi.spyOn(PartitionDO, "getByName").mockReturnValue({ txPrepare, txCommit } as unknown as DurableObjectStub<PartitionDO>);

			await tc.runPrepareRecovery(TX_ID, TOKEN);

			const row = state.storage.sql
				.exec<{ state: TCState; completed_at: number | null }>(`SELECT state, completed_at FROM tc_state WHERE transaction_id = ?`, TX_ID)
				.toArray()[0];
			expect(row.state).toBe("COMMITTED");
			expect(row.completed_at).toBeTypeOf("number");
			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toEqual({
				outcome: "committed",
				transactionId: TX_ID,
				idempotencyToken: TOKEN,
			});
		});
	});

	it("leaves a PREPARED transaction in PREPARED when crossing the bound", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARED", undefined, Date.now() - 30_000);
			insertParticipant(state, { name: "p1", prepare: "accepted" });

			tc.cancelTransactionInStore(TX_ID);

			const row = state.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE transaction_id = ?`, TX_ID).toArray()[0];
			expect(row.state).toBe("PREPARED");
		});
	});

	it("releases locks of an accepted participant after the bound cancels the transaction", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING", undefined, Date.now() - 30_000);
			state.storage.sql.exec(`UPDATE tc_items SET partition_do_name = 'p2' WHERE transaction_id = ? AND op_index = 1`, TX_ID);
			insertParticipant(state, { name: "p1", prepare: "accepted" });
			insertParticipant(state, { name: "p2" });

			const txPrepare = vi.fn(async () => {
				throw new Error("p2 unreachable");
			});
			const txCancelP1 = vi.fn(async () => {});
			const txCancelP2 = vi.fn(async () => {});
			vi.spyOn(PartitionDO, "getByName").mockImplementation(
				(_ns, name) =>
					({
						txPrepare,
						txCancel: name === "p1" ? txCancelP1 : txCancelP2,
					}) as unknown as DurableObjectStub<PartitionDO>,
			);

			await tc.runPrepareRecovery(TX_ID, TOKEN);

			expect(txCancelP1).toHaveBeenCalledWith(
				expect.anything(),
				expect.objectContaining({
					transactionId: TX_ID,
					items: [{ hashKey: kb("hk1"), sortKey: kb("sk1") }],
				}),
			);
			expect(txCancelP2).toHaveBeenCalledWith(
				expect.anything(),
				expect.objectContaining({
					transactionId: TX_ID,
					items: [{ hashKey: kb("hk2"), sortKey: ABSENT_SK }],
				}),
			);

			const row = state.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE transaction_id = ?`, TX_ID).toArray()[0];
			expect(row.state).toBe("CANCELLED");
		});
	});

	it("deletes the cancelled transaction one IDEMPOTENCY_WINDOW_MS after the bound cancelled it", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING", undefined, Date.now() - 30_000);
			insertParticipant(state, { name: "p1" });
			const txPrepare = vi.fn(async () => {
				throw new Error("p1 unreachable");
			});
			const txCancel = vi.fn(async () => {});
			vi.spyOn(PartitionDO, "getByName").mockReturnValue({ txPrepare, txCancel } as unknown as DurableObjectStub<PartitionDO>);

			await tc.runPrepareRecovery(TX_ID, TOKEN);

			expect(countRows(state, "tc_state")).toBe(1);
			const row = state.storage.sql
				.exec<{ state: TCState; completed_at: number | null }>(`SELECT state, completed_at FROM tc_state WHERE transaction_id = ?`, TX_ID)
				.toArray()[0];
			expect(row.state).toBe("CANCELLED");
			expect(row.completed_at).toBeTypeOf("number");

			state.storage.sql.exec(
				`UPDATE tc_state SET completed_at = ? WHERE transaction_id = ?`,
				Date.now() - IDEMPOTENCY_WINDOW_MS - 1,
				TX_ID,
			);
			await state.storage.deleteAlarm();
			await tc.alarm();

			expect(countRows(state, "tc_state")).toBe(0);
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

describe("TransactionCoordinatorDO - the stored cause of a failed prepare", () => {
	function unavailableWire(code: "partition_migrating" | "partition_over_size"): FokosErrorWire {
		return FokosError.toWire(new FokosUnavailableError(UNAVAILABLE_CODES[code], { message: "refused" }));
	}

	function cancelWith(state: DurableObjectState, tc: CoordinatorInternals) {
		state.storage.sql.exec(`UPDATE tc_items SET partition_do_name = 'p2' WHERE transaction_id = ? AND op_index = 1`, TX_ID);
		tc.cancelTransactionInStore(TX_ID);
		const response = tc.loadFinalResponse(TX_ID, TOKEN);
		if (response.outcome !== "cancelled") throw new Error("the transaction did not cancel");
		return response;
	}

	it("reports the stored error of a participant whose prepare threw on its operations, and keeps the other answer", async () => {
		await withCoordinator((tc, state) => {
			seed(state, "PREPARING");
			const stored = unavailableWire("partition_migrating");
			insertParticipant(state, { name: "p1", error: stored });
			insertParticipant(state, { name: "p2", prepare: "accepted" });

			// The same event: the error_id the partition minted survives the storage and the cancel.
			expect(cancelWith(state, tc).results).toEqual([
				{ outcome: "rejected", reason: { code: "partition_migrating", hashKey: "hk1", sortKey: "sk1", error_id: stored.error_id } },
				{ outcome: "passed" },
			]);
		});
	});

	it("reports prepare_unanswered for a participant with no answer and no stored error", async () => {
		await withCoordinator((tc, state) => {
			seed(state, "PREPARING");
			insertParticipant(state, { name: "p1" });
			insertParticipant(state, { name: "p2", prepare: "accepted" });

			expect(cancelWith(state, tc).results).toEqual([
				{
					outcome: "rejected",
					reason: { code: "prepare_unanswered", hashKey: "hk1", sortKey: "sk1", error_id: expect.stringMatching(/^e_mpncbz_/) },
				},
				{ outcome: "passed" },
			]);
		});
	});

	it("reports the stored error of each failed participant on its own operations", async () => {
		await withCoordinator((tc, state) => {
			seed(state, "PREPARING");
			insertParticipant(state, { name: "p2", error: unavailableWire("partition_over_size") });
			insertParticipant(state, { name: "p1", error: unavailableWire("partition_migrating") });

			expect(cancelWith(state, tc).results).toMatchObject([
				{ outcome: "rejected", reason: { code: "partition_migrating", hashKey: "hk1" } },
				{ outcome: "rejected", reason: { code: "partition_over_size", hashKey: "hk2" } },
			]);
		});
	});

	it("uses a later answer of a participant in place of the error it stored", async () => {
		await withCoordinator((tc, state) => {
			seed(state, "PREPARING");
			insertParticipant(state, {
				name: "p1",
				prepare: "rejected",
				error: unavailableWire("partition_migrating"),
				answer: {
					outcome: "rejected",
					results: [{ opIndex: 0, outcome: "rejected", reason: { code: "condition_failed", hashKey: "hk1" } }],
				},
			});
			insertParticipant(state, { name: "p2", prepare: "accepted" });

			const response = cancelWith(state, tc);

			expect(response.results).toEqual([
				{ outcome: "rejected", reason: { code: "condition_failed", hashKey: "hk1" } },
				{ outcome: "passed" },
			]);
		});
	});
});
