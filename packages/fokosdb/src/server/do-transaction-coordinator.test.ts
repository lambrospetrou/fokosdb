import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import type { PartitionDO } from "./do-partition.js";
import * as doStubs from "../shared/do-stubs.js";
import { testCoordinatorContext, testCoordinatorStubByName } from "../../test/stub-helpers.js";
import { FokosError, FokosUnavailableError, TRANSACTION_PENDING_CODES, UNAVAILABLE_CODES, type FokosErrorWire } from "../shared/errors.js";
import { SHARDING_UNAVAILABLE_CODES } from "../sharding/errors.js";
import { KeyCodec } from "../sharding/key-codec.js";
import { FokosRouter } from "../sharding/router.js";
import { FOKOS_KV_KEYS } from "../sharding/sharding-store.js";
import { IDEMPOTENCY_WINDOW_MS, MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX } from "../shared/transaction-limits.js";
import { DEFAULT_COORDINATOR_CONFIG } from "./host-config.js";
import { hashTransactionOperations } from "../shared/transaction-idempotency.js";
import type {
	InitiateWriteRequest,
	InitiateWriteResponseEncoded,
	PrepareResponse,
	TCState,
	TransactWriteOperationResultEncoded,
} from "../shared/transaction-wire-types.js";
import { fokosErrorWith } from "../../test/errors-matchers.js";
import type { FokosMigrationPageBudget } from "../sharding/runtime-config.js";
import type { FokosEnvelope, FokosShardingHooks } from "../sharding/runtime-types.js";
import type { FokosDBPolicy, FokosDBRouteContext } from "../shared/partition-context.js";

const kb = (s: string) => KeyCodec.encode(s);
const ABSENT_SK = KeyCodec.encodeOptional(undefined);

/** A partition answer in its envelope, as a stand-in partition returns it. */
function enveloped<T>(value: T): FokosEnvelope<T> {
	const self = {
		ref: { partitionId: "00", doName: "stand-in" },
		actorId: "stand-in",
		hashDepth: 0,
		rangeDepth: 0,
		role: "executed" as const,
	};
	return { value, routing: { servedBy: [self], forwardCount: 0, servedByTruncated: false } };
}

/**
 * A database size above the admission limit of the coordinator: 110% of the hash split threshold of
 * its table. `testCoordinatorContext` sets that threshold to 100 MB.
 */
const OVER_SIZE_BYTES = 100 * 1024 * 1024 * 1.1 + 1;

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
	/** The public RPC: it goes through the runtime, admission included. */
	initiateWrite(ctx: FokosDBRouteContext, request: InitiateWriteRequest): Promise<FokosEnvelope<InitiateWriteResponseEncoded>>;
	fokos: TransactionCoordinatorDO["fokos"];
	fokosNow(): number;
	hooks(): FokosShardingHooks<FokosDBPolicy>;
	initiateWriteLocal(request: InitiateWriteRequest): Promise<InitiateWriteResponseEncoded>;
	recoverTransactionLocal(transactionId: string): Promise<unknown>;
	/** The step of the `tx_recovery` job. */
	recoverStaleTransactions(): Promise<void>;
	driveTransaction(transactionId: string, idempotencyToken: string, state: TCState, budgetMs: number): Promise<void>;
	earliestRecoveryAt(): number | null;
	/** The step of the `idempotency_sweep` job. */
	sweepExpiredTransactions(): number | null;
	earliestCompletedAt(): number | null;
	buildMigrationPage(
		cursor: string | null,
		belongsToTarget: (key: { hashKey: Uint8Array }) => boolean,
		budget: FokosMigrationPageBudget,
	): {
		page: Array<{ state: { transaction_id: string }; items: Array<{ data: unknown; data_kind: unknown; conditions_json: unknown }> }>;
		nextCursor: string | null;
	};
	loadFinalResponse(transactionId: string, idempotencyToken: string): InitiateWriteResponseEncoded;
	cancelTransactionInStore(transactionId: string, idempotencyToken: string): void;
	storePrepareAnswer(transactionId: string, partitionDoName: string, answer: PrepareResponse): void;
	storePrepareError(transactionId: string, partitionDoName: string, err: unknown): void;
	markPrepared(transactionId: string, idempotencyToken: string): void;
	drivePrepare(transactionId: string, idempotencyToken: string, requestBudgetMs: number): Promise<InitiateWriteResponseEncoded>;
	runPrepareRecovery(transactionId: string, idempotencyToken: string, requestBudgetMs: number): Promise<void>;
	runCommit(transactionId: string, idempotencyToken: string, requestBudgetMs: number): Promise<void>;
	runCancel(transactionId: string, idempotencyToken: string, requestBudgetMs: number): Promise<void>;
	stripPayload(transactionId: string): void;
};

function seed(state: DurableObjectState, tcState: TCState, results?: TransactWriteOperationResultEncoded[], createdAt?: number): void {
	state.storage.sql.exec(`DELETE FROM tc_state`);
	state.storage.sql.exec(`DELETE FROM tc_participants`);
	state.storage.sql.exec(`DELETE FROM tc_items`);
	const now = createdAt ?? Date.now() - 10_000;
	state.storage.sql.exec(
		`INSERT INTO tc_state (idempotency_token, transaction_id, state, transaction_ts, created_at, next_recovery_at, results_json, operations_hash)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		TOKEN,
		TX_ID,
		tcState,
		now,
		now,
		now + DEFAULT_COORDINATOR_CONFIG.staleTransactionMs,
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
			(idempotency_token, transaction_id, state, transaction_ts, created_at, completed_at, next_recovery_at, results_json, operations_hash)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		options.token,
		options.transactionId,
		options.state,
		options.createdAt,
		options.createdAt,
		options.completedAt ?? null,
		options.completedAt == null ? options.createdAt + DEFAULT_COORDINATOR_CONFIG.staleTransactionMs : null,
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
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		TX_ID,
		outcome.name ?? "p1",
		// A valid route context, because the coordinator builds its partition client from it.
		JSON.stringify({ ...testCoordinatorContext(), doName: outcome.name ?? "p1" }),
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

/** A prepare answer that rejects one operation on its condition, with the image of the item. */
function rejection(opIndex: number, hashKey: string, imageBytes: number): PrepareResponse {
	return {
		outcome: "rejected",
		results: [
			{
				opIndex,
				outcome: "rejected",
				reason: { code: "condition_failed", hashKey, item: { hashKey, data: `image-${opIndex}`, kind: "text", version: 1 } },
				imageBytes,
			},
		],
	};
}

// Every table this DO owns — the tc_* tables plus the migrations bookkeeping. The `_cf_*` tables are
// the platform's own and are excluded.
function tableNames(state: DurableObjectState): string[] {
	return state.storage.sql
		.exec<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' ORDER BY name`)
		.toArray()
		.map((r) => r.name);
}

/**
 * Runs `fn` inside a root coordinator that owns every token. A first request gives the runtime its
 * identity, because each transition of the state machine tests that the coordinator owns the token.
 */
/** The retry budget of a direct call to a drive method: the one that a request uses. */
const BUDGET_MS = DEFAULT_COORDINATOR_CONFIG.fanoutRequestBudgetMs;

async function withCoordinator(
	fn: (tc: CoordinatorInternals, state: DurableObjectState, ctx: FokosDBRouteContext) => void | Promise<void>,
): Promise<void> {
	const ctx = testCoordinatorContext();
	const stub = testCoordinatorStubByName(ctx.doName);
	await stub.recoverTransaction(ctx, { transactionId: "no-such-transaction", idempotencyToken: TOKEN });
	await runInDurableObject(stub, async (instance: TransactionCoordinatorDO, state: DurableObjectState) => {
		await fn(instance as unknown as CoordinatorInternals, state, ctx);
	});
}

/** One pass of both host jobs, in the order the scheduler runs them. */
async function runJobs(tc: CoordinatorInternals): Promise<void> {
	await tc.recoverStaleTransactions();
	tc.sweepExpiredTransactions();
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
			expect(err).toMatchObject({ attributes: { transactionId: TX_ID, state: tcState } });
		});
	});
});

describe("TransactionCoordinatorDO - participant resolution", () => {
	it("resolves each item to the root of its key in the table config, and stores the root contexts", async () => {
		await withCoordinator(async (tc, state, ctx) => {
			const topology = { ...ctx.topology, rootTreesN: 8 };
			// A policy that is not the policy of the coordinator: each participant must receive the policy of the table.
			const policy = { ...ctx.policy, hashSplitConditions: { ...ctx.policy.hashSplitConditions, maxSizeMb: 123 } };
			const table = new FokosRouter(topology, ctx.rangeConfig, policy);
			// Keys under two different roots.
			const hashKeys = Array.from({ length: 16 }, (_, i) => KeyCodec.encode(`hk-${i}`));
			const first = table.rootContext(hashKeys[0]);
			const second = hashKeys.find((hk) => table.rootContext(hk).doName !== first.doName)!;
			const items: InitiateWriteRequest["items"] = [hashKeys[0], second].map((hashKey, opIndex) => ({
				opIndex,
				hashKey,
				sortKey: KeyCodec.encodeOptional(undefined),
				operation: "put",
				data: "v",
				kind: "text",
			}));
			// Only the rows written before the first prepare are under test.
			vi.spyOn(tc, "drivePrepare").mockResolvedValue({ outcome: "committed", transactionId: TX_ID, idempotencyToken: TOKEN });

			await tc.initiateWriteLocal({
				clientRequestToken: TOKEN,
				table: { topology, rangeConfig: ctx.rangeConfig, policy, policyVersion: ctx.policyVersion },
				items,
			});

			const itemRows = state.storage.sql
				.exec<{ op_index: number; partition_do_name: string }>(`SELECT op_index, partition_do_name FROM tc_items ORDER BY op_index`)
				.toArray();
			expect(itemRows.map((r) => r.partition_do_name)).toEqual(items.map((i) => table.rootContext(i.hashKey).doName));
			const participantRows = state.storage.sql
				.exec<{
					partition_do_name: string;
					partition_context_json: string;
				}>(`SELECT partition_do_name, partition_context_json FROM tc_participants`)
				.toArray();
			expect(new Map(participantRows.map((r) => [r.partition_do_name, JSON.parse(r.partition_context_json)]))).toEqual(
				new Map(items.map((i) => [table.rootContext(i.hashKey).doName, JSON.parse(JSON.stringify(table.rootContext(i.hashKey)))])),
			);
		});
	});

	it("refuses an invalid table topology before it writes anything", async () => {
		await withCoordinator(async (tc, state, ctx) => {
			await expect(
				tc.initiateWriteLocal({ clientRequestToken: TOKEN, table: { ...ctx, topology: { ...ctx.topology, rootTreesN: 0 } }, items: [] }),
			).rejects.toThrow(fokosErrorWith("partition_context_options_invalid"));
			expect(countRows(state, "tc_state")).toBe(0);
		});
	});
});

describe("TransactionCoordinatorDO - bounded transaction storage", () => {
	it.each([{}, { maxSizeMb: 10_000 }])(
		"splits and refuses new transactions before the storage limit with hashSplitConditions %j",
		async (hashSplitConditions) => {
			await withCoordinator(async (tc, state, ctx) => {
				const policy = { ...ctx.policy, hashSplitConditions };
				// A split needs two tokens.
				for (const token of ["tok-a", "tok-b"]) {
					insertState(state, { token, transactionId: `tx-${token}`, state: "COMMITTED", createdAt: Date.now(), completedAt: Date.now() });
				}
				// One byte above the size limit of a coordinator. Each policy has no size threshold, or a
				// threshold that is larger than this limit. Thus only the limit of the coordinator applies.
				vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(5 * 1024 * 1024 * 1024 + 1);
				expect(tc.hooks().evaluateSplit({ identity: tc.fokos.identity(), policy })).not.toBe(false);
				await expect(tc.initiateWrite({ ...ctx, policy }, { clientRequestToken: TOKEN, table: ctx, items: [] })).rejects.toThrow(
					fokosErrorWith("coordinator_over_size"),
				);
				expect(countRows(state, "tc_state")).toBe(2);
			});
		},
	);

	it("does not split above the limit with fewer than two tokens, because a split moves whole tokens", async () => {
		await withCoordinator(async (tc, state, ctx) => {
			vi.spyOn(console, "error").mockImplementation(() => {});
			vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(Number.MAX_SAFE_INTEGER);
			const evaluate = () => tc.hooks().evaluateSplit({ identity: tc.fokos.identity(), policy: ctx.policy });
			expect(evaluate()).toBe(false);
			insertState(state, { token: "tok-a", transactionId: "tx-a", state: "COMMITTED", createdAt: Date.now(), completedAt: Date.now() });
			expect(evaluate()).toBe(false);
			insertState(state, { token: "tok-b", transactionId: "tx-b", state: "PREPARING", createdAt: Date.now() });
			expect(evaluate()).toEqual({});
		});
	});

	it("refuses a new transaction when the coordinator is above its database size guard", async () => {
		await withCoordinator(async (tc, state, ctx) => {
			vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(OVER_SIZE_BYTES);

			await expect(tc.initiateWrite(ctx, { clientRequestToken: TOKEN, table: ctx, items: [] })).rejects.toThrow(
				fokosErrorWith("coordinator_over_size"),
			);
			expect(countRows(state, "tc_state")).toBe(0);
		});
	});

	it("answers a replay when the coordinator is above its database size guard", async () => {
		await withCoordinator(async (tc, state, ctx) => {
			const items: InitiateWriteRequest["items"] = [];
			insertState(state, {
				token: TOKEN,
				transactionId: TX_ID,
				state: "COMMITTED",
				createdAt: BASE_TIME,
				completedAt: BASE_TIME,
				operationsHash: hashTransactionOperations(items),
			});
			vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(OVER_SIZE_BYTES);

			expect((await tc.initiateWrite(ctx, { clientRequestToken: TOKEN, table: ctx, items })).value).toEqual({
				outcome: "committed",
				transactionId: TX_ID,
				idempotencyToken: TOKEN,
			});
		});
	});

	// A transaction that a drive does not finish goes behind the others, so it cannot use each step.
	it("drives the due transactions in next_recovery_at order and moves each one forward", async () => {
		await withCoordinator(async (tc, state) => {
			const now = Date.now();
			vi.spyOn(tc, "fokosNow").mockReturnValue(now);
			state.storage.sql.exec(`DELETE FROM tc_state`);
			for (const [transactionId, createdAt] of [
				["tx-later", now - 40_000],
				["tx-earlier", now - 60_000],
				["tx-new", now],
			] as const) {
				insertState(state, { token: `token-${transactionId}`, transactionId, state: "COMMITTING", createdAt });
			}
			const drives: { transactionId: string; budgetMs: number }[] = [];
			vi.spyOn(tc, "driveTransaction").mockImplementation(async (transactionId, _token, _state, budgetMs) => {
				drives.push({ transactionId, budgetMs });
			});

			await tc.recoverStaleTransactions();

			expect(drives).toEqual([
				{ transactionId: "tx-earlier", budgetMs: DEFAULT_COORDINATOR_CONFIG.fanoutRequestBudgetMs },
				{ transactionId: "tx-later", budgetMs: DEFAULT_COORDINATOR_CONFIG.fanoutRequestBudgetMs },
			]);
			const nextRecoveryAt = (transactionId: string) =>
				state.storage.sql
					.exec<{ next_recovery_at: number }>(`SELECT next_recovery_at FROM tc_state WHERE transaction_id = ?`, transactionId)
					.one().next_recovery_at;
			// Half the age, at most 30 seconds.
			expect(nextRecoveryAt("tx-earlier")).toBe(now + 30_000);
			expect(nextRecoveryAt("tx-later")).toBe(now + 20_000);
			expect(tc.earliestRecoveryAt()).toBe(now + DEFAULT_COORDINATOR_CONFIG.staleTransactionMs);

			// Nothing is due now, so a second step drives nothing.
			await tc.recoverStaleTransactions();
			expect(drives).toHaveLength(2);

			// A recovery call makes its own transaction due now, before the others.
			await expect(tc.recoverTransactionLocal("tx-later")).resolves.toEqual({ state: "driving" });
			expect(nextRecoveryAt("tx-later")).toBe(now);
			await tc.recoverStaleTransactions();
			expect(drives.map((drive) => drive.transactionId)).toEqual(["tx-earlier", "tx-later", "tx-later"]);
		});
	});

	// The step moves forward only the transactions that it drives. The others keep their place.
	it("keeps the place of a due transaction that the step does not reach before its budget ends", async () => {
		await withCoordinator(async (tc, state) => {
			const now = Date.now();
			const clock = vi.spyOn(tc, "fokosNow").mockReturnValue(now);
			state.storage.sql.exec(`DELETE FROM tc_state`);
			insertState(state, { token: "token-first", transactionId: "tx-first", state: "COMMITTING", createdAt: now - 60_000 });
			insertState(state, { token: "token-second", transactionId: "tx-second", state: "COMMITTING", createdAt: now - 50_000 });
			const before = state.storage.sql
				.exec<{ next_recovery_at: number }>(`SELECT next_recovery_at FROM tc_state WHERE transaction_id = 'tx-second'`)
				.one().next_recovery_at;
			// The first drive uses the whole budget of the step.
			const drive = vi.spyOn(tc, "driveTransaction").mockImplementation(async () => {
				clock.mockReturnValue(now + DEFAULT_COORDINATOR_CONFIG.alarmRecoveryBudgetMs);
			});

			await tc.recoverStaleTransactions();

			expect(drive.mock.calls.map(([transactionId]) => transactionId)).toEqual(["tx-first"]);
			expect(
				state.storage.sql
					.exec<{ next_recovery_at: number }>(`SELECT next_recovery_at FROM tc_state WHERE transaction_id = 'tx-second'`)
					.one().next_recovery_at,
			).toBe(before);
		});
	});

	it("still answers a recovery call and runs its recovery job above the database size guard", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			vi.spyOn(state.storage.sql, "databaseSize", "get").mockReturnValue(OVER_SIZE_BYTES);
			const recover = vi.spyOn(tc, "runPrepareRecovery").mockResolvedValue();

			await expect(tc.recoverTransactionLocal(TX_ID)).resolves.toEqual({ state: "driving" });
			await tc.recoverStaleTransactions();

			expect(recover).toHaveBeenCalledTimes(1);
		});
	});

	// Calls from many participants must not start one drive each. The call makes the job due, and the
	// job drives the transaction.
	it("answers a recovery call for an incomplete transaction at once, and makes the tx_recovery job due", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			const recover = vi.spyOn(tc, "runPrepareRecovery").mockResolvedValue();
			const now = Date.now();
			vi.spyOn(tc, "fokosNow").mockReturnValue(now);

			await expect(tc.recoverTransactionLocal(TX_ID)).resolves.toEqual({ state: "driving" });

			expect(recover).not.toHaveBeenCalled();
			expect(state.storage.kv.get<Record<string, { nextRunAt: number }>>(FOKOS_KV_KEYS.JOBS)?.tx_recovery?.nextRunAt).toBeLessThanOrEqual(
				now,
			);
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

	// The recovery job reads only the transactions that are not complete, in index order, with no sort.
	// A full scan of tc_state would read the whole idempotency window at each step.
	it("reads the due transactions and the recovery deadline from the partial recovery index", async () => {
		await withCoordinator((_tc, state) => {
			const plan = (sql: string, ...params: number[]) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, ...params)
					.toArray()
					.map((r) => r.detail)
					.join(" | ");

			const deadline = plan(`SELECT next_recovery_at FROM tc_state WHERE completed_at IS NULL ORDER BY next_recovery_at LIMIT 1`);
			expect(deadline).toContain("USING INDEX idx_tc_state_recovery");
			expect(deadline).not.toContain("TEMP B-TREE");
			const due = plan(
				`SELECT transaction_id, idempotency_token, created_at FROM tc_state
				  WHERE completed_at IS NULL AND next_recovery_at <= ? ORDER BY next_recovery_at LIMIT ?`,
				1,
				10,
			);
			expect(due).toContain("USING INDEX idx_tc_state_recovery (next_recovery_at<?)");
			expect(due).not.toContain("TEMP B-TREE");
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

	it("keeps the payload in the PREPARED transition", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(`UPDATE tc_items SET conditions_json = '{"op":"test"}' WHERE transaction_id = ?`, TX_ID);
			vi.spyOn(tc, "runCommit").mockResolvedValue();

			await expect(tc.drivePrepare(TX_ID, TOKEN, 0)).rejects.toThrow(fokosErrorWith("transaction_commit_pending"));

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
			expect(items[0]).toMatchObject({ operation: "put", data: "v", data_kind: 1, conditions_json: '{"op":"test"}' });
			expect(items[1]).toMatchObject({ operation: "delete", conditions_json: '{"op":"test"}' });
		});
	});

	it("the recovery job keeps PREPARING payload until prepare receives it", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			insertParticipant(state, {});
			const txPrepare = vi.fn(async (_pCtx: unknown, request: { items: Array<{ data?: string | Uint8Array }> }) => {
				expect(request.items[0].data).toBe("v");
				return enveloped({ outcome: "accepted" as const });
			});
			const txCommit = vi.fn(async () => enveloped({ outcome: "committed" as const }));
			vi.spyOn(doStubs, "partitionStubByName").mockReturnValue({ txPrepare, txCommit } as unknown as DurableObjectStub<PartitionDO>);

			await tc.recoverStaleTransactions();

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
			vi.spyOn(doStubs, "partitionStubByName").mockReturnValue({ txPrepare } as unknown as DurableObjectStub<PartitionDO>);

			await tc.runPrepareRecovery(TX_ID, TOKEN, BUDGET_MS);

			expect(
				state.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE idempotency_token = ?`, TOKEN).toArray()[0].state,
			).toBe("PREPARING");
			expect(() => tc.loadFinalResponse(TX_ID, TOKEN)).toThrow(fokosErrorWith("transaction_undecided"));
		});
	});

	// Every participant answers for its own operations, so the clock_skew of one partition does not hide
	// the condition rejection of another, and that rejection keeps its image.
	it("reports each participant's answer for its own operations, and keeps the image of a rejection", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(`UPDATE tc_items SET partition_do_name = 'p2' WHERE transaction_id = ? AND op_index = 1`, TX_ID);
			const clockSkew = {
				code: "clock_skew" as const,
				hashKey: "hk1",
				sortKey: "sk1",
				serverTimestampMicros: 10,
				transactionTimestampMicros: 99,
			};
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

			tc.cancelTransactionInStore(TX_ID, TOKEN);

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

	// Two drives can prepare the same participant. The answer of the slower drive can arrive after the
	// other drive has stored CANCELLING and deleted the images that the cap dropped.
	it("ignores a prepare answer that arrives after the cancel, and does not add an image again", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(`UPDATE tc_items SET partition_do_name = 'p2' WHERE transaction_id = ? AND op_index = 1`, TX_ID);
			insertParticipant(state, { name: "p1" });
			insertParticipant(state, { name: "p2" });
			// The first image fills the whole cap, so the cap drops the second image.
			tc.storePrepareAnswer(TX_ID, "p1", rejection(0, "hk1", MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX));
			tc.storePrepareAnswer(TX_ID, "p2", rejection(1, "hk2", 6));
			tc.cancelTransactionInStore(TX_ID, TOKEN);
			const before = tc.loadFinalResponse(TX_ID, TOKEN);
			const answersBefore = state.storage.sql.exec(`SELECT * FROM tc_participants ORDER BY partition_do_name`).toArray();
			expect(countRows(state, "tc_results")).toBe(1);

			tc.storePrepareAnswer(TX_ID, "p2", rejection(1, "hk2", 6));

			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toEqual(before);
			expect(state.storage.sql.exec(`SELECT * FROM tc_participants ORDER BY partition_do_name`).toArray()).toEqual(answersBefore);
			expect(countRows(state, "tc_results")).toBe(1);
		});
	});

	// A participant that accepted holds its locks until the decision. A rejection from the same
	// participant that arrives later is an old answer.
	it("keeps an accepted answer when a rejection of the same participant arrives later", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			insertParticipant(state, { name: "p1" });

			tc.storePrepareAnswer(TX_ID, "p1", { outcome: "accepted" });
			tc.storePrepareAnswer(TX_ID, "p1", rejection(0, "hk1", 7));

			const row = state.storage.sql
				.exec<{ prepare_outcome: string | null; answer_json: string | null }>(`SELECT prepare_outcome, answer_json FROM tc_participants`)
				.one();
			expect(row).toEqual({ prepare_outcome: "accepted", answer_json: null });
			expect(countRows(state, "tc_results")).toBe(0);
		});
	});

	// A cancel follows a stored rejection, and the cancel releases a lock that a later prepare took. A
	// stored answer never changes, so the stored images of a transaction only grow until the decision.
	it("keeps the first rejection when a later answer of the same participant arrives", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			insertParticipant(state, { name: "p1" });

			tc.storePrepareAnswer(TX_ID, "p1", rejection(0, "hk1", 7));
			tc.storePrepareAnswer(TX_ID, "p1", { outcome: "accepted" });

			expect(state.storage.sql.exec<{ prepare_outcome: string | null }>(`SELECT prepare_outcome FROM tc_participants`).one()).toEqual({
				prepare_outcome: "rejected",
			});
			expect(countRows(state, "tc_results")).toBe(1);
		});
	});

	// Nine participants each answer under their own cap, and together they go above the combined cap.
	// A PREPARING transaction then held more images than the RPC limit of one migration page.
	it("stores at most the combined image cap while the transaction is PREPARING", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(`DELETE FROM tc_items`);
			const imageBytes = 390 * 1024;
			const data = "x".repeat(imageBytes);
			for (let p = 0; p < 9; p++) {
				insertParticipant(state, { name: `p${p}` });
				for (let i = p * 10; i < p * 10 + 10; i++) {
					state.storage.sql.exec(
						`INSERT INTO tc_items (transaction_id, hk, sk, op_index, operation, partition_do_name) VALUES (?, ?, ?, ?, 'check', ?)`,
						TX_ID,
						kb(`hk${i}`),
						ABSENT_SK,
						i,
						`p${p}`,
					);
				}
			}
			for (let p = 0; p < 9; p++) {
				tc.storePrepareAnswer(TX_ID, `p${p}`, {
					outcome: "rejected",
					results: Array.from({ length: 10 }, (_, k) => {
						const opIndex = p * 10 + k;
						const hashKey = `hk${opIndex}`;
						return {
							opIndex,
							outcome: "rejected" as const,
							reason: { code: "condition_failed" as const, hashKey, item: { hashKey, data, kind: "text" as const, version: 1 } },
							imageBytes,
						};
					}),
				});
			}

			const storedBytes = state.storage.sql
				.exec<{ n: number }>(`SELECT SUM(LENGTH(image_data)) AS n FROM tc_results WHERE transaction_id = ?`, TX_ID)
				.one().n;
			expect(storedBytes).toBe(Math.floor(MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX / imageBytes) * imageBytes);
		});
	});

	// The cap keeps the images in opIndex order. An answer that arrives later can drop an image that an
	// earlier answer stored, and the response is the same in each order of the answers.
	it("gives the same images in the response in each order of the answers", async () => {
		const answerInOrder = async (order: string[]) => {
			let response: unknown;
			await withCoordinator(async (tc, state) => {
				seed(state, "PREPARING");
				state.storage.sql.exec(`UPDATE tc_items SET partition_do_name = 'p2' WHERE transaction_id = ? AND op_index = 1`, TX_ID);
				insertParticipant(state, { name: "p1" });
				insertParticipant(state, { name: "p2" });
				const answers: Record<string, PrepareResponse> = {
					p1: rejection(0, "hk1", MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX - 5),
					p2: rejection(1, "hk2", 10),
				};
				for (const name of order) {
					tc.storePrepareAnswer(TX_ID, name, answers[name]);
				}
				expect(countRows(state, "tc_results")).toBe(1);
				tc.cancelTransactionInStore(TX_ID, TOKEN);
				response = tc.loadFinalResponse(TX_ID, TOKEN);
			});
			return response;
		};

		const first = await answerInOrder(["p1", "p2"]);
		expect(await answerInOrder(["p2", "p1"])).toEqual(first);
		expect(first).toMatchObject({
			outcome: "cancelled",
			results: [
				{ outcome: "rejected", reason: { code: "condition_failed", item: { data: "image-0" } } },
				{ outcome: "rejected", reason: { code: "condition_failed", hashKey: "hk2" }, itemOmitted: "response_too_large" },
			],
		});
		expect((first as { results: Array<{ reason: { item?: unknown } }> }).results[1].reason.item).toBeUndefined();
	});

	it("does not store a prepare error after the decision", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "CANCELLING", [{ outcome: "not_evaluated" }, { outcome: "not_evaluated" }]);
			insertParticipant(state, { name: "p1" });

			tc.storePrepareError(TX_ID, "p1", new Error("late failure"));

			expect(state.storage.sql.exec<{ error_json: string | null }>(`SELECT error_json FROM tc_participants`).one().error_json).toBeNull();
		});
	});

	it("does not move to PREPARED while a participant has no stored accepted answer", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			insertParticipant(state, { name: "p1", prepare: "accepted" });
			insertParticipant(state, { name: "p2" });

			tc.markPrepared(TX_ID, TOKEN);

			expect(state.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state`).one().state).toBe("PREPARING");
		});
	});

	// A request waits for the job schedule after it inserts the transaction. A retry with the same token
	// can drive and decide the transaction in that time.
	it("sends no prepare when another drive has already decided the transaction", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "CANCELLING", [{ outcome: "not_evaluated" }, { outcome: "not_evaluated" }]);
			insertParticipant(state, { name: "p1" });
			const txPrepare = vi.fn(async (_pCtx: unknown, _request: { transactionId: string }) => enveloped({ outcome: "accepted" as const }));
			vi.spyOn(doStubs, "partitionStubByName").mockReturnValue({ txPrepare } as unknown as DurableObjectStub<PartitionDO>);

			await expect(tc.drivePrepare(TX_ID, TOKEN, BUDGET_MS)).resolves.toMatchObject({ outcome: "cancelled" });
			await tc.runPrepareRecovery(TX_ID, TOKEN, BUDGET_MS);

			// The coordinator is shared with other tests, so only the calls for this transaction count.
			expect(txPrepare.mock.calls.filter(([, request]) => request.transactionId === TX_ID)).toEqual([]);
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
			const rejectingPrepare = vi.fn(async () =>
				enveloped({
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
				}),
			);
			vi.spyOn(doStubs, "partitionStubByName").mockImplementation(
				(_env, _ctx, name) =>
					({ txPrepare: name === "p1" ? throwingPrepare : rejectingPrepare }) as unknown as DurableObjectStub<PartitionDO>,
			);
			vi.spyOn(tc, "runCancel").mockResolvedValue();

			const response = await tc.drivePrepare(TX_ID, TOKEN, BUDGET_MS);

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
			const acceptingPrepare = vi.fn(async () => enveloped({ outcome: "accepted" as const }));
			const rejectingPrepare = vi.fn(async () =>
				enveloped({
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
				}),
			);
			vi.spyOn(doStubs, "partitionStubByName").mockImplementation(
				(_env, _ctx, name) =>
					({ txPrepare: name === "p1" ? acceptingPrepare : rejectingPrepare }) as unknown as DurableObjectStub<PartitionDO>,
			);
			vi.spyOn(tc, "runCancel").mockResolvedValue();

			await tc.runPrepareRecovery(TX_ID, TOKEN, BUDGET_MS);

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
			const clockSkew = { code: "clock_skew" as const, hashKey: "hk1", serverTimestampMicros: 5, transactionTimestampMicros: 500 };
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

			await tc.runPrepareRecovery(TX_ID, TOKEN, BUDGET_MS);

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

	it("keeps the payload in the CANCELLING transition", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING");
			state.storage.sql.exec(`UPDATE tc_items SET conditions_json = '{"op":"test"}' WHERE transaction_id = ?`, TX_ID);
			insertParticipant(state, { prepare: "rejected" });
			vi.spyOn(tc, "runCancel").mockResolvedValue();

			await tc.runPrepareRecovery(TX_ID, TOKEN, BUDGET_MS);

			expect(
				state.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE idempotency_token = ?`, TOKEN).toArray()[0].state,
			).toBe("CANCELLING");
			const payload = state.storage.sql
				.exec<{
					data: string | ArrayBuffer | null;
					data_kind: number | null;
					conditions_json: string | null;
				}>(`SELECT data, data_kind, conditions_json FROM tc_items WHERE transaction_id = ? ORDER BY op_index`, TX_ID)
				.toArray();
			expect(payload[0]).toEqual({ data: "v", data_kind: 1, conditions_json: '{"op":"test"}' });
		});
	});

	it.each(["PREPARED", "COMMITTING", "CANCELLING"] as const)(
		"the recovery claim removes the payload of a %s transaction and keeps its keys",
		async (tcState) => {
			await withCoordinator(async (tc, state) => {
				seed(state, tcState);
				state.storage.sql.exec(`UPDATE tc_items SET conditions_json = '{"op":"test"}' WHERE transaction_id = ?`, TX_ID);
				const drive = vi.spyOn(tc, "driveTransaction").mockResolvedValue();

				await tc.recoverStaleTransactions();

				expect(drive).toHaveBeenCalledTimes(1);
				const items = state.storage.sql
					.exec<{
						hk: ArrayBuffer;
						operation: string;
						data: string | ArrayBuffer | null;
						data_kind: number | null;
						conditions_json: string | null;
					}>(`SELECT hk, operation, data, data_kind, conditions_json FROM tc_items WHERE transaction_id = ? ORDER BY op_index`, TX_ID)
					.toArray();
				expect(items.map((item) => item.operation)).toEqual(["put", "delete"]);
				expect(items.map((item) => new Uint8Array(item.hk))).toEqual([new Uint8Array(kb("hk1")), new Uint8Array(kb("hk2"))]);
				expect(items.every((item) => item.data === null && item.data_kind === null && item.conditions_json === null)).toBe(true);
			});
		},
	);

	it("sets completed_at, deletes per-transaction rows, and keeps the committed replay", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "COMMITTING");
			insertParticipant(state, { prepare: "accepted", commit: "committed" });

			await tc.runCommit(TX_ID, TOKEN, BUDGET_MS);

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

			await tc.runCancel(TX_ID, TOKEN, BUDGET_MS);

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

			tc.cancelTransactionInStore(TX_ID, TOKEN);

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

			tc.cancelTransactionInStore(TX_ID, TOKEN);

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
	it("deletes one batch and runs again at once while expired rows remain", async () => {
		await withCoordinator(async (tc, state) => {
			vi.spyOn(tc, "fokosNow").mockReturnValue(BASE_TIME);
			for (let i = 0; i < DEFAULT_COORDINATOR_CONFIG.sweepBatchRows + 3; i++) {
				insertState(state, {
					token: `expired-${i}`,
					transactionId: `tx-expired-${i}`,
					state: "COMMITTED",
					createdAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
					completedAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
				});
			}

			expect(tc.sweepExpiredTransactions()).toBe(BASE_TIME);
			expect(countRows(state, "tc_state")).toBe(3);

			expect(tc.sweepExpiredTransactions()).toBeNull();
			expect(countRows(state, "tc_state")).toBe(0);
		});
	});

	// tc_results is keyed by transaction_id, and the sweep selects one batch of ids and deletes both
	// tables by it. A tc_results row that outlived its tc_state row would be unreachable and unswept.
	it("deletes the images of every transaction it sweeps, and leaves the rest alone", async () => {
		await withCoordinator(async (tc, state) => {
			vi.spyOn(tc, "fokosNow").mockReturnValue(BASE_TIME);
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

			tc.sweepExpiredTransactions();

			const remaining = state.storage.sql
				.exec<{ transaction_id: string }>(`SELECT transaction_id FROM tc_results ORDER BY op_index`)
				.toArray()
				.map((r) => r.transaction_id);
			expect(remaining).toEqual(["tx-live"]);
			expect(countRows(state, "tc_state")).toBe(1);
		});
	});

	it("reports the next expiry as the deadline of the sweep until the last completed row expires", async () => {
		await withCoordinator(async (tc, state) => {
			let now = BASE_TIME;
			vi.spyOn(tc, "fokosNow").mockImplementation(() => now);
			insertState(state, {
				token: "idle-token",
				transactionId: "idle-tx",
				state: "COMMITTED",
				createdAt: BASE_TIME,
				completedAt: BASE_TIME,
			});

			expect(tc.sweepExpiredTransactions()).toBeNull();
			expect(countRows(state, "tc_state")).toBe(1);
			expect(tc.earliestCompletedAt()).toBe(BASE_TIME);

			now = BASE_TIME + IDEMPOTENCY_WINDOW_MS + 1;
			expect(tc.sweepExpiredTransactions()).toBeNull();
			expect(countRows(state, "tc_state")).toBe(0);
			expect(tc.earliestCompletedAt()).toBeNull();
		});
	});

	it("treats a token as a new transaction after its completed row expires", async () => {
		await withCoordinator(async (tc, state, ctx) => {
			vi.spyOn(tc, "fokosNow").mockReturnValue(BASE_TIME);
			const oldTransactionId = "expired-replay-tx";
			insertState(state, {
				token: TOKEN,
				transactionId: oldTransactionId,
				state: "COMMITTED",
				createdAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
				completedAt: BASE_TIME - IDEMPOTENCY_WINDOW_MS - 1,
			});

			tc.sweepExpiredTransactions();
			const result = await tc.initiateWriteLocal({ clientRequestToken: TOKEN, table: ctx, items: [] });

			expect(result.outcome).toBe("committed");
			expect(result.transactionId).not.toBe(oldTransactionId);
			expect(countRows(state, "tc_state")).toBe(1);
		});
	});

	it("runs the sweep job after the recovery job exhausts its budget", async () => {
		await withCoordinator(async (tc, state) => {
			let now = BASE_TIME;
			vi.spyOn(tc, "fokosNow").mockImplementation(() => now);
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
				now += DEFAULT_COORDINATOR_CONFIG.alarmRecoveryBudgetMs;
			});

			await runJobs(tc);

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
			const txCancel = vi.fn(async () => enveloped(undefined));
			vi.spyOn(doStubs, "partitionStubByName").mockReturnValue({ txPrepare, txCancel } as unknown as DurableObjectStub<PartitionDO>);

			await tc.runPrepareRecovery(TX_ID, TOKEN, BUDGET_MS);

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
			vi.spyOn(doStubs, "partitionStubByName").mockReturnValue({ txPrepare } as unknown as DurableObjectStub<PartitionDO>);

			await tc.runPrepareRecovery(TX_ID, TOKEN, BUDGET_MS);

			const row = state.storage.sql
				.exec<{ state: TCState; completed_at: number | null }>(`SELECT state, completed_at FROM tc_state WHERE transaction_id = ?`, TX_ID)
				.toArray()[0];
			expect(row.state).toBe("PREPARING");
			expect(row.completed_at).toBeNull();
			expect(() => tc.loadFinalResponse(TX_ID, TOKEN)).toThrow(fokosErrorWith("transaction_undecided"));
		});
	});

	it("commits the transaction when a participant answers accepted on a pass that crosses the bound", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "PREPARING", undefined, Date.now() - 30_000);
			insertParticipant(state, { name: "p1" });
			const txPrepare = vi.fn(async () => enveloped({ outcome: "accepted" as const }));
			const txCommit = vi.fn(async () => enveloped({ outcome: "committed" as const }));
			vi.spyOn(doStubs, "partitionStubByName").mockReturnValue({ txPrepare, txCommit } as unknown as DurableObjectStub<PartitionDO>);

			await tc.runPrepareRecovery(TX_ID, TOKEN, BUDGET_MS);

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

			tc.cancelTransactionInStore(TX_ID, TOKEN);

			const row = state.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE transaction_id = ?`, TX_ID).toArray()[0];
			expect(row.state).toBe("PREPARED");
		});
	});

	// Two drives of one transaction run at the same time. The other drive wrote its decision while this
	// drive waited for a prepare answer. A fan-out that follows the losing decision breaks atomicity:
	// a cancel releases a lock that the commit must apply, and the participant then answers the commit
	// with the idempotent success.
	describe("a drive whose decision lost to a concurrent drive", () => {
		// The row is new, so the `tx_recovery` job of the coordinator does not drive it as a third drive.
		function twoParticipants(state: DurableObjectState) {
			seed(state, "PREPARING", undefined, Date.now());
			state.storage.sql.exec(`UPDATE tc_items SET partition_do_name = 'p2' WHERE transaction_id = ? AND op_index = 1`, TX_ID);
			insertParticipant(state, { name: "p1" });
			insertParticipant(state, { name: "p2" });
		}

		function mockPartitions(p2Prepare: () => Promise<unknown>) {
			const txCommit = vi.fn(async () => enveloped({ outcome: "committed" as const }));
			const txCancel = vi.fn(async () => enveloped(undefined));
			vi.spyOn(doStubs, "partitionStubByName").mockImplementation(
				(_env, _ctx, name) =>
					({
						txPrepare: name === "p1" ? async () => enveloped({ outcome: "accepted" as const }) : p2Prepare,
						txCommit,
						txCancel,
					}) as unknown as DurableObjectStub<PartitionDO>,
			);
			return { txCommit, txCancel };
		}

		// The coordinator is shared with other tests, and a background drive of their transactions can
		// use these mocks. Only the calls for this transaction count.
		const callsFor = (fn: ReturnType<typeof vi.fn>) =>
			fn.mock.calls.filter((args) => (args[1] as { transactionId?: string } | undefined)?.transactionId === TX_ID);

		const stateOf = (state: DurableObjectState) =>
			state.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE transaction_id = ?`, TX_ID).one().state;

		it("sends no cancel when the other drive decided commit", async () => {
			await withCoordinator(async (tc, state) => {
				twoParticipants(state);
				const { txCommit, txCancel } = mockPartitions(async () => {
					// The other drive received an accept from both participants and wrote the commit decision.
					state.storage.sql.exec(`UPDATE tc_participants SET prepare_outcome = 'accepted' WHERE transaction_id = ?`, TX_ID);
					tc.markPrepared(TX_ID, TOKEN);
					throw new Error("p2 unreachable from this drive");
				});

				await expect(tc.drivePrepare(TX_ID, TOKEN, BUDGET_MS)).rejects.toThrow(fokosErrorWith("transaction_commit_pending"));

				expect(callsFor(txCancel)).toEqual([]);
				expect(callsFor(txCommit)).toEqual([]);
				expect(stateOf(state)).toBe("PREPARED");
			});
		});

		it("sends no commit when the other drive decided cancel", async () => {
			await withCoordinator(async (tc, state) => {
				twoParticipants(state);
				const { txCommit, txCancel } = mockPartitions(async () => {
					// The other drive reached the hold limit and wrote the cancel decision.
					tc.cancelTransactionInStore(TX_ID, TOKEN);
					return enveloped({ outcome: "accepted" as const });
				});

				await expect(tc.drivePrepare(TX_ID, TOKEN, BUDGET_MS)).resolves.toMatchObject({ outcome: "cancelled" });

				expect(callsFor(txCommit)).toEqual([]);
				expect(callsFor(txCancel)).toEqual([]);
				expect(stateOf(state)).toBe("CANCELLING");
			});
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
			const txCancelP1 = vi.fn(async () => enveloped(undefined));
			const txCancelP2 = vi.fn(async () => enveloped(undefined));
			vi.spyOn(doStubs, "partitionStubByName").mockImplementation(
				(_env, _ctx, name) =>
					({
						txPrepare,
						txCancel: name === "p1" ? txCancelP1 : txCancelP2,
					}) as unknown as DurableObjectStub<PartitionDO>,
			);

			await tc.runPrepareRecovery(TX_ID, TOKEN, BUDGET_MS);

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
			const txCancel = vi.fn(async () => enveloped(undefined));
			vi.spyOn(doStubs, "partitionStubByName").mockReturnValue({ txPrepare, txCancel } as unknown as DurableObjectStub<PartitionDO>);

			await tc.runPrepareRecovery(TX_ID, TOKEN, BUDGET_MS);

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
			tc.sweepExpiredTransactions();

			expect(countRows(state, "tc_state")).toBe(0);
		});
	});
});

describe("TransactionCoordinatorDO - migration pages", () => {
	it("pages the ledger by the row budget, and keeps only the transactions the target owns", async () => {
		const budget: FokosMigrationPageBudget = { pageBytes: 20 * 1024 * 1024, pageRows: 10, scanRows: 10 };
		await withCoordinator((tc, state) => {
			const ids = Array.from({ length: 2 * budget.pageRows + 5 }, (_, i) => `tx-${String(i).padStart(5, "0")}`);
			for (const id of ids) {
				insertState(state, { token: `token-${id}`, transactionId: id, state: "COMMITTED", createdAt: BASE_TIME });
			}

			const all = () => true;
			const first = tc.buildMigrationPage(null, all, budget);
			expect(first.page.map((tx) => tx.state.transaction_id)).toEqual(ids.slice(0, budget.pageRows));
			expect(first.nextCursor).toBe(ids[budget.pageRows - 1]);
			const second = tc.buildMigrationPage(first.nextCursor, all, budget);
			expect(second.page.map((tx) => tx.state.transaction_id)).toEqual(ids.slice(budget.pageRows, 2 * budget.pageRows));
			const last = tc.buildMigrationPage(second.nextCursor, all, budget);
			expect(last.page.map((tx) => tx.state.transaction_id)).toEqual(ids.slice(2 * budget.pageRows));
			expect(last.nextCursor).toBeNull();

			// A page that the target owns no row of still advances the cursor past the rows it read.
			const none = tc.buildMigrationPage(null, () => false, budget);
			expect(none).toEqual({ page: [], nextCursor: ids[budget.pageRows - 1] });
		});
	});

	it.each([
		["PREPARING", true],
		["PREPARED", false],
		["COMMITTING", false],
		["CANCELLING", false],
		["COMMITTED", false],
		["CANCELLED", false],
	] as const)("a %s transaction carries its payload in the page: %s", async (tcState, withPayload) => {
		const budget: FokosMigrationPageBudget = { pageBytes: 20 * 1024 * 1024, pageRows: 10, scanRows: 10 };
		await withCoordinator((tc, state) => {
			seed(state, tcState);
			state.storage.sql.exec(`UPDATE tc_items SET conditions_json = '{"op":"test"}' WHERE transaction_id = ?`, TX_ID);

			const { page } = tc.buildMigrationPage(null, () => true, budget);

			expect(page).toHaveLength(1);
			expect(page[0].items).toHaveLength(2);
			expect(page[0].items[0]).toMatchObject(
				withPayload
					? { data: "v", data_kind: 1, conditions_json: '{"op":"test"}' }
					: { data: null, data_kind: null, conditions_json: null },
			);
		});
	});
});

describe("TransactionCoordinatorDO - recoverTransactionForParticipant", () => {
	// A lock stores only the name of the coordinator and the token. The coordinator routes the call
	// with the route context it stored.
	it("answers from the ledger through the stored route context", async () => {
		await withCoordinator(async (tc, state) => {
			insertState(state, { token: TOKEN, transactionId: TX_ID, state: "COMMITTED", createdAt: BASE_TIME, completedAt: BASE_TIME });

			const coordinator = tc as unknown as TransactionCoordinatorDO;
			await expect(coordinator.recoverTransactionForParticipant({ transactionId: TX_ID, idempotencyToken: TOKEN })).resolves.toEqual({
				state: "COMMITTED",
			});
		});
	});

	it("answers not_found on a coordinator with no identity, and does not create one", async () => {
		const stub = testCoordinatorStubByName(testCoordinatorContext().doName);

		await expect(stub.recoverTransactionForParticipant({ transactionId: TX_ID, idempotencyToken: TOKEN })).resolves.toEqual({
			state: "not_found",
		});
		await runInDurableObject(stub, (instance: TransactionCoordinatorDO) => {
			expect(instance.fokos.initialized()).toBe(false);
		});
	});
});

describe("TransactionCoordinatorDO - fokosDestroy", () => {
	// The idempotency window lives in tc_state. A coordinator that survives FokosDB.destroy() answers a
	// replayed clientRequestToken with the old transaction's outcome — "committed" for data that was
	// wiped with the partitions.
	it("wipes the idempotency window and the alarm, then evicts the instance", async () => {
		await withCoordinator(async (tc, state) => {
			seed(state, "COMMITTED");
			await state.storage.setAlarm(Date.now() + 60_000);
			expect(countRows(state, "tc_state")).toBe(1);
			expect(countRows(state, "tc_items")).toBe(2);
			expect(tableNames(state)).toContain("tc_participants");

			// ctx.abort() genuinely evicts the instance, which hangs the workers pool — the same reason
			// test/destroy.test.ts is skipped. Stubbing it keeps the eviction assertable (it is what makes
			// the next caller re-run the migrations) without killing the run.
			const abort = vi.spyOn(state, "abort").mockImplementation(() => {});

			await (tc as unknown as TransactionCoordinatorDO).fokosDestroy();

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
		const def = code === "partition_migrating" ? SHARDING_UNAVAILABLE_CODES.partition_migrating : UNAVAILABLE_CODES.partition_over_size;
		return FokosError.toWire(new FokosUnavailableError(def, { message: "refused" }));
	}

	function cancelWith(state: DurableObjectState, tc: CoordinatorInternals) {
		state.storage.sql.exec(`UPDATE tc_items SET partition_do_name = 'p2' WHERE transaction_id = ? AND op_index = 1`, TX_ID);
		tc.cancelTransactionInStore(TX_ID, TOKEN);
		const response = tc.loadFinalResponse(TX_ID, TOKEN);
		if (response.outcome !== "cancelled") {
			throw new Error("the transaction did not cancel");
		}
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
