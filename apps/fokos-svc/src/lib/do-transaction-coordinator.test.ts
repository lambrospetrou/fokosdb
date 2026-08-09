import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import { KeyCodec } from "./partition-topology/key-codec.js";
import type { InitiateWriteResponse, RejectionReason, TCState } from "./transaction-types.js";

const kb = (s: string) => KeyCodec.encode(s);
const ABSENT_SK = KeyCodec.encodeOptional(undefined);

const TX_ID = "tx-1";
const TOKEN = "tok-1";

// loadFinalResponse is private: TypeScript's `private` is compile-time only, so the running instance
// exposes it. We reach it directly because the states under test (PREPARED / COMMITTING / CANCELLING)
// are only reachable end-to-end by exhausting the participant retry budget — 10 attempts with backoff
// up to 2 s, far too slow for a unit test. The state→outcome mapping is the whole contract here, so
// testing it directly is both faster and more precise than driving it end-to-end.
type WithLoadFinalResponse = {
	loadFinalResponse(transactionId: string, idempotencyToken: string): InitiateWriteResponse;
};

function seed(state: DurableObjectState, tcState: TCState, reason?: RejectionReason): void {
	state.storage.sql.exec(`DELETE FROM tc_state`);
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

// Every table this DO owns — the tc_* tables plus the migrations bookkeeping. The `_cf_*` tables are
// the platform's own and are excluded.
function tableNames(state: DurableObjectState): string[] {
	return state.storage.sql
		.exec<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' ORDER BY name`)
		.toArray()
		.map((r) => r.name);
}

async function withCoordinator(fn: (tc: WithLoadFinalResponse, state: DurableObjectState) => void): Promise<void> {
	const stub = TransactionCoordinatorDO.getByName(env.TRANSACTION_COORDINATOR_DO, `tc-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (instance: TransactionCoordinatorDO, state: DurableObjectState) => {
		fn(instance as unknown as WithLoadFinalResponse, state);
	});
}

describe("TransactionCoordinatorDO - loadFinalResponse answers from the decision, not the cleanup", () => {
	// PREPARED is the point of no return: every participant accepted, and nothing transitions
	// PREPARED → CANCELLING. COMMITTING has already begun applying. Both MUST report committed even
	// while a straggling participant has not acknowledged, or the caller is told a durable write failed.
	it.each(["PREPARED", "COMMITTING", "COMMITTED"] as const)("reports committed in state %s", async (tcState) => {
		await withCoordinator((tc, state) => {
			seed(state, tcState);
			// toEqual, not toMatchObject: an item echo reappearing here is a failure, not an extra.
			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toEqual({
				outcome: "committed",
				transactionId: TX_ID,
				idempotencyToken: TOKEN,
			});
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

	// The only states where the outcome can still go either way, and so the only retryable answer.
	it.each(["CREATED", "PREPARING"] as const)("throws a retryable error in undecided state %s", async (tcState) => {
		await withCoordinator((tc, state) => {
			seed(state, tcState);
			expect(() => tc.loadFinalResponse(TX_ID, TOKEN)).toThrowError(/outcome is not yet decided/);
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
