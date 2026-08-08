import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
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
// up to 2 s, far too slow for a unit test. The state→outcome mapping IS the behaviour being fixed.
type WithLoadFinalResponse = {
	loadFinalResponse(transactionId: string, idempotencyToken: string): InitiateWriteResponse;
};

function seed(state: DurableObjectState, tcState: TCState, reason?: RejectionReason): void {
	state.storage.sql.exec(`DELETE FROM tc_state`);
	state.storage.sql.exec(`DELETE FROM tc_items`);
	state.storage.sql.exec(
		`INSERT INTO tc_state (idempotency_token, transaction_id, state, transaction_ts, created_at, rejection_reason_json)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		TOKEN,
		TX_ID,
		tcState,
		1_000,
		1_000,
		reason === undefined ? null : JSON.stringify(reason),
	);
	// Two items, one with a sort key and one without, so the decode of the absent sentinel is covered.
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
			expect(tc.loadFinalResponse(TX_ID, TOKEN)).toEqual({
				outcome: "committed",
				transactionId: TX_ID,
				idempotencyToken: TOKEN,
				items: [{ hashKey: "hk1", sortKey: "sk1" }, { hashKey: "hk2" }],
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
