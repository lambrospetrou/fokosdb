import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import type { FokosDBTransactionCoordinatorDO } from "../../src/server/do-transaction-coordinator.js";
import { CORE_INTERNAL_CODES, FokosError, TRANSACTION_PENDING_CODES } from "../../src/shared/errors.js";
import { acceptAnyClockSkew, controlledCoordinator, controlledPartition, type Key, keysAcrossPartitions, makeDB } from "./tx-helpers.js";

/**
 * A runtime fault on the hop from `db.ts` to the coordinator hides the outcome of the transaction:
 * the coordinator can have stored it, and then the `tx_recovery` job finishes it. `transactWriteItems`
 * answers the retryable `transaction_undecided` with the token, and a replay of the token returns the
 * final outcome. A foreign error with no runtime marker stays `foreign_error`.
 */
describe("transactions - a runtime fault on the hop to the coordinator", () => {
	const putAll = (keys: Key[], data: string) => keys.map((key) => ({ ...key, operation: "put" as const, data }));

	async function expectCommitted(db: FokosDB, keys: Key[], data: string) {
		for (const key of keys) {
			await expect(db.getItem(key)).resolves.toMatchObject({ item: { data } });
		}
	}

	it("answers transaction_undecided when the coordinator resets during the prepare, and a token replay commits after recovery", async () => {
		const db = makeDB({ controlled: true });
		const keys = keysAcrossPartitions(db, 2, `tc-reset-${crypto.randomUUID()}`);
		await acceptAnyClockSkew(db, keys);
		const participant = controlledPartition(db, keys[1]);
		await participant.testHoldPrepare();
		const token = `tc-reset-${crypto.randomUUID()}`;
		const items = putAll(keys, "after-reset");

		const write = db.transactWriteItems({ items, clientRequestToken: token }).then(
			() => null,
			(e: unknown) => e,
		);
		await vi.waitFor(async () => expect(await participant.testPrepareParked()).toBe(true), { timeout: 5_000, interval: 10 });
		await controlledCoordinator(db)
			.testReset()
			.catch(() => {});
		const err = await write;
		await participant.testReleasePrepare();

		expect(FokosError.isCode(err, TRANSACTION_PENDING_CODES.transaction_undecided)).toBe(true);
		expect(err).toMatchObject({
			origin: "service",
			httpStatusHint: 503,
			attributes: { idempotencyToken: token },
			cause: { code: CORE_INTERNAL_CODES.foreign_error.code, attributes: { durableObjectReset: true } },
		});

		// The reset lost the drive of the request, so the `tx_recovery` job of the new instance drives the transaction.
		await vi.waitFor(
			async () => {
				await runInDurableObject(
					controlledCoordinator(db),
					async (instance: FokosDBTransactionCoordinatorDO, state: DurableObjectState) => {
						state.storage.sql.exec(`UPDATE tc_state SET next_recovery_at = 0 WHERE completed_at IS NULL`);
						await (instance as unknown as { recoverStaleTransactions(): Promise<void> }).recoverStaleTransactions();
					},
				);
				await expect(db.transactWriteItems({ items, clientRequestToken: token })).resolves.toMatchObject({ idempotencyToken: token });
			},
			{ timeout: 10_000, interval: 50 },
		);
		await expectCommitted(db, keys, "after-reset");
	});

	it.each([{ retryable: true }, { overloaded: true }, { durableObjectReset: true }])(
		"answers transaction_undecided with the token of db.ts for a fault with %o after the work, and a replay of that token commits",
		async (fault) => {
			const db = makeDB({ controlled: true });
			const keys = keysAcrossPartitions(db, 2, `tc-fault-${crypto.randomUUID()}`);
			await acceptAnyClockSkew(db, keys);
			await controlledCoordinator(db).testInitiateWriteFault(fault);
			const items = putAll(keys, "after-fault");

			const err = await db.transactWriteItems({ items }).then(
				() => null,
				(e: unknown) => e,
			);

			expect(FokosError.isCode(err, TRANSACTION_PENDING_CODES.transaction_undecided)).toBe(true);
			expect(err).toMatchObject({ cause: { code: CORE_INTERNAL_CODES.foreign_error.code, attributes: fault } });
			const idempotencyToken = (err as FokosError).attributes.idempotencyToken;
			expect(idempotencyToken).toEqual(expect.any(String));
			await expect(db.transactWriteItems({ items, clientRequestToken: idempotencyToken as string })).resolves.toMatchObject({
				idempotencyToken,
			});
			await expectCommitted(db, keys, "after-fault");
		},
	);

	it("keeps foreign_error for an error with no runtime marker", async () => {
		const db = makeDB({ controlled: true });
		const keys = keysAcrossPartitions(db, 2, `tc-defect-${crypto.randomUUID()}`);
		await acceptAnyClockSkew(db, keys);
		await controlledCoordinator(db).testInitiateWriteFault({});

		const err = await db.transactWriteItems({ items: putAll(keys, "defect"), clientRequestToken: `tc-defect-${crypto.randomUUID()}` }).then(
			() => null,
			(e: unknown) => e,
		);

		expect(FokosError.isCode(err, CORE_INTERNAL_CODES.foreign_error)).toBe(true);
		expect(err).toMatchObject({ origin: "internal", httpStatusHint: 500 });
	});
});
