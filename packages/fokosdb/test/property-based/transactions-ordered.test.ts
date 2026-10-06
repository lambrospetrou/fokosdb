// Stateless property for a write transaction with more than one operation for one item. It draws a
// random start state and a random operation list over a small key pool, and runs the list three
// ways on one TransactionParticipant with one pinned transaction timestamp:
//
//   - the single-partition path (`executeSingleShot` with the whole list),
//   - the two-phase path (`prepareLocal`, then `commitLocal`),
//   - the reference run: each operation alone, as a one-operation `executeSingleShot`.
//
// All three must leave the same `items` (data, `v`, TTL, stamps, `item_id`, `est_row_bytes`), the
// same `deletion_metadata`, and the same `key_size_estimates`. Every operation of the list passes,
// so the comparison holds: a check has a condition that is always true, and the list has no
// condition on `v` after a write, because the version-reference check refuses that case (its own
// tests cover it).
//
// The three runs share one Durable Object. Before each run, the property empties the tables, so each
// run starts from the same state. harness.ts says how to replay a failure.
import { runInDurableObject } from "cloudflare:test";
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { DEFAULT_PARTITION_CONFIG } from "../../src/server/host-config.js";
import { KeyCodec, KeyPairMap } from "../../src/sharding/key-codec.js";
import { compileConditionExpression, compileUpdateExpression } from "../../src/shared/expression/compiler.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { TransactionParticipant } from "../../src/shared/partition/transaction-participant.js";
import { TX_ORDER_TS_UNITS_PER_MS } from "../../src/shared/transaction-limits.js";
import type { TransactionItem, TransactionItemKey } from "../../src/shared/transaction-wire-types.js";
import { testCoordinatorRef, testPartitionStub } from "../stub-helpers.js";
import { propertyRuns } from "./harness.js";

const PROPERTY_RUNS = propertyRuns(100);
const BASE_NOW = 1_000_000;
const T = (BASE_NOW + 100) * TX_ORDER_TS_UNITS_PER_MS;
const KEYS = ["a", "b", "c"].map((sk) => ({ hashKey: KeyCodec.encode("h"), sortKey: KeyCodec.encode(sk) }));

const alwaysTrue = compileConditionExpression({
	op: "or",
	args: [
		{ op: "exists", args: [{ ref: "hashKey" }] },
		{ op: "not_exists", args: [{ ref: "hashKey" }] },
	],
});
const incN = compileUpdateExpression([
	{
		action: "set",
		target: { ref: "data", path: "$.n" },
		value: { fn: "+", args: [{ fn: "if_not_exists", args: [{ ref: "data", path: "$.n" }, { val: 0 }] }, { val: 1 }] },
	},
]);

type Op = Omit<TransactionItem, "opIndex">;

const arbTtl = fc.option(
	fc.integer({ min: 1, max: 5 }).map((d) => 4_000_000_000 + d),
	{ nil: undefined },
);
const arbOp: fc.Arbitrary<Op> = fc
	.tuple(fc.constantFrom(...KEYS), fc.constantFrom("put", "update", "delete", "check"), fc.integer({ min: 0, max: 400 }), arbTtl)
	.map(([key, operation, size, ttlAt]): Op => {
		switch (operation) {
			case "put":
				return {
					...key,
					operation,
					data: JSON.stringify({ s: "x".repeat(size) }),
					kind: "json",
					...(ttlAt === undefined ? {} : { ttlAt }),
				};
			case "update":
				return { ...key, operation, update: incN, ...(ttlAt === undefined ? {} : { ttlAt }) };
			case "delete":
				return { ...key, operation };
			default:
				return { ...key, operation: "check", condition: alwaysTrue };
		}
	});
/** The start state of each key: absent, or present with a json document of this size. */
const arbStart = fc.array(fc.option(fc.integer({ min: 0, max: 400 }), { nil: undefined }), {
	minLength: KEYS.length,
	maxLength: KEYS.length,
});
const arbCase = fc.record({
	start: arbStart,
	deletedV: fc.integer({ min: 0, max: 5 }),
	ops: fc.array(arbOp, { minLength: 1, maxLength: 12 }),
});

function reset(storage: DurableObjectStorage): void {
	for (const table of ["items", "key_size_estimates", "pending_transactions", "pending_tx_info"]) {
		storage.sql.exec(`DELETE FROM ${table}`);
	}
	storage.sql.exec(`UPDATE deletion_metadata SET max_delete_tx_order_ts = 0, max_deleted_v = 0`);
}

function seed(store: PartitionStore, start: (number | undefined)[], deletedV: number): void {
	// A deleted row with v = deletedV raises max_deleted_v, so a new row starts above it.
	for (let i = 0; i < deletedV; i++) {
		store.upsertItem({ hk: KeyCodec.encode("gone"), sk: KeyCodec.encode("s"), data: "g", kind: "text", ttlAt: null, txOrderTs: 1 });
	}
	if (deletedV > 0) {
		store.deleteItem({ hk: KeyCodec.encode("gone"), sk: KeyCodec.encode("s"), txOrderTs: 2 });
	}
	start.forEach((size, i) => {
		if (size !== undefined) {
			store.upsertItem({
				hk: KEYS[i].hashKey,
				sk: KEYS[i].sortKey,
				data: JSON.stringify({ s: "y".repeat(size) }),
				kind: "json",
				ttlAt: null,
				txOrderTs: 3,
			});
		}
	});
}

const b64 = (value: unknown) => (typeof value === "string" ? value : new Uint8Array(value as ArrayBuffer).toBase64());

function snapshot(storage: DurableObjectStorage) {
	return {
		items: storage.sql
			.exec(
				`SELECT item_id, hk, sk, v, data, data_kind, ttl_epoch_utc_seconds, last_read_ts, last_write_ts, est_row_bytes FROM items ORDER BY hk, sk`,
			)
			.toArray()
			.map((r) => ({ ...r, hk: b64(r.hk), sk: b64(r.sk), data: b64(r.data) })),
		deletion: storage.sql.exec(`SELECT max_delete_tx_order_ts, max_deleted_v FROM deletion_metadata`).one(),
		sizes: storage.sql
			.exec(`SELECT hk, est_bytes FROM key_size_estimates ORDER BY hk`)
			.toArray()
			.map((r) => ({ ...r, hk: b64(r.hk) })),
		locks: storage.sql.exec(`SELECT COUNT(*) AS n FROM pending_transactions`).one().n,
	};
}

function uniqueKeys(items: readonly TransactionItem[]): TransactionItemKey[] {
	const keys = new KeyPairMap<TransactionItemKey>();
	for (const { hashKey, sortKey } of items) {
		keys.set(hashKey, sortKey, { hashKey, sortKey });
	}
	return [...keys.values()];
}

describe("ordered per-item execution — the paths agree with the reference run", () => {
	it(
		"gives the same items, deletion metadata, and size estimates on both paths and in the reference run",
		{ timeout: PROPERTY_RUNS * 1_000 },
		async () => {
			const stub = testPartitionStub(`ordered-property.${crypto.randomUUID()}`);
			await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
				const store = new PartitionStore(state.storage);
				const participant = new TransactionParticipant({
					store,
					now: () => BASE_NOW,
					maxClockSkewMs: () => DEFAULT_PARTITION_CONFIG.maxClockSkewMs,
					staleTransactionMs: () => DEFAULT_PARTITION_CONFIG.staleTransactionMs,
					txOrderTimestamp: () => T,
					ownerCheck: () => () => true,
				});
				const run = (start: (number | undefined)[], deletedV: number, apply: () => void) => {
					reset(state.storage);
					seed(store, start, deletedV);
					apply();
					return snapshot(state.storage);
				};

				await fc.assert(
					fc.asyncProperty(arbCase, async ({ start, deletedV, ops }) => {
						const items = ops.map((op, opIndex) => ({ ...op, opIndex }));
						const reference = run(start, deletedV, () => {
							for (const op of ops) {
								expect(participant.executeSingleShot({ items: [{ ...op, opIndex: 0 }] }).response).toEqual({ outcome: "committed" });
							}
						});
						const single = run(start, deletedV, () => {
							expect(participant.executeSingleShot({ items }).response).toEqual({ outcome: "committed" });
						});
						const twoPhase = run(start, deletedV, () => {
							const transactionId = crypto.randomUUID();
							const prepare = { transactionId, coordinator: testCoordinatorRef("tok-property"), transactionTimestamp: T, items };
							expect(participant.prepareLocal(prepare)).toEqual({ outcome: "accepted" });
							participant.commitLocal({ transactionId, transactionTimestamp: T, items: uniqueKeys(items) });
						});
						expect(reference.locks).toBe(0);
						expect(single).toEqual(reference);
						expect(twoPhase).toEqual(reference);
					}),
					{ numRuns: PROPERTY_RUNS },
				);
			});
		},
	);
});
