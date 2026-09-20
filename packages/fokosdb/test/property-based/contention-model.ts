// Write-write contention: the transactions of one batch write the SAME items at the same time.
//
// The batch of `arbConcurrentBatch` (model.ts) keeps the key sets of its transactions disjoint, so
// the model predicts every outcome exactly. Here they overlap, and no model can predict which
// transaction wins. The oracle is serializability instead: once the batch has drained, the state of
// the pool must be the state that SOME order of the committed transactions leaves behind, and every
// committed transaction must find its premises true where it stands in that order. 2PC holds the
// lock of an item from the prepare to the outcome, so two transactions that meet on an item never
// commit together and such an order always exists.
//
// A lost update, a loser that applied a part of its operations, and a winner whose condition never
// held all fit no order, so each one fails the run.
//
// Every transaction here carries its own `clientRequestToken`. Under contention a commit can answer
// `transaction_commit_pending`, which says the decision is durable and not yet applied everywhere.
// The token names the coordinator, so the same call again resumes that same transaction and reads
// its one outcome. Without a token the repeat would start a second transaction and write twice.
import fc from "fast-check";
import { expect } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import {
	CONFLICT_CODES,
	FokosError,
	FokosTransactionPendingError,
	FokosUnavailableError,
	UNAVAILABLE_CODES,
} from "../../src/shared/errors.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import type { JsonValue } from "../../src/shared/json-types.js";
import type { TransactWriteOperationResult } from "../../src/shared/transaction-api-types.js";
import { keyId, type DataKind, type ItemData, type ItemKey } from "./arbitraries.js";
import {
	applyTxOps,
	describeTxOps,
	expectedRejection,
	expectKeyUnlocked,
	recordTransaction,
	toTransactWriteItem,
	txOpArbitrary,
	untilAvailable,
	type Model,
	type ModelItem,
	type TransactionStats,
	type TxOp,
} from "./model.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// The reasons a transaction of a contending batch may cancel. A premise fails with
// `condition_failed` or `update_not_applicable`; a contender holds the lock of an item
// (`pending_conflict`) or wrote it in the same millisecond (`timestamp_conflict`); and a partition
// that splits or imports its share answers 503. Any other code is a defect.
const ACCEPTED_CANCEL_CODES: ReadonlySet<string> = new Set([
	"condition_failed",
	"update_not_applicable",
	"pending_conflict",
	"timestamp_conflict",
	"clock_skew",
	...Object.keys(UNAVAILABLE_CODES),
]);

// ─── The state of the pool ────────────────────────────────────────────────────

// A json document comes back from SQLite with its fields in its own order, and the order says
// nothing about the value, so the canonical form sorts them. Text and bytes never compare equal,
// because the signature carries the `kind` of the item as well.
function canonicalJson(value: JsonValue): string {
	if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
	if (typeof value === "object" && value !== null) {
		return `{${Object.keys(value)
			.sort()
			.map((field) => `${JSON.stringify(field)}:${canonicalJson(value[field])}`)
			.join(",")}}`;
	}
	return JSON.stringify(value);
}

function canonicalData(data: ItemData): string {
	if (data instanceof Uint8Array) return [...data].map((b) => b.toString(16).padStart(2, "0")).join("");
	if (typeof data === "string") return JSON.stringify(data);
	return canonicalJson(data);
}

/** One line per key. Two states agree exactly when their signatures are the same string. */
function stateSignature(m: Model, keys: readonly ItemKey[]): string {
	return keys
		.map((key) => {
			const item = m.items.get(keyId(key));
			return `${keyId(key)} → ${item === undefined ? "absent" : `v${item.version} ${item.kind} ${canonicalData(item.data)}`}`;
		})
		.join("\n");
}

const cloneModel = (m: Model): Model => ({ items: new Map([...m.items].map(([id, item]) => [id, { ...item }])) });

// A cancelled transaction answers its caller as soon as it is decided, and its last participants can
// still hold their locks. A read transaction refuses an item that holds a pending write, so the read
// repeats while that clears. A lock that never clears fails the run here.
const QUIET_READ_TIMEOUT_MS = 15_000;
const QUIET_READ_DELAY_MS = 50;

/** Reads the state of every pool key once the batch has settled. */
async function readPool(db: FokosDB, keys: readonly ItemKey[]): Promise<Model> {
	const deadlineMs = Date.now() + QUIET_READ_TIMEOUT_MS;
	for (;;) {
		try {
			const res = await untilAvailable(() => db.transactGetItems({ items: [...keys] }));
			const items = new Map<string, ModelItem>();
			keys.forEach((key, i) => {
				const read = res.items[i];
				if (read.found) items.set(keyId(key), { data: read.data as ItemData, kind: read.kind as DataKind, version: read.version });
			});
			return { items };
		} catch (e) {
			const clears = FokosError.isCode(e, CONFLICT_CODES.pending_write) || FokosError.isCode(e, CONFLICT_CODES.read_conflict);
			if (!clears || Date.now() > deadlineMs) throw e;
			await sleep(QUIET_READ_DELAY_MS);
		}
	}
}

// ─── The oracle ───────────────────────────────────────────────────────────────

function permutations<T>(items: readonly T[]): T[][] {
	if (items.length <= 1) return [[...items]];
	return items.flatMap((item, i) => permutations([...items.slice(0, i), ...items.slice(i + 1)]).map((rest) => [item, ...rest]));
}

/** Applies one transaction to a candidate order, or reports that its premises do not hold there. */
function applyWhenPremisesHold(m: Model, ops: readonly TxOp[]): boolean {
	// Every operation of a transaction reads the state the transaction starts from, and the keys of
	// one transaction are unique, so the premises are evaluated before any of them applies.
	if (ops.some((op) => expectedRejection(m, op) !== null)) return false;
	applyTxOps(m, ops);
	return true;
}

/** True when some order of the committed transactions turns `before` into the observed state. */
function someOrderExplains(before: Model, committed: readonly (readonly TxOp[])[], observed: Model, keys: readonly ItemKey[]): boolean {
	const target = stateSignature(observed, keys);
	return permutations(committed).some((order) => {
		const candidate = cloneModel(before);
		return order.every((ops) => applyWhenPremisesHold(candidate, ops)) && stateSignature(candidate, keys) === target;
	});
}

// ─── One transaction of a batch ───────────────────────────────────────────────

const PENDING_RETRY_DELAY_MS = 50;
const PENDING_RETRY_LIMIT = 600;

/**
 * Sends one transaction under `token` until it answers an outcome. `undefined` says it committed,
 * and an array holds the positional answers of a cancel.
 *
 * A 503 and a commit that is still pending both clear on their own, and the token makes the repeat
 * safe: the coordinator finds the ledger row of the first attempt and resumes that transaction. A
 * cancel is terminal for the same reason — the coordinator stored it and answers it again — so it is
 * an outcome here and not something to retry.
 */
async function sendPinned(db: FokosDB, ops: readonly TxOp[], token: string): Promise<TransactWriteOperationResult[] | undefined> {
	const items = ops.map(toTransactWriteItem);
	for (let attempt = 0; ; attempt++) {
		try {
			await db.transactWriteItems({ items, clientRequestToken: token });
			return undefined;
		} catch (e) {
			if (FokosTransactionCancelledError.is(e)) return e.results;
			const transient = FokosUnavailableError.is(e) || FokosTransactionPendingError.is(e);
			if (!transient || attempt >= PENDING_RETRY_LIMIT) throw e;
			await sleep(PENDING_RETRY_DELAY_MS);
		}
	}
}

/** The outcome as one comparable string: a commit, or the answer to each operation in request order. */
function describeOutcome(results: TransactWriteOperationResult[] | undefined): string {
	if (results === undefined) return "committed";
	return `cancelled(${results.map((r) => (r.outcome === "rejected" ? r.reason.code : r.outcome)).join(", ")})`;
}

/** One logical transaction of a batch: its operations, and the token that names its coordinator. */
export class ContendingTransaction {
	#token = "";

	constructor(
		readonly ops: TxOp[],
		readonly stats: TransactionStats,
	) {}

	/** Sends the transaction. Each call mints a token, so one run never resumes the transaction of another. */
	async send(db: FokosDB): Promise<TransactWriteOperationResult[] | undefined> {
		this.#token = `pbt${crypto.randomUUID().replaceAll("-", "")}`;
		return await recordTransaction(this.stats, () => sendPinned(db, this.ops, this.#token));
	}

	/** Sends the same operations under the same token again. It must answer what `send` answered and write nothing more. */
	async replay(db: FokosDB): Promise<TransactWriteOperationResult[] | undefined> {
		return await sendPinned(db, this.ops, this.#token);
	}

	toString(): string {
		return `Tx(${describeTxOps(this.ops)})`;
	}
}

// ─── The command ──────────────────────────────────────────────────────────────

/**
 * A batch of transactions that meet on the same items, sent at one time. Once it has drained, the
 * state of the pool must agree with some order of the transactions that committed, one transaction
 * must answer the same outcome when it is sent again under its token, and every key the batch
 * touched must take a non-transactional write, which proves that the batch left no lock behind.
 */
export class ContendingTransactWrites implements fc.AsyncCommand<Model, FokosDB> {
	constructor(
		readonly transactions: ContendingTransaction[],
		readonly keys: readonly ItemKey[],
		readonly replayIndex: number,
		readonly stats: TransactionStats,
	) {}

	check(): boolean {
		return true;
	}

	async run(m: Model, db: FokosDB): Promise<void> {
		const before = cloneModel(m);
		// A transaction orders itself with a millisecond timestamp against the last write of every item
		// it touches, so the batch first lets the clock move past the command before it.
		await sleep(2);

		const settled = await Promise.allSettled(this.transactions.map((tx) => tx.send(db)));
		// Every transaction is awaited before the first failure is raised, so a failing batch never
		// leaves the next command with a request of this one still in flight.
		for (const outcome of settled) if (outcome.status === "rejected") throw outcome.reason;
		const outcomes = settled.flatMap((outcome) => (outcome.status === "fulfilled" ? [outcome.value] : []));

		for (const results of outcomes) {
			for (const r of results ?? []) if (r.outcome === "rejected") expect(ACCEPTED_CANCEL_CODES).toContain(r.reason.code);
		}

		const committed = this.transactions.filter((_, i) => outcomes[i] === undefined).map((tx) => tx.ops);
		const observed = await readPool(db, this.keys);
		expect(
			someOrderExplains(before, committed, observed, this.keys),
			`no order of the committed transactions leaves the state the batch left\n${this.describe(before, observed, outcomes)}`,
		).toBe(true);
		// The observed state agrees with an order of the batch, so it is the state the run goes on from.
		m.items = observed.items;

		const replayed = this.transactions[this.replayIndex];
		const answer = await replayed.replay(db);
		expect(describeOutcome(answer), `a replay under the same token answered differently — ${replayed}`).toBe(
			describeOutcome(outcomes[this.replayIndex]),
		);
		const afterReplay = await readPool(db, this.keys);
		expect(stateSignature(afterReplay, this.keys), `a replay under the same token wrote again — ${replayed}`).toBe(
			stateSignature(observed, this.keys),
		);

		for (const tx of this.transactions) {
			for (const op of tx.ops) await expectKeyUnlocked(m, db, op.key, this.stats);
		}
	}

	private describe(before: Model, observed: Model, outcomes: readonly (TransactWriteOperationResult[] | undefined)[]): string {
		return [
			`before:\n${stateSignature(before, this.keys)}`,
			...this.transactions.map((tx, i) => `${describeOutcome(outcomes[i])} ${tx}`),
			`observed:\n${stateSignature(observed, this.keys)}`,
		].join("\n");
	}

	toString(): string {
		return `Contending(${this.transactions.join(" || ")})`;
	}
}

const MAX_CONTENDING_TRANSACTIONS = 4;
// The keys of one batch. A small number of them makes a meeting on an item the common case and
// still leaves room for a transaction that spans partitions.
const MAX_CONTENDED_KEYS = 3;

/**
 * A batch of 2 to `MAX_CONTENDING_TRANSACTIONS` transactions over the same few keys. Every
 * transaction of the batch operates on the first key of that pool, so the batch always contends,
 * and its other operations spread over the rest of the pool.
 */
export function arbContendingBatch(
	keys: readonly ItemKey[],
	data: fc.Arbitrary<ItemData>,
	stats: TransactionStats,
): fc.Arbitrary<ContendingTransactWrites> {
	return fc
		.shuffledSubarray([...keys], { minLength: 1, maxLength: MAX_CONTENDED_KEYS })
		.chain(([contended, ...rest]) => {
			// A transaction rejects two operations on one key, so the extra keys are unique and never the
			// contended one.
			const arbRest =
				rest.length === 0
					? fc.constant<TxOp[]>([])
					: fc.uniqueArray(txOpArbitrary(fc.constantFrom(...rest), data), { maxLength: rest.length, selector: (op) => keyId(op.key) });
			const arbOps = fc.tuple(txOpArbitrary(fc.constant(contended), data), arbRest).map(([first, others]) => [first, ...others]);
			return fc.record({
				transactions: fc.array(arbOps, { minLength: 2, maxLength: MAX_CONTENDING_TRANSACTIONS }),
				replayIndex: fc.nat({ max: MAX_CONTENDING_TRANSACTIONS - 1 }),
			});
		})
		.map(
			({ transactions, replayIndex }) =>
				new ContendingTransactWrites(
					transactions.map((ops) => new ContendingTransaction(ops, stats)),
					keys,
					replayIndex % transactions.length,
					stats,
				),
		);
}
