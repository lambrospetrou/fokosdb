// The shared model and commands of the stateful suites. The model is a map from `keyId` to the
// item the database must hold. Every command runs one public operation, compares the answer with
// the model, and then advances the model. A write transaction advances it only on a commit.
import fc from "fast-check";
import { expect } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import { CONFLICT_CODES, FokosError, FokosUnavailableError, UNAVAILABLE_CODES } from "../../src/shared/errors.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import type { ConditionExpression, UpdateExpression } from "../../src/shared/expression/types.js";
import type { JsonComposite, JsonPrimitive, JsonValue } from "../../src/shared/json-types.js";
import type { TransactWriteItem, TransactWriteOperationResult } from "../../src/shared/transaction-api-types.js";
import type { DeleteItemResult, PutItemResult } from "../../src/shared/types.js";
import { expectedDataKind, keyId, type DataKind, type ItemData, type ItemKey } from "./arbitraries.js";

export type ModelItem = { data: ItemData; kind: DataKind; version: number };
export type Model = { items: Map<string, ModelItem> };

/** Applies the put of a command to the model. A new item starts at version 1, an overwrite adds one. */
export function applyPut(m: Model, key: ItemKey, data: ItemData): number {
	const id = keyId(key);
	const version = (m.items.get(id)?.version ?? 0) + 1;
	m.items.set(id, { data, kind: expectedDataKind(data), version });
	return version;
}

/** Compares one read answer (a `getItem` result or one `transactGetItems` entry) with the model. */
export function expectRead(m: Model, key: ItemKey, res: unknown): void {
	const expected = m.items.get(keyId(key));
	if (expected === undefined) {
		expect(res).toMatchObject({ found: false, ...key });
		return;
	}
	expect(res).toMatchObject({ found: true, ...key, kind: expected.kind, version: expected.version });
	// `toMatchObject` matches a SUBSET of an object value, so it accepts a document that kept a field
	// the model removed. The data of a found item is therefore compared exactly.
	expect((res as { data: unknown }).data).toEqual(expected.data);
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const RETRY_DELAY_MS = 50;
const RETRY_LIMIT = 600;

/**
 * Runs one operation and repeats it while a partition is unavailable. A partition that splits or
 * imports its share of a parent answers 503 (`partition_migrating`, `partition_over_size`), and a
 * write transaction whose every failure is such a 503 cancels with `origin: "service"`. Both clear
 * on their own, and a client is expected to retry them. Every other error is a real failure.
 */
export async function untilAvailable<T>(fn: () => Promise<T>): Promise<T> {
	for (let attempt = 0; ; attempt++) {
		try {
			return await fn();
		} catch (e) {
			const transient = FokosUnavailableError.is(e) || (FokosTransactionCancelledError.is(e) && e.origin === "service");
			if (!transient || attempt >= RETRY_LIMIT) throw e;
			await sleep(RETRY_DELAY_MS);
		}
	}
}

// A cancelled transaction answers its caller as soon as it is decided, and the last participants of
// its fan-out can still hold their locks, which an alarm of the coordinator then clears. So a lock
// that clears on its own is acceptable and only one that stays is a leak.
const LOCK_WAIT_TIMEOUT_MS = 15_000;
const LOCK_WAIT_DELAY_MS = 50;

/**
 * Runs one non-transactional write and repeats it while a transaction holds the lock of its item. A
 * write to a locked item is REFUSED and not delayed, so the caller waits here. It reports whether it
 * had to wait, and it raises the refusal when the lock stays.
 */
async function untilUnlocked<T>(fn: () => Promise<T>): Promise<{ value: T; waited: boolean }> {
	const deadlineMs = Date.now() + LOCK_WAIT_TIMEOUT_MS;
	for (let attempt = 0; ; attempt++) {
		try {
			return { value: await untilAvailable(fn), waited: attempt > 0 };
		} catch (e) {
			if (!FokosError.isCode(e, CONFLICT_CODES.item_locked_by_transaction) || Date.now() > deadlineMs) throw e;
			await sleep(LOCK_WAIT_DELAY_MS);
		}
	}
}

// One pre-image kind per pool key, so a run starts with a text, a bytes, an object and an array item
// and an update meets every applicability rule from its first command on. Without it a run begins on
// an empty table and nearly every update lands on an absent key.
const SEED_DATA: readonly ItemData[] = [
	"seed-text",
	new Uint8Array([0x01, 0x02]),
	{ alpha: 1, beta: "two" },
	[1, "two", true],
	{ gamma: null },
];

/**
 * Puts the pool in a known state and records it in the model: one item for every key but the last,
 * which stays absent so a run still covers the paths that create an item.
 *
 * The pool can already hold these keys, because a suite whose runs share one table meets them again
 * when fast-check shrinks a failure and runs the shrunk value over the same keys. The seed therefore
 * deletes the last key instead of assuming it absent, and takes the version each write reports
 * instead of the first version an empty pool would give. A repeated run then starts from the same
 * state as the first one, and the counterexample it prints is the real one.
 */
export async function seedPool(db: FokosDB, m: Model, keys: readonly ItemKey[], seedData: readonly ItemData[] = SEED_DATA): Promise<void> {
	for (const [index, key] of keys.slice(0, -1).entries()) {
		const data = seedData[index % seedData.length];
		const { value } = await untilUnlocked(() => db.putItem({ ...key, data }));
		m.items.set(keyId(key), { data, kind: expectedDataKind(data), version: value.version });
	}
	const last = keys[keys.length - 1];
	await untilUnlocked(() => db.deleteItem(last));
	m.items.delete(keyId(last));
}

/** Reads every key of the pool in one transaction and compares each answer with the model. */
export async function expectModelMatches(db: FokosDB, m: Model, keys: readonly ItemKey[]): Promise<void> {
	const res = await untilAvailable(() => db.transactGetItems({ items: [...keys] }));
	keys.forEach((key, i) => expectRead(m, key, res.items[i]));
}

// One action of an update, in the model's own vocabulary. The target is always a top-level field and
// the value is always a literal, so the model can apply the action to its own copy of the document.
export type ModelUpdateAction = { action: "set"; field: string; value: JsonPrimitive } | { action: "remove"; field: string };

// One operation of a write transaction, in the model's own vocabulary. `expectExists` is the only
// condition the model can evaluate: it is required for a check and optional for the others.
export type TxOp = { key: ItemKey; expectExists?: boolean } & (
	| { operation: "put"; data: ItemData }
	| { operation: "delete" }
	| { operation: "check"; expectExists: boolean }
	| { operation: "update"; actions: ModelUpdateAction[] }
);

/**
 * The document an update leaves behind, or `null` when the update does not apply. An absent item has
 * the empty document as its pre-image, so an update creates it. Three rules decide the rest: a text
 * or bytes pre-image is not a document; a `set` of a top-level field needs an object parent, which an
 * array does not give; and a `remove` of a field an array cannot hold is a no-op.
 */
function updatedDocument(item: ModelItem | undefined, actions: readonly ModelUpdateAction[]): JsonComposite | null {
	if (item !== undefined && item.kind !== "json") return null;
	const preImage = (item?.data ?? {}) as JsonComposite;
	if (Array.isArray(preImage)) return actions.some((a) => a.action === "set") ? null : preImage;

	const document: { [field: string]: JsonValue } = { ...(preImage as { [field: string]: JsonValue }) };
	for (const action of actions) {
		if (action.action === "set") document[action.field] = action.value;
		else delete document[action.field];
	}
	return document;
}

/** The one place that applies an update to the model. An update always stores a json document. */
export function applyUpdate(m: Model, key: ItemKey, actions: readonly ModelUpdateAction[]): void {
	const id = keyId(key);
	const document = updatedDocument(m.items.get(id), actions);
	if (document === null) throw new Error("an update that does not apply must not commit");
	m.items.set(id, { data: document, kind: "json", version: (m.items.get(id)?.version ?? 0) + 1 });
}

/** Applies the operations of a committed transaction to the model. */
export function applyTxOps(m: Model, ops: readonly TxOp[]): void {
	for (const op of ops) {
		if (op.operation === "put") applyPut(m, op.key, op.data);
		else if (op.operation === "delete") m.items.delete(keyId(op.key));
		else if (op.operation === "update") applyUpdate(m, op.key, op.actions);
	}
}

/** The operations of a transaction as one line, for the name of a command and for a failure message. */
export function describeTxOps(ops: readonly TxOp[]): string {
	return ops
		.map((op) => {
			const condition = op.expectExists === undefined ? "" : op.expectExists ? "?exists" : "?absent";
			// The actions decide the document, so a counterexample must print them.
			const actions =
				op.operation === "update"
					? ` [${op.actions.map((a) => (a.action === "set" ? `set ${a.field}=${JSON.stringify(a.value)}` : `remove ${a.field}`)).join(", ")}]`
					: "";
			return `${op.operation}${condition}(${keyId(op.key)})${actions}`;
		})
		.join(", ");
}

function existsCondition(expectExists: boolean): ConditionExpression {
	return { op: expectExists ? "exists" : "not_exists", args: [{ ref: "hashKey" }] };
}

export function toTransactWriteItem(op: TxOp): TransactWriteItem {
	const condition = op.expectExists === undefined ? undefined : existsCondition(op.expectExists);
	if (op.operation === "put") return { operation: "put", ...op.key, data: op.data, condition };
	if (op.operation === "delete") return { operation: "delete", ...op.key, condition };
	if (op.operation === "update") {
		const update: UpdateExpression = op.actions.map((action) =>
			action.action === "set"
				? { action: "set", target: { ref: "data", path: `$.${action.field}` }, value: { val: action.value } }
				: { action: "remove", target: { ref: "data", path: `$.${action.field}` } },
		);
		return { operation: "update", ...op.key, update, condition };
	}
	return { operation: "check", ...op.key, condition: existsCondition(op.expectExists) };
}

// A cancel with no failing condition is acceptable only for these reasons. A transaction in the
// same millisecond as the last write of an item is a `timestamp_conflict`; the 503 codes come from
// a partition that is mid-split when a mixed cancel also carries a caller-side reason.
const ACCEPTED_CANCEL_CODES: ReadonlySet<string> = new Set(["timestamp_conflict", ...Object.keys(UNAVAILABLE_CODES)]);

// The rejection codes that say a premise the MODEL evaluates did not hold. Every other code comes
// from the ordering or the availability of the partition, which the model does not predict.
const PREMISE_CODES: ReadonlySet<string> = new Set(["condition_failed", "update_not_applicable"]);

const premiseCode = (r: TransactWriteOperationResult): string | undefined =>
	r.outcome === "rejected" && PREMISE_CODES.has(r.reason.code) ? r.reason.code : undefined;

/**
 * The codes an operation must be rejected with, or `null` when the model knows of no failing premise.
 * Both premises can fail on one operation, and the participant reports the first one it evaluates, so
 * the answer is the set of acceptable codes and not one code.
 */
export function expectedRejection(m: Model, op: TxOp): string[] | null {
	const codes: string[] = [];
	if (op.expectExists !== undefined && op.expectExists !== m.items.has(keyId(op.key))) codes.push("condition_failed");
	if (op.operation === "update" && updatedDocument(m.items.get(keyId(op.key)), op.actions) === null) {
		codes.push("update_not_applicable");
	}
	return codes.length === 0 ? null : codes;
}

abstract class ModelCommand implements fc.AsyncCommand<Model, FokosDB> {
	check(): boolean {
		return true;
	}
	abstract run(m: Model, db: FokosDB): Promise<void>;
}

export class PutItem extends ModelCommand {
	constructor(
		readonly key: ItemKey,
		readonly data: ItemData,
	) {
		super();
	}
	async run(m: Model, db: FokosDB): Promise<void> {
		const res = await untilAvailable(() => db.putItem({ ...this.key, data: this.data }));
		expect(res).toMatchObject({ item: this.key, version: applyPut(m, this.key, this.data) });
	}
	toString(): string {
		return `PutItem(${keyId(this.key)})`;
	}
}

export class GetItem extends ModelCommand {
	constructor(readonly key: ItemKey) {
		super();
	}
	async run(m: Model, db: FokosDB): Promise<void> {
		const res = await untilAvailable(() => db.getItem(this.key));
		expectRead(m, this.key, { ...res.item, found: res.found });
	}
	toString(): string {
		return `GetItem(${keyId(this.key)})`;
	}
}

export class DeleteItem extends ModelCommand {
	constructor(readonly key: ItemKey) {
		super();
	}
	async run(m: Model, db: FokosDB): Promise<void> {
		const id = keyId(this.key);
		const res = await untilAvailable(() => db.deleteItem(this.key));
		expect(res).toMatchObject({ item: this.key, deleted: m.items.has(id) });
		m.items.delete(id);
	}
	toString(): string {
		return `DeleteItem(${keyId(this.key)})`;
	}
}

export class TransactGet extends ModelCommand {
	constructor(readonly keys: ItemKey[]) {
		super();
	}
	async run(m: Model, db: FokosDB): Promise<void> {
		const res = await untilAvailable(() => db.transactGetItems({ items: this.keys }));
		expect(res.items).toHaveLength(this.keys.length);
		this.keys.forEach((key, i) => expectRead(m, key, res.items[i]));
	}
	toString(): string {
		return `TransactGet(${this.keys.map(keyId).join(", ")})`;
	}
}

export class TransactWrite extends ModelCommand {
	constructor(
		readonly ops: TxOp[],
		readonly stats?: TransactionStats,
	) {
		super();
	}
	async run(m: Model, db: FokosDB): Promise<void> {
		// A transaction orders itself with a millisecond timestamp against the last write of every
		// item it touches, so the run first lets the clock move to keep commits common.
		await sleep(2);

		const rejections = this.ops.map((op) => expectedRejection(m, op));
		// `results` is undefined on a commit and holds the positional answers on a cancel.
		const results = await recordTransaction(this.stats, () =>
			untilAvailable(() => db.transactWriteItems({ items: this.ops.map(toTransactWriteItem) })).then(
				() => undefined,
				(e: unknown) => {
					if (FokosTransactionCancelledError.is(e)) return e.results;
					throw e;
				},
			),
		);

		if (rejections.some((codes) => codes !== null)) {
			// The model knows a premise failed, so the transaction must cancel and name that operation.
			// Every participant answers its own operations in parallel, so no failing one stays
			// `not_evaluated`. The model does not change.
			expect(results, "a failing premise must cancel the transaction").toBeDefined();
			if (results === undefined) return;
			expect(results).toHaveLength(this.ops.length);
			rejections.forEach((codes, i) => {
				// An operation whose premises hold must not be blamed for one: it may still be rejected
				// for an ordering or availability reason, which is another code.
				if (codes === null) expect(premiseCode(results[i])).toBeUndefined();
				else expect(codes).toContain(premiseCode(results[i]));
			});
			return;
		}

		if (results !== undefined) {
			// Every premise held, so only an ordering or availability reason may cancel, and
			// atomicity says the model does not change.
			for (const r of results) if (r.outcome === "rejected") expect(ACCEPTED_CANCEL_CODES).toContain(r.reason.code);
			return;
		}

		applyTxOps(m, this.ops);
	}
	toString(): string {
		return `TransactWrite(${describeTxOps(this.ops)})`;
	}
}

// A random condition holds half of the time, and one failing operation cancels the whole set. The
// optional condition is rare and the check operation is light, so most sets commit and the run still
// sees enough cancels.
export const arbOptionalCondition = fc.oneof({ arbitrary: fc.constant(undefined), weight: 4 }, { arbitrary: fc.boolean(), weight: 1 });

// Four field names over the whole pool, so an update of one key often targets a field another update
// wrote or removed. Two actions of one update must not name one field, because their order would then
// decide the document and the model does not know it.
const arbField = fc.constantFrom("alpha", "beta", "gamma", "delta");
const arbFieldValue: fc.Arbitrary<JsonPrimitive> = fc.oneof(fc.string({ maxLength: 8 }), fc.integer(), fc.boolean(), fc.constant(null));
const arbUpdateAction: fc.Arbitrary<ModelUpdateAction> = fc.oneof(
	{ arbitrary: fc.tuple(arbField, arbFieldValue).map(([field, value]) => ({ action: "set" as const, field, value })), weight: 3 },
	{ arbitrary: arbField.map((field) => ({ action: "remove" as const, field })), weight: 1 },
);
export const arbUpdateActions = fc.uniqueArray(arbUpdateAction, { minLength: 1, maxLength: 3, selector: (action) => action.field });

/** One operation of a write transaction, over a key that `keys` draws and a payload that `data` draws. */
export function txOpArbitrary(keys: fc.Arbitrary<ItemKey>, data: fc.Arbitrary<ItemData>): fc.Arbitrary<TxOp> {
	return fc
		.tuple(
			keys,
			fc.oneof(
				{ arbitrary: fc.record({ operation: fc.constant("put" as const), data, expectExists: arbOptionalCondition }), weight: 3 },
				{ arbitrary: fc.record({ operation: fc.constant("delete" as const), expectExists: arbOptionalCondition }), weight: 2 },
				{
					arbitrary: fc.record({
						operation: fc.constant("update" as const),
						actions: arbUpdateActions,
						expectExists: arbOptionalCondition,
					}),
					weight: 3,
				},
				{ arbitrary: fc.record({ operation: fc.constant("check" as const), expectExists: fc.boolean() }), weight: 1 },
			),
		)
		.map(([key, spec]) => ({ key, ...spec }));
}

/**
 * The command arbitraries of a stateful run over `keys`. The key arbitrary must draw from a small
 * pool, so commands hit the same keys again; `data` is the payload of every put.
 */
export function commandArbitraries(
	keys: fc.Arbitrary<ItemKey>,
	data: fc.Arbitrary<ItemData>,
): fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>[] {
	const arbTxOp = txOpArbitrary(keys, data);
	// A transaction rejects two operations on one key, so the keys of a set are unique.
	const arbTxOps = fc.uniqueArray(arbTxOp, { minLength: 1, maxLength: 4, selector: (op) => keyId(op.key) });
	const arbReadKeys = fc.uniqueArray(keys, { minLength: 1, maxLength: 4, selector: keyId });

	return [
		fc.tuple(keys, data).map(([key, d]) => new PutItem(key, d)),
		keys.map((key) => new GetItem(key)),
		keys.map((key) => new DeleteItem(key)),
		arbTxOps.map((ops) => new TransactWrite(ops)),
		arbReadKeys.map((ks) => new TransactGet(ks)),
	];
}

// ─── Concurrent transactions ──────────────────────────────────────────────────

/**
 * What a run observed over the transactions it sent. Under contention a suite where every
 * transaction cancels passes every assertion of the model and proves nothing, so a suite asserts
 * on these counts once its property has run.
 */
export type TransactionStats = {
	started: number;
	committed: number;
	cancelled: number;
	/** The largest number of transactions that were in flight at one time. */
	peakInFlight: number;
	/** How many are in flight at this moment. `peakInFlight` is the one a suite asserts on. */
	inFlight: number;
	/** How many rejected operation entries carried each code. */
	rejections: Map<string, number>;
	/** Probes that found a key unlocked after a batch had drained. */
	lockProbes: number;
	/** Lock probes that met a lock and had to wait for it to clear. */
	lockProbeWaits: number;
};

export function newTransactionStats(): TransactionStats {
	return { started: 0, committed: 0, cancelled: 0, peakInFlight: 0, inFlight: 0, rejections: new Map(), lockProbes: 0, lockProbeWaits: 0 };
}

/** One line of the counts, for the message of a coverage assertion. */
export function describeTransactionStats(s: TransactionStats): string {
	const rejections = [...s.rejections].map(([code, n]) => `${code}=${n}`).join(" ") || "none";
	return [
		`${s.started} transactions: ${s.committed} committed, ${s.cancelled} cancelled`,
		`${s.peakInFlight} at most in flight`,
		`${s.lockProbes} lock probes, ${s.lockProbeWaits} of them waited`,
		`rejections: ${rejections}`,
	].join("; ");
}

/** Runs one transaction and counts how many ran at one time and how each one ended. */
export async function recordTransaction(
	stats: TransactionStats | undefined,
	send: () => Promise<TransactWriteOperationResult[] | undefined>,
): Promise<TransactWriteOperationResult[] | undefined> {
	if (stats === undefined) return await send();
	stats.started++;
	stats.inFlight++;
	stats.peakInFlight = Math.max(stats.peakInFlight, stats.inFlight);
	try {
		const results = await send();
		if (results === undefined) stats.committed++;
		else {
			stats.cancelled++;
			for (const r of results) {
				if (r.outcome === "rejected") stats.rejections.set(r.reason.code, (stats.rejections.get(r.reason.code) ?? 0) + 1);
			}
		}
		return results;
	} finally {
		stats.inFlight--;
	}
}

/**
 * Proves that a key a drained batch touched carries no lock. A non-transactional write to a locked
 * item is REFUSED, so a write that lands is the evidence. A present key is written with the data it
 * already holds and an absent key is deleted again, so the probe keeps the kind and the existence
 * that the model expects and only the version moves.
 */
export async function expectKeyUnlocked(m: Model, db: FokosDB, key: ItemKey, stats: TransactionStats): Promise<void> {
	const item = m.items.get(keyId(key));
	const { value, waited } = await untilUnlocked<PutItemResult | DeleteItemResult>(() =>
		item === undefined ? db.deleteItem(key) : db.putItem({ ...key, data: item.data }),
	);
	if (item === undefined) expect(value).toMatchObject({ item: key, deleted: false });
	else expect(value).toMatchObject({ item: key, version: applyPut(m, key, item.data) });
	stats.lockProbes++;
	if (waited) stats.lockProbeWaits++;
}

/**
 * A batch of transactions that the run sends at one time. Their key sets are disjoint, so no two of
 * them meet on one item and the model still predicts every outcome exactly, while many coordinators
 * drive one table and one partition serves several transactions at once.
 *
 * Once the batch has drained, every key it touched must take a non-transactional write, which
 * proves that the batch left no lock behind.
 */
export class ConcurrentTransactWrites extends ModelCommand {
	constructor(
		readonly transactions: TransactWrite[],
		readonly stats: TransactionStats,
	) {
		super();
	}

	async run(m: Model, db: FokosDB): Promise<void> {
		// Every transaction is awaited before the first failure is raised, so a failing batch never
		// leaves the next command with a request of this one still in flight.
		const settled = await Promise.allSettled(this.transactions.map((tx) => tx.run(m, db)));
		for (const outcome of settled) if (outcome.status === "rejected") throw outcome.reason;

		for (const tx of this.transactions) {
			for (const op of tx.ops) await expectKeyUnlocked(m, db, op.key, this.stats);
		}
	}

	toString(): string {
		return `Concurrent(${this.transactions.map((tx) => tx.toString()).join(" || ")})`;
	}
}

const MAX_CONCURRENT_TRANSACTIONS = 4;

/**
 * A batch of 2 to `MAX_CONCURRENT_TRANSACTIONS` transactions over disjoint key sets. The keys come
 * from the pool in a random order and are dealt round-robin, so every transaction of the batch holds
 * at least one key and no key reaches two of them.
 */
export function arbConcurrentBatch(
	keys: readonly ItemKey[],
	data: fc.Arbitrary<ItemData>,
	stats: TransactionStats,
): fc.Arbitrary<ConcurrentTransactWrites> {
	return fc
		.shuffledSubarray([...keys], { minLength: 2 })
		.chain((pool) =>
			fc.record({
				ops: fc.tuple(...pool.map((key) => txOpArbitrary(fc.constant(key), data))),
				count: fc.integer({ min: 2, max: Math.min(MAX_CONCURRENT_TRANSACTIONS, pool.length) }),
			}),
		)
		.map(({ ops, count }) => {
			const groups: TxOp[][] = Array.from({ length: count }, () => []);
			ops.forEach((op, i) => groups[i % count].push(op));
			return new ConcurrentTransactWrites(
				groups.map((groupOps) => new TransactWrite(groupOps, stats)),
				stats,
			);
		});
}
