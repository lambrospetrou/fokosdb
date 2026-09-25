// The harness that every property-based suite of this directory uses.
//
// A property-based suite draws random input, runs it against real Durable Objects, and compares
// what the database answers with what a model of the database says. fast-check then shrinks a
// failing input to a small counterexample. Every arbitrary here produces input that the public API
// accepts, so a failure is a library defect and never a validation error.
//
// What the file holds, in the order it holds it:
//
//   1. The size of a run, the test table, and the arbitraries of the keys and the payloads.
//   2. The waits. Three conditions of the database clear on their own, and a client tries again.
//   3. The model: a map from `keyId` to the item the database must hold.
//   4. The operations of a write transaction, in the vocabulary of the model.
//   5. The counters that prove a run met the states it exists for.
//   6. The key pool: the state a run starts from, and the reads that compare it.
//   7. The commands, their arbitraries, and the run that drives them.
//   8. A batch of transactions over disjoint keys, where the model predicts every outcome.
//   9. A batch of transactions over the same keys, where the oracle is serializability.
//  10. The table whose partitions split while a suite writes to it.
//
// query-harness.ts holds the second model, for `queryItems`: a key order oracle, the pages of a
// request, and the range tree the split suites read.
//
// A failure prints `seed` and `path`. Put them in the `fc.assert` parameters, for example
// `{ seed: 42, path: "3:1:0" }`, to replay the shrunk counterexample. A command sequence also
// prints `replayPath`; pass it to `fc.commands` as `{ replayPath: "..." }` next to the seed.
// `FOKOS_PROPERTY_RUNS=500 pnpm vitest run test/property-based/` searches deeper than the default.
import fc from "fast-check";
import { expect } from "vitest";
import { FokosDB } from "../../src/client/db.js";
import {
	CONFLICT_CODES,
	FokosError,
	FokosTransactionPendingError,
	FokosUnavailableError,
	UNAVAILABLE_CODES,
} from "../../src/shared/errors.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import type { ConditionExpression, UpdateExpression } from "../../src/shared/expression/types.js";
import type { JsonComposite, JsonPrimitive, JsonValue } from "../../src/shared/json-types.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { PartitionContextCreator } from "../../src/shared/partition-context.js";
import { FokosRouter } from "../../src/sharding/router.js";
import { MAX_HASH_KEY_BYTES, MAX_SORT_KEY_BYTES } from "../../src/shared/transaction-limits.js";
import type { MaybeReadItem, TransactWriteItem, TransactWriteOperationResult } from "../../src/shared/transaction-api-types.js";
import type { DeleteItemResult, PutItemResult } from "../../src/shared/types.js";

// ─── The size of a run ────────────────────────────────────────────────────────

// A suite runs inside the Workers runtime and cannot read the shell environment, so
// `vitest.config.ts` substitutes FOKOS_PROPERTY_RUNS into this constant when it builds the module.
declare const __FOKOS_PROPERTY_RUNS__: string;

/**
 * The run count of one property: FOKOS_PROPERTY_RUNS when the shell sets it, else `defaultRuns`.
 * Keep the default small enough for every run of the test suite, and give a deeper search the
 * variable.
 */
export function propertyRuns(defaultRuns: number): number {
	const configured = __FOKOS_PROPERTY_RUNS__;
	if (configured === "") {
		return defaultRuns;
	}
	const runs = Number(configured);
	if (!Number.isSafeInteger(runs) || runs <= 0) {
		throw new Error(`FOKOS_PROPERTY_RUNS must be a positive integer, got ${JSON.stringify(configured)}`);
	}
	return runs;
}

export const textEncoder = new TextEncoder();
export const textDecoder = new TextDecoder();

// ─── The table ────────────────────────────────────────────────────────────────

/**
 * A fresh table on three root partitions, so random hash keys spread over the partition DOs. The
 * default split thresholds are far above what a run writes, so no partition splits. A suite that
 * wants hash splits passes a small `hashSplitMaxSizeMb`, and one that wants a key promoted into a
 * range tree passes a small `rangeSplitMaxSizeMb` as well.
 */
export function makeTestDB(opts?: { hashSplitMaxSizeMb?: number; rangeSplitMaxSizeMb?: number }): FokosDB {
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: `test.pbt.${crypto.randomUUID()}`,
		rootTreesN: 3,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: opts?.hashSplitMaxSizeMb ?? 500 },
		rangeSplitConditions: { maxSizeMb: opts?.rangeSplitMaxSizeMb ?? 500 },
	});
	return new FokosDB({ topology: new FokosRouter(base.topology, base.rangeConfig, base.policy) });
}

// ─── The keys and the payloads ────────────────────────────────────────────────

export type ItemKey = { hashKey: string | Uint8Array; sortKey?: string | Uint8Array };
export type ItemData = string | Uint8Array | JsonComposite;
export type DataKind = "text" | "bytes" | "json";

/** The stable identity of a key pair, usable as a `Map` key. A `Uint8Array` compares by reference. */
export function keyId(key: ItemKey): string {
	return `${KeyCodec.encode(key.hashKey).toHex()}:${KeyCodec.encodeOptional(key.sortKey).toHex()}`;
}

/** The `kind` that a read returns for the data a write accepted. */
export function expectedDataKind(data: ItemData): DataKind {
	if (data instanceof Uint8Array) {
		return "bytes";
	}
	if (typeof data === "string") {
		return "text";
	}
	return "json";
}

// `unit: "binary"` draws from the whole code point range, so the filter below does real work: it
// rejects NUL, which a string key must not hold, and it holds the encoded size under the key limit.
function arbStringKey(maxBytes: number): fc.Arbitrary<string> {
	return fc
		.string({ minLength: 1, maxLength: 32, unit: "binary" })
		.filter((s) => !s.includes("\0") && KeyCodec.encode(s).byteLength <= maxBytes);
}

// A binary key gets a one-byte tag when the codec encodes it, so its raw length stays one under the limit.
function arbBinaryKey(maxBytes: number): fc.Arbitrary<Uint8Array> {
	return fc.uint8Array({ minLength: 1, maxLength: Math.min(64, maxBytes - 1) });
}

const arbHashKey = fc.oneof(arbStringKey(MAX_HASH_KEY_BYTES), arbBinaryKey(MAX_HASH_KEY_BYTES));
const arbSortKey = fc.oneof(arbStringKey(MAX_SORT_KEY_BYTES), arbBinaryKey(MAX_SORT_KEY_BYTES), fc.constant(undefined));
export const arbItemKey: fc.Arbitrary<ItemKey> = fc.record({ hashKey: arbHashKey, sortKey: arbSortKey });

/** `hashKey` with `prefix` in front, so a run on a shared table writes in a key space of its own. */
export function prefixHashKey(prefix: string, hashKey: string | Uint8Array): string | Uint8Array {
	if (typeof hashKey === "string") {
		return `${prefix}:${hashKey}`;
	}
	const head = textEncoder.encode(`${prefix}:`);
	const out = new Uint8Array(head.length + hashKey.byteLength);
	out.set(head);
	out.set(hashKey, head.length);
	return out;
}

const arbJsonLeaf: fc.Arbitrary<JsonPrimitive> = fc.oneof(fc.string({ maxLength: 32 }), fc.integer(), fc.boolean(), fc.constant(null));

// A stored json value comes back through JSON.parse. The same round trip over the generated value
// gives the model the exact object a read returns: a plain prototype, and no `-0`.
const jsonRoundTrip = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

export const arbJsonData: fc.Arbitrary<JsonComposite> = fc.oneof(
	fc
		.dictionary(fc.string({ minLength: 1, maxLength: 8 }), arbJsonLeaf, { maxKeys: 4 })
		.map((o) => jsonRoundTrip(o as { [k: string]: JsonValue })),
	fc.array(arbJsonLeaf, { maxLength: 4 }).map((a) => jsonRoundTrip(a as JsonValue[])),
);

export const arbItemData: fc.Arbitrary<ItemData> = fc.oneof(
	fc.string({ maxLength: 256, unit: "binary" }),
	fc.uint8Array({ maxLength: 256 }),
	arbJsonData,
);

/**
 * The key pool with `prefix` in front of every hash key. It mixes string and binary keys and a
 * hash-key-only item. A stateful run needs repeated hits on the same keys, and random keys almost
 * never collide, so the commands draw from this small pool. A suite whose runs share one table
 * gives each run its own prefix, so the runs never see the items of each other.
 */
function poolKeys(prefix: string): ItemKey[] {
	const bin = (...bytes: number[]) => new Uint8Array([...textEncoder.encode(prefix), ...bytes]);
	return [
		{ hashKey: `${prefix}user:1`, sortKey: "profile" },
		{ hashKey: `${prefix}user:1`, sortKey: "settings" },
		{ hashKey: `${prefix}user:2` },
		{ hashKey: bin(0x01, 0x02, 0x03), sortKey: new Uint8Array([0x0a, 0x0b]) },
		{ hashKey: bin(0xff, 0xfe) },
	];
}

export const POOL_KEYS: readonly ItemKey[] = poolKeys("");
export const arbPoolKey: fc.Arbitrary<ItemKey> = fc.constantFrom(...POOL_KEYS);

// ─── The waits ────────────────────────────────────────────────────────────────

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// One try every 50 ms, and 600 tries, so a wait lasts 30 seconds at most.
const RETRY_DELAY_MS = 50;
const RETRY_LIMIT = 600;

/**
 * Runs `fn`, and repeats it while `clears` accepts the error. `onRetry` counts the repeats for a
 * caller that reports them. The error of the last try leaves this function.
 */
async function retryWhile<T>(fn: () => Promise<T>, clears: (e: unknown) => boolean, onRetry?: () => void): Promise<T> {
	for (let attempt = 0; ; attempt++) {
		try {
			return await fn();
		} catch (e) {
			if (!clears(e) || attempt >= RETRY_LIMIT) {
				throw e;
			}
			onRetry?.();
			await sleep(RETRY_DELAY_MS);
		}
	}
}

/**
 * True when a partition cannot serve the call now. A partition that splits, and a child that
 * imports its share of a parent, answer 503: `partition_migrating` or `partition_over_size`. A
 * write transaction whose every failure is such a 503 cancels with `origin: "service"`.
 */
const isUnavailable = (e: unknown): boolean =>
	FokosUnavailableError.is(e) || (FokosTransactionCancelledError.is(e) && e.origin === "service");

/** Runs one operation and repeats it while a partition cannot serve it. Every other error fails the run. */
export const untilAvailable = <T>(fn: () => Promise<T>): Promise<T> => retryWhile(fn, isUnavailable);

/** True when a transaction holds the lock of the item. */
const isItemLocked = (e: unknown): boolean => FokosError.isCode(e, CONFLICT_CODES.item_locked_by_transaction);

/**
 * Runs one write that no transaction drives, and repeats it while a transaction holds the lock of
 * the item. The database REFUSES such a write and never delays it, so the caller waits here.
 *
 * A cancelled transaction answers its caller as soon as the coordinator decides, and the last
 * participants of the fan-out can still hold their locks, which an alarm of the coordinator then
 * clears. A lock that clears on its own is therefore acceptable, and only a lock that stays is a
 * leak. `waited` reports whether the write met a lock.
 */
async function untilUnlocked<T>(fn: () => Promise<T>): Promise<{ value: T; waited: boolean }> {
	let waited = false;
	const value = await retryWhile(
		() => untilAvailable(fn),
		isItemLocked,
		() => {
			waited = true;
		},
	);
	return { value, waited };
}

// ─── The model ────────────────────────────────────────────────────────────────

export type ModelItem = { data: ItemData; kind: DataKind; version: number };
export type Model = { items: Map<string, ModelItem> };

/** Applies the put of a command to the model. A new item starts at version 1, an overwrite adds one. */
function applyPut(m: Model, key: ItemKey, data: ItemData): number {
	const id = keyId(key);
	const version = (m.items.get(id)?.version ?? 0) + 1;
	m.items.set(id, { data, kind: expectedDataKind(data), version });
	return version;
}

/**
 * The document an update leaves behind, or `null` when the update does not apply. An absent item has
 * the empty document as its pre-image, so an update creates it. Three rules decide the rest: a text
 * or bytes pre-image is not a document; a `set` of a top-level field needs an object parent, which
 * an array does not give; and a `remove` of a field an array cannot hold changes nothing.
 */
function updatedDocument(item: ModelItem | undefined, actions: readonly ModelUpdateAction[]): JsonComposite | null {
	if (item !== undefined && item.kind !== "json") {
		return null;
	}
	const preImage = (item?.data ?? {}) as JsonComposite;
	if (Array.isArray(preImage)) {
		return actions.some((a) => a.action === "set") ? null : preImage;
	}

	const document: { [field: string]: JsonValue } = { ...(preImage as { [field: string]: JsonValue }) };
	for (const action of actions) {
		if (action.action === "set") {
			document[action.field] = action.value;
		} else {
			delete document[action.field];
		}
	}
	return document;
}

/** Applies the update of a command to the model. An update always stores a json document. */
function applyUpdate(m: Model, key: ItemKey, actions: readonly ModelUpdateAction[]): void {
	const id = keyId(key);
	const document = updatedDocument(m.items.get(id), actions);
	if (document === null) {
		throw new Error("an update that does not apply must not commit");
	}
	m.items.set(id, { data: document, kind: "json", version: (m.items.get(id)?.version ?? 0) + 1 });
}

/** Compares one read answer (a `getItem` result or one `transactGetItems` entry) with the model. */
function expectRead(m: Model, key: ItemKey, res: unknown): void {
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

// ─── The operations of a write transaction ────────────────────────────────────

// One action of an update, in the vocabulary of the model. The target is always a top-level field
// and the value is always a literal, so the model applies the action to its own copy of the document.
export type ModelUpdateAction = { action: "set"; field: string; value: JsonPrimitive } | { action: "remove"; field: string };

// One operation of a write transaction, in the vocabulary of the model. `expectExists` is the only
// condition the model evaluates: a check needs it, and the other operations take it or leave it out.
export type TxOp = { key: ItemKey; expectExists?: boolean } & (
	| { operation: "put"; data: ItemData }
	| { operation: "delete" }
	| { operation: "check"; expectExists: boolean }
	| { operation: "update"; actions: ModelUpdateAction[] }
);

/** Applies the operations of a committed transaction to the model. */
function applyTxOps(m: Model, ops: readonly TxOp[]): void {
	for (const op of ops) {
		if (op.operation === "put") {
			applyPut(m, op.key, op.data);
		} else if (op.operation === "delete") {
			m.items.delete(keyId(op.key));
		} else if (op.operation === "update") {
			applyUpdate(m, op.key, op.actions);
		}
	}
}

/** The operations of a transaction as one line, for the name of a command and for a failure message. */
function describeTxOps(ops: readonly TxOp[]): string {
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

/** Translates one operation of the model into the request that the public API takes. */
function toTransactWriteItem(op: TxOp): TransactWriteItem {
	const condition = op.expectExists === undefined ? undefined : existsCondition(op.expectExists);
	if (op.operation === "put") {
		return { operation: "put", ...op.key, data: op.data, condition };
	}
	if (op.operation === "delete") {
		return { operation: "delete", ...op.key, condition };
	}
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

// The rejection codes that say a premise the MODEL evaluates did not hold. Every other code comes
// from the order or the availability of the partition, which the model does not predict.
const PREMISE_CODES: ReadonlySet<string> = new Set(["condition_failed", "update_not_applicable"]);

const premiseCode = (r: TransactWriteOperationResult): string | undefined =>
	r.outcome === "rejected" && PREMISE_CODES.has(r.reason.code) ? r.reason.code : undefined;

/**
 * The codes the participant must reject an operation with, or `null` when the model knows of no
 * failing premise. Both premises can fail on one operation, and the participant reports the first
 * one it evaluates, so the answer is a set of codes and not one code.
 */
function expectedRejection(m: Model, op: TxOp): string[] | null {
	const codes: string[] = [];
	if (op.expectExists !== undefined && op.expectExists !== m.items.has(keyId(op.key))) {
		codes.push("condition_failed");
	}
	if (op.operation === "update" && updatedDocument(m.items.get(keyId(op.key)), op.actions) === null) {
		codes.push("update_not_applicable");
	}
	return codes.length === 0 ? null : codes;
}

/**
 * The codes that cancel a transaction whose premises all hold. A transaction in the same
 * millisecond as the last write of an item gets `timestamp_conflict`, and a partition that splits
 * or imports its share answers 503. A batch over the same keys meets more of them, which
 * `CONTENTION_CANCEL_CODES` lists.
 */
const ORDERING_CANCEL_CODES: ReadonlySet<string> = new Set(["timestamp_conflict", ...Object.keys(UNAVAILABLE_CODES)]);

// ─── The counters of a run ────────────────────────────────────────────────────

/**
 * What a run observed over the transactions it sent. A suite that sends transactions at one time
 * asserts on these counts once its property has run, because a suite where every transaction
 * cancels agrees with the model and proves nothing.
 */
export type TransactionStats = {
	started: number;
	committed: number;
	cancelled: number;
	/** The largest number of transactions that were in flight at one time. */
	peakInFlight: number;
	/** How many are in flight now. A suite asserts on `peakInFlight` instead. */
	inFlight: number;
	/** How many rejected operation entries carried each code. */
	rejections: Map<string, number>;
	/** Probes that found a key unlocked after a batch had drained. */
	lockProbes: number;
	/** Lock probes that met a lock and waited for it to clear. */
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
async function recordTransaction(
	stats: TransactionStats | undefined,
	send: () => Promise<TransactWriteOperationResult[] | undefined>,
): Promise<TransactWriteOperationResult[] | undefined> {
	if (stats === undefined) {
		return await send();
	}
	stats.started++;
	stats.inFlight++;
	stats.peakInFlight = Math.max(stats.peakInFlight, stats.inFlight);
	try {
		const results = await send();
		if (results === undefined) {
			stats.committed++;
		} else {
			stats.cancelled++;
			for (const r of results) {
				if (r.outcome === "rejected") {
					stats.rejections.set(r.reason.code, (stats.rejections.get(r.reason.code) ?? 0) + 1);
				}
			}
		}
		return results;
	} finally {
		stats.inFlight--;
	}
}

// ─── The key pool of a run ────────────────────────────────────────────────────

// One pre-image kind per pool key, so a run starts with a text, a bytes, an object and an array
// item. An update then meets every applicability rule from the first command on. Without the seed a
// run starts on an empty table, and nearly every update lands on an absent key.
const SEED_DATA: readonly ItemData[] = [
	"seed-text",
	new Uint8Array([0x01, 0x02]),
	{ alpha: 1, beta: "two" },
	[1, "two", true],
	{ gamma: null },
];

/**
 * Puts the pool in a known state and records that state in the model: one item for every key but
 * the last, which stays absent so a run still covers the paths that create an item.
 *
 * The pool can already hold these keys. A suite whose runs share one table meets them again when
 * fast-check shrinks a failure and runs the shrunk value over the same keys. The seed therefore
 * deletes the last key instead of an assumption that it is absent, and it takes the version each
 * write reports instead of the first version that an empty pool gives. A repeated run then starts
 * from the same state as the first one, and the counterexample it prints is the real one.
 */
async function seedPool(db: FokosDB, m: Model, keys: readonly ItemKey[], seedData: readonly ItemData[] = SEED_DATA): Promise<void> {
	for (const [index, key] of keys.slice(0, -1).entries()) {
		const data = seedData[index % seedData.length];
		const { value } = await untilUnlocked(() => db.putItem({ ...key, data }));
		m.items.set(keyId(key), { data, kind: expectedDataKind(data), version: value.version });
	}
	const last = keys[keys.length - 1];
	await untilUnlocked(() => db.deleteItem(last));
	m.items.delete(keyId(last));
}

/**
 * Reads every key of the pool in one transaction, and waits out the locks of a batch that has just
 * drained. A read transaction refuses an item that holds the pending write of an in-flight
 * transaction, and it aborts when the committed state moves under it between its two phases. A
 * cancelled transaction answers its caller before its last participants release their locks, so
 * both conditions clear on their own here.
 */
async function readPool(db: FokosDB, keys: readonly ItemKey[]): Promise<MaybeReadItem[]> {
	const isReadBlocked = (e: unknown): boolean =>
		FokosError.isCode(e, CONFLICT_CODES.pending_write) || FokosError.isCode(e, CONFLICT_CODES.read_conflict);
	const res = await retryWhile(() => untilAvailable(() => db.transactGetItems({ items: [...keys] })), isReadBlocked);
	return [...res.items];
}

/** Reads every key of the pool and compares each answer with the model. */
async function expectModelMatches(db: FokosDB, m: Model, keys: readonly ItemKey[]): Promise<void> {
	const items = await readPool(db, keys);
	keys.forEach((key, i) => expectRead(m, key, items[i]));
}

/** The model the pool holds now, built from the answers of `readPool`. */
function poolModel(items: readonly MaybeReadItem[], keys: readonly ItemKey[]): Model {
	const model: Model = { items: new Map() };
	keys.forEach((key, i) => {
		const read = items[i];
		if (read.found) {
			model.items.set(keyId(key), { data: read.data as ItemData, kind: read.kind as DataKind, version: read.version });
		}
	});
	return model;
}

/**
 * Proves that a key a drained batch touched carries no lock. The database REFUSES a write to a
 * locked item, so a write that lands is the evidence. The probe writes a present key with the data
 * it already holds, and it deletes an absent key again. The kind and the existence the model
 * expects therefore stay as they are, and only the version moves.
 */
async function expectKeyUnlocked(m: Model, db: FokosDB, key: ItemKey, stats: TransactionStats): Promise<void> {
	const item = m.items.get(keyId(key));
	const { value, waited } = await untilUnlocked<PutItemResult | DeleteItemResult>(() =>
		item === undefined ? db.deleteItem(key) : db.putItem({ ...key, data: item.data }),
	);
	if (item === undefined) {
		expect(value).toMatchObject({ item: key, deleted: false });
	} else {
		expect(value).toMatchObject({ item: key, version: applyPut(m, key, item.data) });
	}
	stats.lockProbes++;
	if (waited) {
		stats.lockProbeWaits++;
	}
}

// ─── The commands ─────────────────────────────────────────────────────────────

// `check` says whether a command can run in the state the model holds. Every command here runs in
// every state, so a command over an absent key is a real test case and not a skipped one.
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
		// item it touches. The run therefore lets the clock move first, which keeps commits common.
		await sleep(2);

		const rejections = this.ops.map((op) => expectedRejection(m, op));
		// `results` is undefined on a commit and holds the positional answers on a cancel.
		const results = await recordTransaction(this.stats, () =>
			untilAvailable(() => db.transactWriteItems({ items: this.ops.map(toTransactWriteItem) })).then(
				() => undefined,
				(e: unknown) => {
					if (FokosTransactionCancelledError.is(e)) {
						return e.results;
					}
					throw e;
				},
			),
		);

		if (rejections.some((codes) => codes !== null)) {
			// The model knows that a premise failed, so the transaction must cancel and name that
			// operation. Every participant answers its own operations at the same time, so a failing
			// operation never stays `not_evaluated`. The model does not change.
			expect(results, "a failing premise must cancel the transaction").toBeDefined();
			if (results === undefined) {
				return;
			}
			expect(results).toHaveLength(this.ops.length);
			rejections.forEach((codes, i) => {
				// The participant must not blame an operation whose premises hold. It can still reject
				// that operation for an order or an availability reason, which is another code.
				if (codes === null) {
					expect(premiseCode(results[i])).toBeUndefined();
				} else {
					expect(codes).toContain(premiseCode(results[i]));
				}
			});
			return;
		}

		if (results !== undefined) {
			// Every premise held, so only an order or an availability reason cancels the transaction.
			// The transaction is atomic, so the model does not change.
			for (const r of results) {
				if (r.outcome === "rejected") {
					expect(ORDERING_CANCEL_CODES).toContain(r.reason.code);
				}
			}
			return;
		}

		applyTxOps(m, this.ops);
	}
	toString(): string {
		return `TransactWrite(${describeTxOps(this.ops)})`;
	}
}

// ─── The arbitraries of the commands ──────────────────────────────────────────

// A random condition holds half of the time, and one failing operation cancels the whole set. The
// optional condition is rare and the check operation is light, so most sets commit and a run still
// sees enough cancels.
export const arbOptionalCondition = fc.oneof({ arbitrary: fc.constant(undefined), weight: 4 }, { arbitrary: fc.boolean(), weight: 1 });

// Four field names over the whole pool, so an update of one key often targets a field that another
// update wrote or removed. Two actions of one update must not name one field, because their order
// would then decide the document and the model does not know that order.
const arbField = fc.constantFrom("alpha", "beta", "gamma", "delta");
const arbFieldValue: fc.Arbitrary<JsonPrimitive> = fc.oneof(fc.string({ maxLength: 8 }), fc.integer(), fc.boolean(), fc.constant(null));
const arbUpdateAction: fc.Arbitrary<ModelUpdateAction> = fc.oneof(
	{ arbitrary: fc.tuple(arbField, arbFieldValue).map(([field, value]) => ({ action: "set" as const, field, value })), weight: 3 },
	{ arbitrary: arbField.map((field) => ({ action: "remove" as const, field })), weight: 1 },
);
export const arbUpdateActions = fc.uniqueArray(arbUpdateAction, { minLength: 1, maxLength: 3, selector: (action) => action.field });

/** One operation of a write transaction, over a key that `keys` draws and a payload that `data` draws. */
function txOpArbitrary(keys: fc.Arbitrary<ItemKey>, data: fc.Arbitrary<ItemData>): fc.Arbitrary<TxOp> {
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
 * pool, so the commands hit the same keys again. `data` is the payload of every put.
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

// ─── One run of a stateful property ───────────────────────────────────────────

/** The commands of one run, and the key pool they draw their keys from. */
export type Run = { keys: readonly ItemKey[]; cmds: Iterable<fc.AsyncCommand<Model, FokosDB>> };

/**
 * A run over a key pool of its own. `makeCommands` builds the command arbitraries over that pool.
 * The run draws the prefix of its pool, so a replay with the same seed uses the same keys.
 */
export function arbRun(
	makeCommands: (keys: ItemKey[]) => fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>[],
	constraints: fc.CommandsContraints,
): fc.Arbitrary<Run> {
	return fc.uuid().chain((prefix) => {
		const keys = poolKeys(`${prefix}:`);
		return fc.record({ keys: fc.constant(keys), cmds: fc.commands(makeCommands(keys), constraints) });
	});
}

/**
 * Runs one command sequence against the database and the model at the same time. It seeds the pool,
 * runs the commands, and then compares every key of the pool. The last comparison fails a run whose
 * divergence no read command observed.
 */
export async function runCommands(db: FokosDB, run: Run, seedData?: readonly ItemData[]): Promise<void> {
	const model: Model = { items: new Map() };
	await seedPool(db, model, run.keys, seedData);
	await fc.asyncModelRun(() => ({ model, real: db }), run.cmds);
	await expectModelMatches(db, model, run.keys);
}

// ─── A batch of transactions over disjoint keys ───────────────────────────────

/**
 * A batch of transactions that the run sends at one time. Their key sets are disjoint, so no two of
 * them meet on one item and the model still predicts every outcome exactly, while many coordinators
 * drive one table and one partition serves several transactions at once.
 *
 * Once the batch has drained, every key it touched must take a write that no transaction drives,
 * which proves that the batch left no lock behind.
 */
class ConcurrentTransactWrites extends ModelCommand {
	constructor(
		readonly transactions: TransactWrite[],
		readonly stats: TransactionStats,
	) {
		super();
	}

	async run(m: Model, db: FokosDB): Promise<void> {
		// The run awaits every transaction before it raises the first failure, so a failing batch
		// never leaves the next command with a request of this one still in flight.
		const settled = await Promise.allSettled(this.transactions.map((tx) => tx.run(m, db)));
		for (const outcome of settled) {
			if (outcome.status === "rejected") {
				throw outcome.reason;
			}
		}

		for (const tx of this.transactions) {
			for (const op of tx.ops) {
				await expectKeyUnlocked(m, db, op.key, this.stats);
			}
		}
	}

	toString(): string {
		return `Concurrent(${this.transactions.map((tx) => tx.toString()).join(" || ")})`;
	}
}

const MAX_CONCURRENT_TRANSACTIONS = 4;

/**
 * A batch of 2 to `MAX_CONCURRENT_TRANSACTIONS` transactions over disjoint key sets. The keys come
 * from the pool in a random order, and the batch deals them round-robin, so every transaction of
 * the batch holds at least one key and no key reaches two of them.
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

// ─── A batch of transactions over the same keys ───────────────────────────────
//
// The batch above keeps the key sets of its transactions disjoint, so the model predicts every
// outcome. Here they overlap, and no model predicts which transaction wins. The oracle is
// serializability instead: once the batch has drained, the state of the pool must be the state that
// SOME order of the committed transactions leaves behind, and every committed transaction must find
// its premises true where it stands in that order. 2PC holds the lock of an item from the prepare
// to the outcome, so two transactions that meet on an item never commit together, and such an order
// always exists.
//
// A lost update, a loser that applied a part of its operations, and a winner whose condition never
// held all fit no order, so each one fails the run.
//
// Every transaction here carries a `clientRequestToken` of its own. Under contention a commit can
// answer `transaction_commit_pending`, which says the decision is durable and not applied
// everywhere yet. The token names the coordinator, so the same call again resumes that same
// transaction and reads its one outcome. Without a token the repeat starts a second transaction and
// writes twice.

// The reasons a transaction of a contending batch cancels. A premise fails with `condition_failed`
// or `update_not_applicable`; a contender holds the lock of an item (`pending_conflict`) or wrote it
// in the same millisecond (`timestamp_conflict`); and a partition that splits or imports its share
// answers 503. Any other code is a defect.
const CONTENTION_CANCEL_CODES: ReadonlySet<string> = new Set([
	...ORDERING_CANCEL_CODES,
	"condition_failed",
	"update_not_applicable",
	"pending_conflict",
	"clock_skew",
]);

// A json document comes back from SQLite with its fields in an order of its own, and that order
// says nothing about the value, so the canonical form sorts them. Text and bytes never compare
// equal, because the signature below carries the `kind` of the item as well.
function canonicalJson(value: JsonValue): string {
	if (Array.isArray(value)) {
		return `[${value.map(canonicalJson).join(",")}]`;
	}
	if (typeof value === "object" && value !== null) {
		return `{${Object.keys(value)
			.sort()
			.map((field) => `${JSON.stringify(field)}:${canonicalJson(value[field])}`)
			.join(",")}}`;
	}
	return JSON.stringify(value);
}

function canonicalData(data: ItemData): string {
	if (data instanceof Uint8Array) {
		return [...data].map((b) => b.toString(16).padStart(2, "0")).join("");
	}
	if (typeof data === "string") {
		return JSON.stringify(data);
	}
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

function permutations<T>(items: readonly T[]): T[][] {
	if (items.length <= 1) {
		return [[...items]];
	}
	return items.flatMap((item, i) => permutations([...items.slice(0, i), ...items.slice(i + 1)]).map((rest) => [item, ...rest]));
}

/** Applies one transaction to a candidate order, or reports that its premises do not hold there. */
function applyWhenPremisesHold(m: Model, ops: readonly TxOp[]): boolean {
	// Every operation of a transaction reads the state the transaction starts from, and the keys of
	// one transaction are unique, so the participant evaluates the premises before any of them applies.
	if (ops.some((op) => expectedRejection(m, op) !== null)) {
		return false;
	}
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

/**
 * Sends one transaction under `token` until it answers an outcome. `undefined` says that it
 * committed, and an array holds the positional answers of a cancel.
 *
 * A 503 and a commit that is still pending both clear on their own, and the token makes the repeat
 * safe: the coordinator finds the ledger row of the first try and resumes that transaction. A
 * cancel is terminal for the same reason — the coordinator stored it and answers it again — so a
 * cancel is an outcome here and not something to repeat.
 */
async function sendPinned(db: FokosDB, ops: readonly TxOp[], token: string): Promise<TransactWriteOperationResult[] | undefined> {
	const items = ops.map(toTransactWriteItem);
	const send = async (): Promise<TransactWriteOperationResult[] | undefined> => {
		try {
			await db.transactWriteItems({ items, clientRequestToken: token });
			return undefined;
		} catch (e) {
			if (FokosTransactionCancelledError.is(e)) {
				return e.results;
			}
			throw e;
		}
	};
	return await retryWhile(send, (e) => FokosUnavailableError.is(e) || FokosTransactionPendingError.is(e));
}

/** The outcome as one comparable string: a commit, or the answer to each operation in request order. */
function describeOutcome(results: TransactWriteOperationResult[] | undefined): string {
	if (results === undefined) {
		return "committed";
	}
	return `cancelled(${results.map((r) => (r.outcome === "rejected" ? r.reason.code : r.outcome)).join(", ")})`;
}

/** One logical transaction of a contending batch: its operations, and the token that names its coordinator. */
class ContendingTransaction {
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

/**
 * A batch of transactions that meet on the same items, sent at one time. Once the batch has
 * drained, three rules hold: the state of the pool agrees with some order of the transactions that
 * committed; one transaction answers the same outcome when the run sends it again under its token;
 * and every key the batch touched takes a write that no transaction drives, which proves that the
 * batch left no lock behind.
 */
class ContendingTransactWrites implements fc.AsyncCommand<Model, FokosDB> {
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
		// A transaction orders itself with a millisecond timestamp against the last write of every
		// item it touches, so the batch lets the clock move past the command before it.
		await sleep(2);

		const settled = await Promise.allSettled(this.transactions.map((tx) => tx.send(db)));
		// The run awaits every transaction before it raises the first failure, so a failing batch
		// never leaves the next command with a request of this one still in flight.
		for (const outcome of settled) {
			if (outcome.status === "rejected") {
				throw outcome.reason;
			}
		}
		const outcomes = settled.flatMap((outcome) => (outcome.status === "fulfilled" ? [outcome.value] : []));

		for (const results of outcomes) {
			for (const r of results ?? []) {
				if (r.outcome === "rejected") {
					expect(CONTENTION_CANCEL_CODES).toContain(r.reason.code);
				}
			}
		}

		const committed = this.transactions.filter((_, i) => outcomes[i] === undefined).map((tx) => tx.ops);
		const observed = poolModel(await readPool(db, this.keys), this.keys);
		expect(
			someOrderExplains(before, committed, observed, this.keys),
			`no order of the committed transactions leaves the state the batch left\n${this.describe(before, observed, outcomes)}`,
		).toBe(true);
		// The observed state agrees with an order of the batch, so the run goes on from it.
		m.items = observed.items;

		const replayed = this.transactions[this.replayIndex];
		const answer = await replayed.replay(db);
		expect(describeOutcome(answer), `a replay under the same token answered differently — ${replayed.toString()}`).toBe(
			describeOutcome(outcomes[this.replayIndex]),
		);
		const afterReplay = poolModel(await readPool(db, this.keys), this.keys);
		expect(stateSignature(afterReplay, this.keys), `a replay under the same token wrote again — ${replayed.toString()}`).toBe(
			stateSignature(observed, this.keys),
		);

		for (const tx of this.transactions) {
			for (const op of tx.ops) {
				await expectKeyUnlocked(m, db, op.key, this.stats);
			}
		}
	}

	private describe(before: Model, observed: Model, outcomes: readonly (TransactWriteOperationResult[] | undefined)[]): string {
		return [
			`before:\n${stateSignature(before, this.keys)}`,
			...this.transactions.map((tx, i) => `${describeOutcome(outcomes[i])} ${tx.toString()}`),
			`observed:\n${stateSignature(observed, this.keys)}`,
		].join("\n");
	}

	toString(): string {
		return `Contending(${this.transactions.join(" || ")})`;
	}
}

const MAX_CONTENDING_TRANSACTIONS = 4;
// The keys of one batch. A small number of them makes a meeting on an item the common case, and it
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
			// A transaction rejects two operations on one key, so the extra keys are unique and never
			// the contended one.
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

// ─── The table whose partitions split ─────────────────────────────────────────
//
// The split threshold of this table is small, and the seed writes far more than it, so every root
// partition splits, most children split again, and the large puts of a property keep new splits in
// flight while the commands run.
//
// One table serves every run, because a seeded and split table is too slow to build per run. Each
// run works on a key pool of its own and an empty model, and a suite reads the seeded items back
// before and after a property, so a split that drops or duplicates a row fails the suite.

export const HASH_SPLIT_MAX_SIZE_MB = 0.5;
// Fewer, larger items: the split thresholds care about bytes, not rows, so the same four
// megabytes split the tree the same way as more smaller writes,
// while the puts and the read-backs cost half the calls.
const SEED_ITEMS = 500;
const SEED_CONCURRENCY = 20;
const SEED_ITEM_BYTES = 8 * 1024;

const seedKey = (i: number) => ({ hashKey: `seed:${String(i).padStart(4, "0")}` });
const seedItemData = (i: number) => `${i}:`.padEnd(SEED_ITEM_BYTES, "x");

// Half of the puts of a property carry a large payload, so the table keeps growing past the
// threshold of its children and splits keep happening while the commands run.
export const arbLargeOrSmallData: fc.Arbitrary<ItemData> = fc.oneof(arbItemData, fc.constant("y".repeat(2 * SEED_ITEM_BYTES)));

/** Writes the seed. The caller then asserts with `expectSeedIntact` that the seed split the table. */
export async function seedSplitTable(db: FokosDB): Promise<void> {
	for (let start = 0; start < SEED_ITEMS; start += SEED_CONCURRENCY) {
		const batch = Array.from({ length: Math.min(SEED_CONCURRENCY, SEED_ITEMS - start) }, (_, j) => start + j);
		await Promise.all(batch.map((i) => untilAvailable(() => db.putItem({ ...seedKey(i), data: seedItemData(i) }))));
	}
}

/** Reads every seeded item back and returns the deepest hash tree level a read went through. */
export async function expectSeedIntact(db: FokosDB): Promise<number> {
	let maxHashDepth = 0;
	for (let start = 0; start < SEED_ITEMS; start += SEED_CONCURRENCY) {
		const batch = Array.from({ length: Math.min(SEED_CONCURRENCY, SEED_ITEMS - start) }, (_, j) => start + j);
		const reads = await Promise.all(batch.map((i) => untilAvailable(() => db.getItem(seedKey(i)))));
		reads.forEach((res, j) => {
			expect(res).toMatchObject({ found: true, item: { data: seedItemData(batch[j]), version: 1 } });
			maxHashDepth = Math.max(maxHashDepth, res.meta.hashDepth);
		});
	}
	return maxHashDepth;
}
