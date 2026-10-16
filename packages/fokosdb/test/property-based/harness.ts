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
//      A second pool puts a known number of keys on each root partition.
//   7. The commands, their arbitraries, and the run that drives them.
//   8. A batch of transactions over disjoint keys, where the model predicts every outcome.
//   9. A batch of transactions over the same keys, where the oracle is serializability.
//  10. The table whose partitions split while a suite writes to it.
//  11. A write transaction with a fault on one participant, and the table that takes faults.
//
// query-harness.ts holds the second model, for `queryItems`: a key order oracle, the pages of a
// request, and the range tree the split suites read.
//
// A failure prints the shell commands that replay it. README.md in this directory says how a run is
// sized and how a failure replays.
import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import fc from "fast-check";
import { expect } from "vitest";
import { FokosDB } from "../../src/client/db.js";
import {
	CONFLICT_CODES,
	CORE_INTERNAL_CODES,
	FokosError,
	FokosTransactionPendingError,
	FokosUnavailableError,
	UNAVAILABLE_CODES,
} from "../../src/shared/errors.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import type { ConditionExpression, UpdateExpression } from "../../src/shared/expression/types.js";
import type { JsonComposite, JsonPrimitive, JsonValue } from "../../src/shared/json-types.js";
import { SHARDING_UNAVAILABLE_CODES } from "../../src/sharding/errors.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { MAX_HASH_KEY_BYTES, MAX_SORT_KEY_BYTES } from "../../src/shared/transaction-limits.js";
import type { MaybeReadItem, TransactWriteItem, TransactWriteOperationResult } from "../../src/shared/transaction-api-types.js";
import type { DeleteItemResult, PutItemResult } from "../../src/shared/types.js";
import { testControlledPartitionStub } from "../stub-helpers.js";
import { controlledCoordinator, partitionRouter } from "../transactions/tx-helpers.js";

// ─── The size of a run ────────────────────────────────────────────────────────

// A suite runs inside the Workers runtime and cannot read the shell environment, so
// `vitest.config.ts` substitutes FOKOS_PROPERTY_RUNS and FOKOS_PROPERTY_REPLAY_PATH into these
// constants when it builds the module.
declare const __FOKOS_PROPERTY_RUNS__: string;
declare const __FOKOS_PROPERTY_REPLAY_PATH__: string;

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
 * range tree passes a small `rangeSplitMaxSizeMb` as well. `controlled` puts the table on the
 * Durable Object classes that have test controls, with one root coordinator, so a suite can make a
 * call to one partition fail.
 */
export function makeTestDB(opts?: { hashSplitMaxSizeMb?: number; rangeSplitMaxSizeMb?: number; controlled?: boolean }): FokosDB {
	return new FokosDB({
		table: {
			name: `test.pbt.${crypto.randomUUID()}`,
			ns: opts?.controlled ? "CONTROLLED_PARTITION_DO" : "PARTITION_DO",
			nsTx: opts?.controlled ? "CONTROLLED_TRANSACTION_COORDINATOR_DO" : "TRANSACTION_COORDINATOR_DO",
			rootTreesN: 3,
			hashSplitN: 2,
			coordinatorRootsN: opts?.controlled ? 1 : undefined,
		},
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: opts?.hashSplitMaxSizeMb ?? 500 },
		rangeSplitConditions: { maxSizeMb: opts?.rangeSplitMaxSizeMb ?? 500 },
	});
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

/** A json object. Every update applies to it, which is not true of an array, a text or a bytes item. */
export const arbJsonObject: fc.Arbitrary<JsonComposite> = fc
	.dictionary(fc.string({ minLength: 1, maxLength: 8 }), arbJsonLeaf, { maxKeys: 4 })
	.map((o) => jsonRoundTrip(o as { [k: string]: JsonValue }));

export const arbJsonData: fc.Arbitrary<JsonComposite> = fc.oneof(
	arbJsonObject,
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

/**
 * The name of the root partition that owns `key`. On a table that does not split, that partition
 * is the participant of every transaction on the key.
 */
function rootPartitionOf(db: FokosDB, key: ItemKey): string {
	return partitionRouter(db).rootContext(KeyCodec.encode(key.hashKey)).doName;
}

/**
 * A key pool with two keys on each root partition of `db`, so a run can choose how many partitions
 * a transaction touches. `poolKeys` leaves that number to the hash of four keys, and a run then
 * often has no key on one of the partitions.
 *
 * The function tries string and binary hash keys in turn and gives each partition the first two it
 * owns. Each partition gets one item with a sort key. The second item has no sort key, but on the
 * first partition it shares the hash key of the first item, so the pool also holds two items of one
 * hash key.
 */
export function placedPoolKeys(db: FokosDB, prefix: string): ItemKey[] {
	const roots = partitionRouter(db).allRoots().length;
	const hashKeys = new Map<string, (string | Uint8Array)[]>();
	const isFull = () => hashKeys.size === roots && [...hashKeys.values()].every((owned) => owned.length === 2);
	for (let i = 0; !isFull(); i++) {
		const hashKey = i % 2 === 0 ? `${prefix}${i}` : new Uint8Array([...textEncoder.encode(prefix), 0xff, i >> 8, i & 0xff]);
		const owned = hashKeys.get(rootPartitionOf(db, { hashKey })) ?? [];
		if (owned.length < 2) {
			hashKeys.set(rootPartitionOf(db, { hashKey }), [...owned, hashKey]);
		}
	}
	return [...hashKeys.values()].flatMap(([first, second], partition) => [
		{ hashKey: first, sortKey: typeof first === "string" ? "profile" : new Uint8Array([0x0a, 0x0b]) },
		partition === 0 ? { hashKey: first, sortKey: "settings" } : { hashKey: second },
	]);
}

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
async function untilUnlocked<T>(db: FokosDB, fn: () => Promise<T>): Promise<{ value: T; waited: boolean }> {
	let waited = false;
	try {
		const value = await retryWhile(
			() => untilAvailable(fn),
			isItemLocked,
			() => {
				waited = true;
			},
		);
		return { value, waited };
	} catch (e) {
		if (!isItemLocked(e)) {
			throw e;
		}
		const waitedSeconds = (RETRY_LIMIT * RETRY_DELAY_MS) / 1000;
		throw new Error(`a lock stayed for ${waitedSeconds} s or more\n${await describeLock(db, e)}`, { cause: e });
	}
}

/**
 * What the partition and the coordinator store for the lock that `e` reports, as text for an error
 * message. The partition gives the row of the transaction with its recovery times, and the
 * coordinator gives the state of the transaction and the answers of its participants. Each part also
 * gives the clock and the alarm of its Durable Object.
 *
 * A recovery that is slow shows a `next_recovery_at` or an alarm after `now`. A recovery that cannot
 * finish shows a `guarded_at`, a transaction that is not complete, or no alarm.
 */
async function describeLock(db: FokosDB, e: unknown): Promise<string> {
	try {
		const { ns, nsTx } = db.options().table;
		const { attributes, meta } = e as { attributes: { transactionId: string }; meta?: { servedByActorName?: string } };
		const { transactionId } = attributes;
		const partitionName = meta?.servedByActorName;
		if (!partitionName) {
			return `transaction ${transactionId}: the error does not name its partition`;
		}
		const read = (stub: DurableObjectStub, queries: Record<string, string>) =>
			runInDurableObject(stub, async (_instance, state) => ({
				now: Date.now(),
				alarm: await state.storage.getAlarm(),
				rows: Object.fromEntries(
					Object.entries(queries).map(([name, sql]) => [name, state.storage.sql.exec(sql, transactionId).toArray()]),
				),
			}));
		const partition = await read(env[ns].getByName(partitionName), {
			transaction: `SELECT transaction_ts, created_at, guarded_at, next_recovery_at, coordinator_json FROM pending_tx_info WHERE transaction_id = ?`,
			lockRows: `SELECT COUNT(*) AS n FROM pending_transactions WHERE transaction_id = ?`,
		});
		const lines = [`transaction ${transactionId}`, `partition ${partitionName}: ${JSON.stringify(partition)}`];
		const [info] = partition.rows.transaction as { coordinator_json: string }[];
		if (info) {
			const coordinatorName = (JSON.parse(info.coordinator_json) as { doName: string }).doName;
			const coordinator = await read(env[nsTx].getByName(coordinatorName), {
				transaction: `SELECT state, created_at, completed_at, next_recovery_at FROM tc_state WHERE transaction_id = ?`,
				participants: `SELECT partition_do_name, prepare_outcome, commit_outcome, cancel_outcome, error_json FROM tc_participants WHERE transaction_id = ?`,
			});
			lines.push(`coordinator ${coordinatorName}: ${JSON.stringify(coordinator)}`);
		}
		return lines.join("\n");
	} catch (reportError) {
		// The report must not hide the lock error.
		return `the report of the lock failed: ${String(reportError)}`;
	}
}

// ─── The model ────────────────────────────────────────────────────────────────

/**
 * One item of the model. A new item starts at `max_deleted_v + 1` of its partition, and the model does
 * not know the partition. Its version is then only a lower bound (`versionIsLowerBound`): the
 * version of the key never repeats, so it is above every earlier version of the key. The first read
 * or write answer that reports the version gives the exact value.
 */
export type ModelItem = { data: ItemData; kind: DataKind; version: number; versionIsLowerBound?: boolean };
/** `removedVersions` holds the last known version of each key that the model removed. */
export type Model = { items: Map<string, ModelItem>; removedVersions: Map<string, number> };

export const emptyModel = (): Model => ({ items: new Map(), removedVersions: new Map() });

/** The version of the next write of `id`: an overwrite adds one, a create is above every earlier version. */
function nextVersion(m: Model, id: string): Pick<ModelItem, "version" | "versionIsLowerBound"> {
	const item = m.items.get(id);
	if (item !== undefined) {
		return { version: item.version + 1, versionIsLowerBound: item.versionIsLowerBound };
	}
	return { version: (m.removedVersions.get(id) ?? 0) + 1, versionIsLowerBound: true };
}

/** Applies the put of a command to the model. */
function applyPut(m: Model, key: ItemKey, data: ItemData): void {
	const id = keyId(key);
	m.items.set(id, { data, kind: expectedDataKind(data), ...nextVersion(m, id) });
}

/** Applies the delete of a command to the model. */
function applyDelete(m: Model, key: ItemKey): void {
	const id = keyId(key);
	const item = m.items.get(id);
	if (item !== undefined) {
		m.removedVersions.set(id, Math.max(item.version, m.removedVersions.get(id) ?? 0));
		m.items.delete(id);
	}
}

/** True when the database can report `actual` for the model item. */
function versionAgrees(item: ModelItem, actual: number): boolean {
	return item.versionIsLowerBound ? actual >= item.version : actual === item.version;
}

const describeVersion = (item: ModelItem): string => `v${item.versionIsLowerBound ? "≥" : ""}${item.version}`;

/** Compares the version the database reported for `key` with the model, and keeps the exact value. */
function expectVersion(m: Model, key: ItemKey, actual: unknown): void {
	const item = m.items.get(keyId(key));
	expect(item, `the model holds no item for ${keyId(key)}`).toBeDefined();
	expect(typeof actual).toBe("number");
	expect(
		versionAgrees(item!, actual as number),
		`${keyId(key)} reported v${actual as number}, the model holds ${describeVersion(item!)}`,
	).toBe(true);
	item!.version = actual as number;
	item!.versionIsLowerBound = false;
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
	m.items.set(id, { data: document, kind: "json", ...nextVersion(m, id) });
}

/** Compares one read answer (a `getItem` result or one `transactGetItems` entry) with the model. */
function expectRead(m: Model, key: ItemKey, res: unknown): void {
	const expected = m.items.get(keyId(key));
	if (expected === undefined) {
		expect(res).toMatchObject({ found: false, ...key });
		return;
	}
	expect(res).toMatchObject({ found: true, ...key, kind: expected.kind });
	expectVersion(m, key, (res as { version: unknown }).version);
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
			applyDelete(m, op.key);
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
 * or imports its share answers 503. The sharding runtime defines `partition_migrating`, so its
 * codes are in the set too. A batch over the same keys meets more of them, which
 * `CONTENTION_CANCEL_CODES` lists.
 */
const ORDERING_CANCEL_CODES: ReadonlySet<string> = new Set([
	"timestamp_conflict",
	...Object.keys(UNAVAILABLE_CODES),
	...Object.keys(SHARDING_UNAVAILABLE_CODES),
]);

// ─── The counters of a run ────────────────────────────────────────────────────

/**
 * The paths a write transaction can take, by the number of partitions it touches. The client sends
 * a transaction on one partition to that partition in one round trip, unless the transaction
 * carries a `clientRequestToken`. Every other transaction goes through a coordinator and two phases.
 */
export const TRANSACTION_PATHS = [
	"single-shot, 1 participant",
	"coordinator, 1 participant",
	"coordinator, 2 participants",
	"coordinator, 3 participants",
] as const;
export type TransactionPath = (typeof TRANSACTION_PATHS)[number];

const transactionPath = (participants: number, withToken: boolean): TransactionPath =>
	TRANSACTION_PATHS[participants === 1 && !withToken ? 0 : participants];

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
	/** How the transactions of each path ended. Only a run that knows the placement of its keys fills it. */
	paths: Map<TransactionPath, { committed: number; cancelled: number }>;
	/** The fault and the answer of each transaction that a `FaultyTransactWrite` sent. */
	faults: { fault: Fault | undefined; outcome: FaultOutcome }[];
};

export function newTransactionStats(): TransactionStats {
	return {
		started: 0,
		committed: 0,
		cancelled: 0,
		peakInFlight: 0,
		inFlight: 0,
		rejections: new Map(),
		lockProbes: 0,
		lockProbeWaits: 0,
		paths: new Map(),
		faults: [],
	};
}

/** One line of the counts, for the message of a coverage assertion. */
export function describeTransactionStats(s: TransactionStats): string {
	const rejections = [...s.rejections].map(([code, n]) => `${code}=${n}`).join(" ") || "none";
	return [
		`${s.started} transactions: ${s.committed} committed, ${s.cancelled} cancelled`,
		`${s.peakInFlight} at most in flight`,
		`${s.lockProbes} lock probes, ${s.lockProbeWaits} of them waited`,
		`rejections: ${rejections}`,
		...[...s.paths].map(([path, n]) => `${path}: ${n.committed} committed, ${n.cancelled} cancelled`),
	].join("; ");
}

/** Runs one transaction and counts how many ran at one time and how each one ended, also for its `path`. */
async function recordTransaction(
	stats: TransactionStats | undefined,
	send: () => Promise<TransactWriteOperationResult[] | undefined>,
	path?: TransactionPath,
): Promise<TransactWriteOperationResult[] | undefined> {
	if (stats === undefined) {
		return await send();
	}
	stats.started++;
	stats.inFlight++;
	stats.peakInFlight = Math.max(stats.peakInFlight, stats.inFlight);
	try {
		const results = await send();
		if (path !== undefined) {
			const ended = stats.paths.get(path) ?? { committed: 0, cancelled: 0 };
			ended[results === undefined ? "committed" : "cancelled"]++;
			stats.paths.set(path, ended);
		}
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
		const { value } = await untilUnlocked(db, () => db.putItem({ ...key, data }));
		m.items.set(keyId(key), { data, kind: expectedDataKind(data), version: value.version });
	}
	const last = keys[keys.length - 1];
	await untilUnlocked(db, () => db.deleteItem(last));
	applyDelete(m, last);
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
	const model = emptyModel();
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
	const { value, waited } = await untilUnlocked<PutItemResult | DeleteItemResult>(db, () =>
		item === undefined ? db.deleteItem(key) : db.putItem({ ...key, data: item.data }),
	);
	if (item === undefined) {
		expect(value).toMatchObject({ item: key, deleted: false });
	} else {
		expect(value).toMatchObject({ item: key });
		applyPut(m, key, item.data);
		expectVersion(m, key, (value as PutItemResult).version);
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
		expect(res).toMatchObject({ item: this.key });
		applyPut(m, this.key, this.data);
		expectVersion(m, this.key, res.version);
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
		applyDelete(m, this.key);
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
	/**
	 * `withToken` sends the transaction with a `clientRequestToken`, which keeps a transaction on one
	 * partition on the coordinator path. `path` is the path the run expects, for the counters.
	 */
	constructor(
		readonly ops: TxOp[],
		readonly stats?: TransactionStats,
		readonly withToken = false,
		readonly path?: TransactionPath,
	) {
		super();
	}
	async run(m: Model, db: FokosDB): Promise<void> {
		// A transaction orders itself with a millisecond timestamp against the last write of every
		// item it touches. The run therefore lets the clock move first, which keeps commits common.
		await sleep(2);

		const rejections = this.ops.map((op) => expectedRejection(m, op));
		// `results` is undefined on a commit and holds the positional answers on a cancel.
		// Each try gets a token of its own, as the client gives a call that has none. A try after a 503
		// is then a new transaction, and never the stored answer of the try before it.
		const request = () => ({
			items: this.ops.map(toTransactWriteItem),
			clientRequestToken: this.withToken ? `pbt${crypto.randomUUID().replaceAll("-", "")}` : undefined,
		});
		const results = await recordTransaction(
			this.stats,
			() =>
				untilAvailable(() => db.transactWriteItems(request())).then(
					() => undefined,
					(e: unknown) => {
						if (FokosTransactionCancelledError.is(e)) {
							return e.results;
						}
						throw e;
					},
				),
			this.path,
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
		return `TransactWrite${this.withToken ? "[token]" : ""}(${describeTxOps(this.ops)})`;
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
 * pool, so the commands hit the same keys again. `data` is the payload of every put. `transactions`
 * replaces the two transaction commands, for a run that chooses the partitions of each one.
 */
export function commandArbitraries(
	keys: fc.Arbitrary<ItemKey>,
	data: fc.Arbitrary<ItemData>,
	transactions?: { write: fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>; readKeys: fc.Arbitrary<ItemKey[]> },
): fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>[] {
	const arbTxOp = txOpArbitrary(keys, data);
	// A transaction rejects two operations on one key, so the keys of a set are unique.
	const arbTxOps = fc.uniqueArray(arbTxOp, { minLength: 1, maxLength: 4, selector: (op) => keyId(op.key) });
	const arbReadKeys = fc.uniqueArray(keys, { minLength: 1, maxLength: 4, selector: keyId });

	return [
		fc.tuple(keys, data).map(([key, d]) => new PutItem(key, d)),
		keys.map((key) => new GetItem(key)),
		keys.map((key) => new DeleteItem(key)),
		transactions?.write ?? arbTxOps.map((ops) => new TransactWrite(ops)),
		(transactions?.readKeys ?? arbReadKeys).map((ks) => new TransactGet(ks)),
	];
}

/**
 * The keys of one transaction over a pool of `placedPoolKeys`: first the partitions it touches, from
 * one to all of them, and then one or more of the pool keys of each.
 */
function arbPlacedKeys(db: FokosDB, keys: readonly ItemKey[]): fc.Arbitrary<ItemKey[]> {
	const partitions = [...Map.groupBy(keys, (key) => rootPartitionOf(db, key)).values()];
	return (
		fc
			.shuffledSubarray(partitions, { minLength: 1 })
			.chain((touched) => fc.tuple(...touched.map((owned) => fc.shuffledSubarray(owned, { minLength: 1 }))))
			// The last shuffle mixes the keys of the partitions, so the request order does not group them.
			.chain((groups) => fc.shuffledSubarray(groups.flat(), { minLength: groups.flat().length }))
	);
}

/** One operation for each of the keys that `keys` draws. */
function arbOpsOver(keys: fc.Arbitrary<ItemKey[]>, data: fc.Arbitrary<ItemData>): fc.Arbitrary<TxOp[]> {
	return keys.chain((ks) => fc.tuple(...ks.map((key) => txOpArbitrary(fc.constant(key), data))));
}

/** The root partitions that a transaction of `ops` touches. */
const participantsOf = (db: FokosDB, ops: readonly TxOp[]): string[] => [...new Set(ops.map((op) => rootPartitionOf(db, op.key)))];

/**
 * The commands of a run over a placed pool, with `write` in the place of the write transaction. The
 * counters of a suite are for the write transactions, so a run draws one more often than every
 * other command together.
 */
function placedCommands(
	keys: readonly ItemKey[],
	data: fc.Arbitrary<ItemData>,
	write: fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>,
	readKeys: fc.Arbitrary<ItemKey[]>,
): fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>[] {
	const commands = commandArbitraries(fc.constantFrom(...keys), data, { write, readKeys });
	return [fc.oneof({ arbitrary: write, weight: 4 }, ...commands.map((arbitrary) => ({ arbitrary, weight: 1 })))];
}

/**
 * The command arbitraries of a run over a pool of `placedPoolKeys`. A transaction draws the
 * partitions it touches, as `arbPlacedKeys` says. A write transaction also draws whether it carries
 * a token, and counts how it ended in `stats.paths`.
 */
export function placedCommandArbitraries(
	db: FokosDB,
	keys: readonly ItemKey[],
	data: fc.Arbitrary<ItemData>,
	stats: TransactionStats,
): fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>[] {
	const arbKeys = arbPlacedKeys(db, keys);
	const write = fc
		.tuple(arbOpsOver(arbKeys, data), fc.boolean())
		.map(([ops, withToken]) => new TransactWrite(ops, stats, withToken, transactionPath(participantsOf(db, ops).length, withToken)));
	return placedCommands(keys, data, write, arbKeys);
}

// ─── One run of a stateful property ───────────────────────────────────────────

/** The commands of one run, and the key pool they draw their keys from. */
export type Run = { keys: readonly ItemKey[]; cmds: Iterable<fc.AsyncCommand<Model, FokosDB>> };

/**
 * A run over a key pool of its own. `makeCommands` builds the command arbitraries over that pool.
 * The run draws the prefix of its pool, so a replay with the same seed uses the same keys. `pool`
 * builds the keys from the prefix.
 */
export function arbRun(
	makeCommands: (keys: ItemKey[]) => fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>[],
	constraints: fc.CommandsContraints,
	pool: (prefix: string) => ItemKey[] = poolKeys,
): fc.Arbitrary<Run> {
	// The shrink of a command sequence depends on which commands ran, so a replay of a shrunk sequence
	// needs the `replayPath` that the failure printed, next to its seed and path. A `replayPath` in
	// `constraints` wins, as the parameters of an `fc.assert` call win over the global ones.
	const replay = __FOKOS_PROPERTY_REPLAY_PATH__ === "" ? {} : { replayPath: __FOKOS_PROPERTY_REPLAY_PATH__ };
	return fc.uuid().chain((prefix) => {
		const keys = pool(`${prefix}:`);
		return fc.record({ keys: fc.constant(keys), cmds: fc.commands(makeCommands(keys), { ...replay, ...constraints }) });
	});
}

/**
 * Runs one command sequence against the database and the model at the same time. It seeds the pool,
 * runs the commands, and then compares every key of the pool. The last comparison fails a run whose
 * divergence no read command observed.
 */
export async function runCommands(db: FokosDB, run: Run, seedData?: readonly ItemData[]): Promise<void> {
	const model = emptyModel();
	try {
		await seedPool(db, model, run.keys, seedData);
	} catch (e) {
		const earlier = firstRunFailures.get(db);
		throw new Error(
			[
				"the seed of the pool failed before a command ran, so this counterexample is not a failure of its commands.",
				"An earlier run on the same keys left the state that stops the seed.",
				earlier === undefined
					? "No run of this table failed before."
					: `The first failure of this table was:\n${earlier instanceof Error ? (earlier.stack ?? earlier.message) : JSON.stringify(earlier)}`,
			].join("\n"),
			{ cause: e },
		);
	}
	try {
		await fc.asyncModelRun(() => ({ model, real: db }), run.cmds);
		await expectModelMatches(db, model, run.keys);
	} catch (e) {
		if (!firstRunFailures.has(db)) {
			firstRunFailures.set(db, e);
		}
		throw e;
	}
}

// The first failure of the commands of each table. fast-check runs smaller inputs on the same table
// after a failure and reports only the last one that fails. When the first failure leaves a lock, each
// later run fails in the seed, and the report then has no trace of the first failure.
const firstRunFailures = new WeakMap<FokosDB, unknown>();

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
			return `${keyId(key)} → ${item === undefined ? "absent" : `${describeVersion(item)} ${item.kind} ${canonicalData(item.data)}`}`;
		})
		.join("\n");
}

/** True when the observed state can be the candidate state: a lower-bound version accepts a higher one. */
function stateAgrees(candidate: Model, observed: Model, keys: readonly ItemKey[]): boolean {
	return keys.every((key) => {
		const want = candidate.items.get(keyId(key));
		const got = observed.items.get(keyId(key));
		if (want === undefined || got === undefined) {
			return want === got;
		}
		return want.kind === got.kind && canonicalData(want.data) === canonicalData(got.data) && versionAgrees(want, got.version);
	});
}

const cloneModel = (m: Model): Model => ({
	items: new Map([...m.items].map(([id, item]) => [id, { ...item }])),
	removedVersions: new Map(m.removedVersions),
});

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
	return permutations(committed).some((order) => {
		const candidate = cloneModel(before);
		return order.every((ops) => applyWhenPremisesHold(candidate, ops)) && stateAgrees(candidate, observed, keys);
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
		for (const key of this.keys) {
			if (!observed.items.has(keyId(key))) {
				applyDelete(m, key);
			}
		}
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
			maxHashDepth = Math.max(maxHashDepth, res.meta.partitions[0].hashDepth!);
		});
	}
	return maxHashDepth;
}

// ─── A write transaction with a fault on one participant ──────────────────────
//
// A fault makes one RPC of one participant fail. The request is lost before the partition does its
// work, or the partition does its work and the answer is lost. One call fails, or each call fails
// until the command removes the fault.
//
// The run is serial, so the model still predicts the state exactly. What the client must get depends
// on the RPC:
//
//   - `txExecuteSingleShot`: the client reports the failure, and cannot know if the write applied.
//     The model knows: the write applied only when the answer was lost and every premise held.
//   - `txPrepare`: the coordinator sends a prepare again, so one failure changes nothing. A
//     participant that never answers cancels the transaction.
//   - `txCommit`: the decision is durable, so the transaction commits. While a participant does not
//     confirm, the client gets `transaction_commit_pending`.
//   - `txCancel`: the transaction is cancelled and applied nothing. The participant keeps its locks
//     until the recovery of the partition or of the coordinator releases them.
//
// After each transaction, the keys it touched must agree with the model, each key must take a write
// that no transaction drives, and a call with the same token must give the same outcome. The recovery
// jobs or that call finish the work that the fault stopped, and the reads and the writes wait for it.

type FaultOp = "txExecuteSingleShot" | "txPrepare" | "txCommit" | "txCancel";

export type Fault = {
	op: FaultOp;
	/** The name of the root partition whose calls fail. */
	partition: string;
	/** True when the partition does its work and the answer is lost. False when the request is lost. */
	replyLost: boolean;
	/** True when each call fails until the command removes the fault. False when one call fails. */
	untilCleared: boolean;
};

/** What the client got: `failed` is an error that does not say if the transaction applied. */
export type FaultOutcome = "committed" | "cancelled" | "pending" | "failed";

const describeFault = (fault: Fault | undefined): string =>
	fault === undefined
		? "no fault"
		: `${fault.op}, ${fault.replyLost ? "reply lost" : "request lost"}, ${fault.untilCleared ? "until cleared" : "once"}`;

/** One line per fault kind with the count of each outcome, for the message of a coverage assertion. */
export function describeFaultStats(stats: TransactionStats): string {
	const counts = new Map<string, number>();
	for (const { fault, outcome } of stats.faults) {
		const line = `${describeFault(fault)} → ${outcome}`;
		counts.set(line, (counts.get(line) ?? 0) + 1);
	}
	return [...counts]
		.map(([line, n]) => `${line}: ${n}`)
		.sort((a, b) => a.localeCompare(b))
		.join("\n");
}

// The code of an operation whose participant never answered the prepare: the coordinator stores the
// error of its last try, and an injected failure is not an error of the library.
const PREPARE_FAULT_CODES: ReadonlySet<string> = new Set([CORE_INTERNAL_CODES.foreign_error.code]);

// Short times, so the answer to a fault that stays and the recovery after it take a part of a
// second. A participant retry must fit in the request budget many times.
const FAULT_BUDGET_MS = 250;

/**
 * Gives the coordinator and every root partition of a `controlled` table short budgets, short
 * stale times and short retry waits. A setting is a field of one Durable Object instance.
 */
export async function useShortRecoveryTimes(db: FokosDB): Promise<void> {
	await controlledCoordinator(db).testConfig({
		fanoutRequestBudgetMs: FAULT_BUDGET_MS,
		staleTransactionMs: FAULT_BUDGET_MS,
		participantRetry: { baseDelayMs: 10, maxDelayMs: 40 },
	});
	for (const root of partitionRouter(db).allRoots()) {
		await testControlledPartitionStub(root.doName).testConfig({
			staleTransactionMs: FAULT_BUDGET_MS,
			coordinatorFanoutBudgetMs: FAULT_BUDGET_MS,
		});
	}
}

/**
 * One write transaction on a `controlled` table, with `fault` on one of its participants while the
 * client waits. `replayFirst` sends the call with the same token again before the keys are read, so
 * that call finishes the transaction. Without it, the recovery jobs do.
 */
export class FaultyTransactWrite extends ModelCommand {
	constructor(
		readonly ops: TxOp[],
		readonly withToken: boolean,
		readonly fault: Fault | undefined,
		readonly replayFirst: boolean,
		readonly stats: TransactionStats,
	) {
		super();
	}

	async run(m: Model, db: FokosDB): Promise<void> {
		// The clock moves first, as in `TransactWrite`.
		await sleep(2);

		const rejections = this.ops.map((op) => expectedRejection(m, op));
		const premisesHold = rejections.every((codes) => codes === null);
		const token = this.withToken ? `pbt${crypto.randomUUID().replaceAll("-", "")}` : undefined;
		const fault = this.fault;
		const partition = fault && testControlledPartitionStub(fault.partition);

		await partition?.testTxResponse(fault!.op, {
			error: "the test fails this call",
			afterWork: fault!.replyLost,
			...(fault!.untilCleared ? {} : { times: 1 }),
		});
		let answer: TransactWriteOperationResult[] | Exclude<FaultOutcome, "cancelled">;
		try {
			answer = await db.transactWriteItems({ items: this.ops.map(toTransactWriteItem), clientRequestToken: token }).then(
				() => "committed" as const,
				(e: unknown) => {
					if (FokosTransactionCancelledError.is(e)) {
						return e.results;
					}
					if (FokosTransactionPendingError.is(e)) {
						return "pending" as const;
					}
					if (FokosError.isCode(e, CORE_INTERNAL_CODES.foreign_error)) {
						return "failed" as const;
					}
					throw e;
				},
			);
		} finally {
			// A fault that no call used must not fail a call of the next command.
			await partition?.testClearTxResponse(fault!.op);
		}
		const outcome = typeof answer === "string" ? answer : "cancelled";
		this.stats.faults.push({ fault, outcome });

		const prepareNeverAnswers = fault?.op === "txPrepare" && fault.untilCleared;
		const context = `${this.toString()} answered ${outcome}`;
		if (fault?.op === "txExecuteSingleShot") {
			expect(outcome, `the client has one round trip and it failed — ${context}`).toBe("failed");
		} else if (!premisesHold || prepareNeverAnswers) {
			expect(outcome, `a failing premise and a prepare with no answer must cancel — ${context}`).toBe("cancelled");
		} else {
			// The premises hold, so the transaction commits unless the order of the partition refuses it.
			// A slow commit can also answer pending when no fault is there.
			expect(outcome, `only a single round trip can fail with no outcome — ${context}`).not.toBe("failed");
			if (fault?.op === "txCommit" && fault.untilCleared) {
				expect(outcome, `a participant that never confirms must keep the answer pending — ${context}`).not.toBe("committed");
			}
		}
		if (typeof answer !== "string") {
			expect(answer).toHaveLength(this.ops.length);
			answer.forEach((r, i) => {
				const code = r.outcome === "rejected" ? r.reason.code : undefined;
				if (prepareNeverAnswers && rootPartitionOf(db, this.ops[i].key) === fault.partition) {
					// The coordinator has no answer for these operations, also when a premise of one failed.
					expect(PREPARE_FAULT_CODES, `operation ${i} of ${context}`).toContain(code);
				} else if (rejections[i] !== null) {
					expect(rejections[i], `operation ${i} of ${context}`).toContain(premiseCode(r));
				} else if (code !== undefined) {
					expect(ORDERING_CANCEL_CODES, `operation ${i} of ${context}`).toContain(code);
				}
			});
		}

		// A pending answer says that the transaction will commit. A failed single round trip applied
		// when the partition did its work and every premise held.
		if (outcome === "committed" || outcome === "pending" || (outcome === "failed" && fault!.replyLost && premisesHold)) {
			applyTxOps(m, this.ops);
		}

		const replay = async (): Promise<void> => {
			if (token === undefined) {
				return;
			}
			// `sendPinned` waits out a pending answer, so this is the terminal outcome of the transaction.
			const again = await sendPinned(db, this.ops, token);
			expect(describeOutcome(again), `a call with the same token answered differently — ${context}`).toBe(
				describeOutcome(typeof answer === "string" ? undefined : answer),
			);
		};
		const keys = this.ops.map((op) => op.key);
		if (this.replayFirst) {
			await replay();
		}
		// The read waits while a key holds a pending write, and the probe waits while it holds any lock.
		await expectModelMatches(db, m, keys);
		for (const key of keys) {
			await expectKeyUnlocked(m, db, key, this.stats);
		}
		if (!this.replayFirst) {
			await replay();
		}
	}

	toString(): string {
		const flags = [this.withToken ? "[token]" : "", this.withToken && this.replayFirst ? "[replay first]" : ""].join("");
		return `FaultyTransactWrite${flags}[${describeFault(this.fault)}](${describeTxOps(this.ops)})`;
	}
}

/**
 * The command arbitraries of a run over a pool of `placedPoolKeys` on a `controlled` table. Each
 * write transaction is a `FaultyTransactWrite`, and seven of eight have a fault. A transaction that
 * goes to one partition in one round trip has the fault on that call. Every other transaction
 * draws the prepare, the commit or the cancel of one of its participants.
 */
export function faultyCommandArbitraries(
	db: FokosDB,
	keys: readonly ItemKey[],
	data: fc.Arbitrary<ItemData>,
	stats: TransactionStats,
): fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>[] {
	const arbKeys = arbPlacedKeys(db, keys);
	const write = fc
		.record({
			ops: arbOpsOver(arbKeys, data),
			withToken: fc.boolean(),
			faulted: fc.oneof({ arbitrary: fc.constant(true), weight: 7 }, { arbitrary: fc.constant(false), weight: 1 }),
			op: fc.constantFrom<FaultOp>("txPrepare", "txCommit", "txCancel"),
			participant: fc.nat(),
			replyLost: fc.boolean(),
			untilCleared: fc.boolean(),
			replayFirst: fc.boolean(),
		})
		.map(({ ops, withToken, faulted, op, participant, replyLost, untilCleared, replayFirst }) => {
			const participants = participantsOf(db, ops);
			const singleRoundTrip = participants.length === 1 && !withToken;
			const fault: Fault | undefined = faulted
				? {
						op: singleRoundTrip ? "txExecuteSingleShot" : op,
						partition: participants[participant % participants.length],
						replyLost,
						// The client does not send a single round trip again, so one failure is all it meets.
						untilCleared: !singleRoundTrip && untilCleared,
					}
				: undefined;
			return new FaultyTransactWrite(ops, withToken, fault, replayFirst, stats);
		});
	return placedCommands(keys, data, write, arbKeys);
}
