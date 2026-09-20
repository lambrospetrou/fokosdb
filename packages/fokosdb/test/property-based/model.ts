// The shared model and commands of the stateful suites. The model is a map from `keyId` to the
// item the database must hold. Every command runs one public operation, compares the answer with
// the model, and then advances the model. A write transaction advances it only on a commit.
import fc from "fast-check";
import { expect } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import { FokosUnavailableError, UNAVAILABLE_CODES } from "../../src/shared/errors.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import type { ConditionExpression, UpdateExpression } from "../../src/shared/expression/types.js";
import type { JsonComposite, JsonPrimitive, JsonValue } from "../../src/shared/json-types.js";
import type { TransactWriteItem, TransactWriteOperationResult } from "../../src/shared/transaction-api-types.js";
import { expectedDataKind, keyId, type DataKind, type ItemData, type ItemKey } from "./arbitraries.js";

export type ModelItem = { data: ItemData; kind: DataKind; version: number };
export type Model = { items: Map<string, ModelItem> };

/** The one place that applies a put to the model. A new item starts at version 1, an overwrite adds one. */
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
 * Writes one item for every pool key but the last, and records them in the model. The last key stays
 * absent, so a run still covers the paths that create an item.
 */
export async function seedPool(db: FokosDB, m: Model, keys: readonly ItemKey[], seedData: readonly ItemData[] = SEED_DATA): Promise<void> {
	for (const [index, key] of keys.slice(0, -1).entries()) {
		const data = seedData[index % seedData.length];
		await untilAvailable(() => db.putItem({ ...key, data }));
		applyPut(m, key, data);
	}
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

function existsCondition(expectExists: boolean): ConditionExpression {
	return { op: expectExists ? "exists" : "not_exists", args: [{ ref: "hashKey" }] };
}

function toTransactWriteItem(op: TxOp): TransactWriteItem {
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
function expectedRejection(m: Model, op: TxOp): string[] | null {
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
	constructor(readonly ops: TxOp[]) {
		super();
	}
	async run(m: Model, db: FokosDB): Promise<void> {
		// A transaction orders itself with a millisecond timestamp against the last write of every
		// item it touches, so the run first lets the clock move to keep commits common.
		await sleep(2);

		const rejections = this.ops.map((op) => expectedRejection(m, op));
		// `results` is undefined on a commit and holds the positional answers on a cancel.
		const results = await untilAvailable(() => db.transactWriteItems({ items: this.ops.map(toTransactWriteItem) })).then(
			() => undefined,
			(e: unknown) => {
				if (FokosTransactionCancelledError.is(e)) return e.results;
				throw e;
			},
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

		for (const op of this.ops) {
			if (op.operation === "put") applyPut(m, op.key, op.data);
			else if (op.operation === "delete") m.items.delete(keyId(op.key));
			else if (op.operation === "update") applyUpdate(m, op.key, op.actions);
		}
	}
	toString(): string {
		const ops = this.ops.map((op) => {
			const condition = op.expectExists === undefined ? "" : op.expectExists ? "?exists" : "?absent";
			// The actions decide the document, so a counterexample must print them.
			const actions =
				op.operation === "update"
					? ` [${op.actions.map((a) => (a.action === "set" ? `set ${a.field}=${JSON.stringify(a.value)}` : `remove ${a.field}`)).join(", ")}]`
					: "";
			return `${op.operation}${condition}(${keyId(op.key)})${actions}`;
		});
		return `TransactWrite(${ops.join(", ")})`;
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

/**
 * The command arbitraries of a stateful run over `keys`. The key arbitrary must draw from a small
 * pool, so commands hit the same keys again; `data` is the payload of every put.
 */
export function commandArbitraries(
	keys: fc.Arbitrary<ItemKey>,
	data: fc.Arbitrary<ItemData>,
): fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>[] {
	const arbTxOp: fc.Arbitrary<TxOp> = fc
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
