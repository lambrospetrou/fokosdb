// The shared model and commands of the stateful suites. The model is a map from `keyId` to the
// item the database must hold. Every command runs one public operation, compares the answer with
// the model, and then advances the model. A write transaction advances it only on a commit.
import fc from "fast-check";
import { expect } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import { FokosUnavailableError, UNAVAILABLE_CODES } from "../../src/shared/errors.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import type { ConditionExpression } from "../../src/shared/expression/types.js";
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
	if (expected === undefined) expect(res).toMatchObject({ found: false, ...key });
	else expect(res).toMatchObject({ found: true, ...key, ...expected });
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

/** Reads every key of the pool in one transaction and compares each answer with the model. */
export async function expectModelMatches(db: FokosDB, m: Model, keys: readonly ItemKey[]): Promise<void> {
	const res = await untilAvailable(() => db.transactGetItems({ items: [...keys] }));
	keys.forEach((key, i) => expectRead(m, key, res.items[i]));
}

// One operation of a write transaction, in the model's own vocabulary. `expectExists` is the only
// condition the model can evaluate: it is required for a check and optional for a put or a delete.
export type TxOp = { key: ItemKey; expectExists?: boolean } & (
	| { operation: "put"; data: ItemData }
	| { operation: "delete" }
	| { operation: "check"; expectExists: boolean }
);

function existsCondition(expectExists: boolean): ConditionExpression {
	return { op: expectExists ? "exists" : "not_exists", args: [{ ref: "hashKey" }] };
}

function toTransactWriteItem(op: TxOp): TransactWriteItem {
	const condition = op.expectExists === undefined ? undefined : existsCondition(op.expectExists);
	if (op.operation === "put") return { operation: "put", ...op.key, data: op.data, condition };
	if (op.operation === "delete") return { operation: "delete", ...op.key, condition };
	return { operation: "check", ...op.key, condition: existsCondition(op.expectExists) };
}

// A cancel with no failing condition is acceptable only for these reasons. A transaction in the
// same millisecond as the last write of an item is a `timestamp_conflict`; the 503 codes come from
// a partition that is mid-split when a mixed cancel also carries a caller-side reason.
const ACCEPTED_CANCEL_CODES: ReadonlySet<string> = new Set(["timestamp_conflict", ...Object.keys(UNAVAILABLE_CODES)]);

const isConditionFailed = (r: TransactWriteOperationResult) => r.outcome === "rejected" && r.reason.code === "condition_failed";

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

		const failing = this.ops.map((op) => op.expectExists !== undefined && op.expectExists !== m.items.has(keyId(op.key)));
		// `results` is undefined on a commit and holds the positional answers on a cancel.
		const results = await untilAvailable(() => db.transactWriteItems({ items: this.ops.map(toTransactWriteItem) })).then(
			() => undefined,
			(e: unknown) => {
				if (FokosTransactionCancelledError.is(e)) return e.results;
				throw e;
			},
		);

		if (failing.includes(true)) {
			// The model knows a condition failed, so the transaction must cancel and name that operation.
			// Every participant answers its own operations in parallel, so no failing one stays
			// `not_evaluated`. The model does not change.
			expect(results, "a failing condition must cancel the transaction").toBeDefined();
			if (results === undefined) return;
			expect(results).toHaveLength(this.ops.length);
			failing.forEach((fails, i) => expect(isConditionFailed(results[i])).toBe(fails));
			return;
		}

		if (results !== undefined) {
			// Every condition held, so only an ordering or availability reason may cancel, and
			// atomicity says the model does not change.
			for (const r of results) if (r.outcome === "rejected") expect(ACCEPTED_CANCEL_CODES).toContain(r.reason.code);
			return;
		}

		for (const op of this.ops) {
			if (op.operation === "put") applyPut(m, op.key, op.data);
			else if (op.operation === "delete") m.items.delete(keyId(op.key));
		}
	}
	toString(): string {
		const ops = this.ops.map(
			(op) => `${op.operation}${op.expectExists === undefined ? "" : op.expectExists ? "?exists" : "?absent"}(${keyId(op.key)})`,
		);
		return `TransactWrite(${ops.join(", ")})`;
	}
}

/**
 * The command arbitraries of a stateful run over `keys`. The key arbitrary must draw from a small
 * pool, so commands hit the same keys again; `data` is the payload of every put.
 */
export function commandArbitraries(
	keys: fc.Arbitrary<ItemKey>,
	data: fc.Arbitrary<ItemData>,
): fc.Arbitrary<fc.AsyncCommand<Model, FokosDB>>[] {
	// A random condition holds half of the time, and one failing operation cancels the whole set. The
	// optional condition is rare and the check operation is light, so most sets commit and the run
	// still sees enough cancels.
	const arbOptionalCondition = fc.oneof({ arbitrary: fc.constant(undefined), weight: 4 }, { arbitrary: fc.boolean(), weight: 1 });
	const arbTxOp: fc.Arbitrary<TxOp> = fc
		.tuple(
			keys,
			fc.oneof(
				{ arbitrary: fc.record({ operation: fc.constant("put" as const), data, expectExists: arbOptionalCondition }), weight: 3 },
				{ arbitrary: fc.record({ operation: fc.constant("delete" as const), expectExists: arbOptionalCondition }), weight: 2 },
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
