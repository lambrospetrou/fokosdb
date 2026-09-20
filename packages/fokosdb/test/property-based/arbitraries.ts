// Shared fast-check arbitraries for the property-based suites. An arbitrary generates random
// values of one shape, and fast-check shrinks a failing value to a minimal counterexample. Every
// arbitrary here produces only inputs that the public API accepts, so a failure is a library bug
// and never a validation error.
import fc from "fast-check";
import { FokosDB } from "../../src/client/db.js";
import type { JsonComposite, JsonPrimitive, JsonValue } from "../../src/shared/json-types.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import { PartitionContextCreator } from "../../src/shared/partition-topology/partition-context.js";
import { PartitionTopologyRouterImpl } from "../../src/shared/partition-topology/router.js";
import { MAX_HASH_KEY_BYTES, MAX_SORT_KEY_BYTES } from "../../src/shared/transaction-limits.js";

// A suite runs inside the Workers runtime and cannot read the shell environment, so
// `vitest.config.ts` substitutes FOKOS_PROPERTY_RUNS into this constant when it builds the module.
declare const __FOKOS_PROPERTY_RUNS__: string;

/**
 * The run count of one property: FOKOS_PROPERTY_RUNS when the shell sets it, else `defaultRuns`.
 * Keep the default small enough for every run of the test suite, and give a deeper search the
 * variable: `FOKOS_PROPERTY_RUNS=500 pnpm vitest run test/property-based/`.
 */
export function propertyRuns(defaultRuns: number): number {
	const configured = __FOKOS_PROPERTY_RUNS__;
	if (configured === "") return defaultRuns;
	const runs = Number(configured);
	if (!Number.isSafeInteger(runs) || runs <= 0) {
		throw new Error(`FOKOS_PROPERTY_RUNS must be a positive integer, got ${JSON.stringify(configured)}`);
	}
	return runs;
}

export type ItemKey = { hashKey: string | Uint8Array; sortKey?: string | Uint8Array };
export type ItemData = string | Uint8Array | JsonComposite;
export type DataKind = "text" | "bytes" | "json";

/** Stable identity of a key pair, usable as a `Map` key. A `Uint8Array` compares by reference. */
export function keyId(key: ItemKey): string {
	return `${KeyCodec.encode(key.hashKey).toHex()}:${KeyCodec.encodeOptional(key.sortKey).toHex()}`;
}

/** The `kind` that a read returns for the data a write accepted. */
export function expectedDataKind(data: ItemData): DataKind {
	if (data instanceof Uint8Array) return "bytes";
	if (typeof data === "string") return "text";
	return "json";
}

/**
 * A fresh table on three root partitions, so random hash keys spread across partition DOs. The
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
	return new FokosDB({ topology: new PartitionTopologyRouterImpl(base) });
}

// `unit: "binary"` draws from the whole code point range, so the filter below does real work: it
// rejects NUL (a string key must not contain it) and holds the encoded size under the key limit.
function arbStringKey(maxBytes: number): fc.Arbitrary<string> {
	return fc
		.string({ minLength: 1, maxLength: 32, unit: "binary" })
		.filter((s) => !s.includes("\0") && KeyCodec.encode(s).byteLength <= maxBytes);
}

// A binary key gets a one-byte tag when encoded, so its raw length stays one under the limit.
function arbBinaryKey(maxBytes: number): fc.Arbitrary<Uint8Array> {
	return fc.uint8Array({ minLength: 1, maxLength: Math.min(64, maxBytes - 1) });
}

export const arbHashKey = fc.oneof(arbStringKey(MAX_HASH_KEY_BYTES), arbBinaryKey(MAX_HASH_KEY_BYTES));
export const arbSortKey = fc.oneof(arbStringKey(MAX_SORT_KEY_BYTES), arbBinaryKey(MAX_SORT_KEY_BYTES), fc.constant(undefined));
export const arbItemKey: fc.Arbitrary<ItemKey> = fc.record({ hashKey: arbHashKey, sortKey: arbSortKey });

const arbJsonLeaf: fc.Arbitrary<JsonPrimitive> = fc.oneof(fc.string({ maxLength: 32 }), fc.integer(), fc.boolean(), fc.constant(null));

// A stored json value comes back through JSON.parse. Passing the generated value through the same
// round trip gives the model the exact object a read returns (a plain prototype, no `-0`).
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

// A stateful run needs repeated hits on the same keys. Random keys almost never collide, so the
// commands draw from this small pool. It mixes string and binary keys and hash-key-only items.
export const POOL_KEYS: readonly ItemKey[] = poolKeys("");
export const arbPoolKey: fc.Arbitrary<ItemKey> = fc.constantFrom(...POOL_KEYS);

/**
 * The key pool with `prefix` in front of every hash key. A suite whose runs share one table gives
 * each run its own prefix, so the runs never see each other's items.
 */
export function poolKeys(prefix: string): ItemKey[] {
	const bin = (...bytes: number[]) => new Uint8Array([...new TextEncoder().encode(prefix), ...bytes]);
	return [
		{ hashKey: `${prefix}user:1`, sortKey: "profile" },
		{ hashKey: `${prefix}user:1`, sortKey: "settings" },
		{ hashKey: `${prefix}user:2` },
		{ hashKey: bin(0x01, 0x02, 0x03), sortKey: new Uint8Array([0x0a, 0x0b]) },
		{ hashKey: bin(0xff, 0xfe) },
	];
}
