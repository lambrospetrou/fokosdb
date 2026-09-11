import { describe, expect, it } from "vitest";
import {
	CONFLICT_CODES,
	FOKOS_CODE_TABLES,
	FOKOS_ERROR_CATEGORIES,
	FokosConflictError,
	FokosError,
	FokosInternalError,
	FokosUnavailableError,
	FokosValidationError,
	INTERNAL_CODES,
	UNAVAILABLE_CODES,
	VALIDATION_CODES,
	defineCodes,
	defineErrorGuard,
	isRuntimeRetryableError,
	type FokosCodeDef,
	type FokosErrorOptions,
} from "./errors.js";
import { isFokosAnyError, type FokosAnyError } from "./errors-operations.js";

const DEFS: FokosCodeDef[] = FOKOS_CODE_TABLES.flatMap((table) => Object.values(table));

/** One error of `def`, built through the class of its category. */
function errorOf(def: FokosCodeDef, options: Partial<FokosErrorOptions> = {}): FokosError {
	const Category = FOKOS_ERROR_CATEGORIES.get(def.tag)!;
	return new Category(def, { message: "a fixed phrase", ...options });
}

/** The fields that make up the contract of an error. */
function contractOf(e: FokosError) {
	return {
		name: e.name,
		_tag: e._tag,
		type: e.type,
		code: e.code,
		error_id: e.error_id,
		origin: e.origin,
		httpStatusHint: e.httpStatusHint,
		attributes: e.attributes,
		message: e.message,
	};
}

describe("the code tables", () => {
	it("give every code a unique segment", () => {
		const segments = DEFS.map((def) => def.segment);
		expect(new Set(segments).size).toBe(segments.length);
	});

	it("define every code once, under its own name", () => {
		expect(new Set(DEFS.map((def) => def.code)).size).toBe(DEFS.length);
		for (const table of FOKOS_CODE_TABLES) {
			for (const [key, def] of Object.entries(table)) expect(def.code).toBe(key);
		}
	});

	it("take every segment from the unambiguous alphabet", () => {
		for (const def of DEFS) expect(def.segment, def.code).toMatch(/^[a-hjkmnp-z2-9]{6}$/);
	});

	it("give each category of this module one table, and each table one category", () => {
		const tagsOfTables = FOKOS_CODE_TABLES.map((table) => [...new Set(Object.values(table).map((def) => def.tag))]);
		for (const tags of tagsOfTables) expect(tags).toHaveLength(1);
		expect(tagsOfTables.flat().sort()).toEqual([...FOKOS_ERROR_CATEGORIES.keys()].sort());
	});

	it("give clock_skew the service origin, not the one of its category", () => {
		expect([CONFLICT_CODES.clock_skew.origin, CONFLICT_CODES.clock_skew.httpStatusHint]).toEqual(["service", 503]);
		expect([CONFLICT_CODES.read_conflict.origin, CONFLICT_CODES.read_conflict.httpStatusHint]).toEqual(["caller", 409]);
	});
});

describe("the category classes", () => {
	it("declare no prototype member", () => {
		for (const Category of [FokosError, ...FOKOS_ERROR_CATEGORIES.values()]) {
			expect(Object.getOwnPropertyNames(Category.prototype), Category.name).toEqual(["constructor"]);
		}
	});

	it("assign every field as an own property", () => {
		for (const def of DEFS) {
			const e = errorOf(def, { cause: new Error("inner") });
			for (const key of ["name", "message", "_tag", "type", "code", "error_id", "origin", "httpStatusHint", "attributes", "cause"]) {
				expect(Object.hasOwn(e, key), `${def.code}.${key}`).toBe(true);
			}
		}
	});

	it("use the category as name and _tag, and its snake case as type", () => {
		const types = [...FOKOS_ERROR_CATEGORIES.entries()].map(([tag, Category]) => {
			const e = new Category(DEFS.find((def) => def.tag === tag)!, { message: "x" });
			expect(e.name).toBe(tag);
			expect(e._tag).toBe(tag);
			return e.type;
		});
		expect(types).toEqual([
			"validation_error",
			"expression_error",
			"conflict_error",
			"unavailable_error",
			"transaction_pending_error",
			"routing_error",
			"internal_error",
		]);
	});

	it("accept only a code definition of their category, and take its code as a literal", () => {
		const e = new FokosValidationError(VALIDATION_CODES.hash_key_empty, { message: "x" });
		const code: "hash_key_empty" = e.code;
		expect(code).toBe("hash_key_empty");
		// @ts-expect-error foreign_error is a code of the internal category
		expect(new FokosValidationError(INTERNAL_CODES.foreign_error, { message: "x" })._tag).toBe("FokosInternalError");
	});

	it("start the message with the code, then the fixed phrase", () => {
		expect(errorOf(VALIDATION_CODES.hash_key_empty).message).toBe("fokos/hash_key_empty: a fixed phrase");
	});

	it("mint an error_id from the segment of the code", () => {
		const a = errorOf(CONFLICT_CODES.item_locked_by_transaction);
		const b = errorOf(CONFLICT_CODES.item_locked_by_transaction);
		expect(a.error_id).toMatch(/^e_vnfeg6_[0-9a-f]{32}$/);
		expect(a.error_id).not.toBe(b.error_id);
	});

	it("keep an error_id that the call site passes", () => {
		expect(errorOf(INTERNAL_CODES.foreign_error, { error_id: "e_jvufz5_abc" }).error_id).toBe("e_jvufz5_abc");
	});

	it("take the origin and the hint from the code definition unless the call site passes others", () => {
		for (const def of DEFS) {
			const e = errorOf(def);
			expect([e.origin, e.httpStatusHint], def.code).toEqual([def.origin, def.httpStatusHint]);
		}
		const e = errorOf(INTERNAL_CODES.foreign_error, { origin: "service", httpStatusHint: 503 });
		expect([e.origin, e.httpStatusHint]).toEqual(["service", 503]);
	});
});

describe("FokosError.is", () => {
	it("holds for every category on the base class", () => {
		for (const def of DEFS) expect(FokosError.is(errorOf(def)), def.code).toBe(true);
	});

	it("holds for its own category only on a category class", () => {
		expect(FokosConflictError.is(errorOf(CONFLICT_CODES.item_locked_by_transaction))).toBe(true);
		expect(FokosConflictError.is(errorOf(VALIDATION_CODES.hash_key_empty))).toBe(false);
		expect(FokosValidationError.is(errorOf(VALIDATION_CODES.hash_key_empty))).toBe(true);
	});

	it("reads own properties, so a plain copy of an error holds as well", () => {
		const copy = Object.assign(new Error("fokos/read_conflict: x"), { ...errorOf(CONFLICT_CODES.read_conflict) });
		expect(FokosError.is(copy)).toBe(true);
		expect(FokosConflictError.is(copy)).toBe(true);
	});

	it("does not hold for a foreign error, a wire record, or a value that is not an object", () => {
		expect(FokosError.is(new Error("x"))).toBe(false);
		expect(FokosError.is(Object.assign(new Error("x"), { _tag: "FokosConflictError", code: "x" }))).toBe(false);
		expect(FokosError.is(FokosError.toWire(errorOf(CONFLICT_CODES.read_conflict)))).toBe(false);
		for (const value of [null, undefined, "FokosConflictError", 42]) expect(FokosError.is(value)).toBe(false);
	});
});

describe("FokosError.isCode", () => {
	it("holds for the code of a definition, and narrows the code to its literal", () => {
		const e: unknown = errorOf(UNAVAILABLE_CODES.partition_migrating);
		expect(FokosError.isCode(e, UNAVAILABLE_CODES.partition_migrating)).toBe(true);
		if (!FokosError.isCode(e, UNAVAILABLE_CODES.partition_migrating)) throw new Error("unreachable");
		const code: "partition_migrating" = e.code;
		const tag: "FokosUnavailableError" = e._tag;
		expect([code, tag]).toEqual(["partition_migrating", "FokosUnavailableError"]);
	});

	it("holds for a plain string, which compares the code only, and narrows the code as well", () => {
		const e: unknown = errorOf(UNAVAILABLE_CODES.partition_migrating);
		expect(FokosError.isCode(e, "partition_migrating")).toBe(true);
		if (!FokosError.isCode(e, "partition_migrating")) throw new Error("unreachable");
		const code: "partition_migrating" = e.code;
		expect(code).toBe("partition_migrating");
	});

	it("does not hold for another code, a code of the same name in another category, or a value that is not a FokosError", () => {
		const e = errorOf(UNAVAILABLE_CODES.partition_migrating);
		const [sameNameOtherCategory] = Object.values(defineCodes("FokosRoutingError", "internal", 500, { partition_migrating: "zzzzzz" }));
		expect(FokosError.isCode(e, UNAVAILABLE_CODES.partition_over_size)).toBe(false);
		expect(FokosError.isCode(e, "partition_over_size")).toBe(false);
		expect(FokosError.isCode(e, sameNameOtherCategory)).toBe(false);
		for (const value of [new Error("partition_migrating"), { code: "partition_migrating" }, undefined, null]) {
			expect(FokosError.isCode(value, UNAVAILABLE_CODES.partition_migrating)).toBe(false);
			expect(FokosError.isCode(value, "partition_migrating")).toBe(false);
		}
	});
});

describe("isFokosAnyError", () => {
	it("holds for every code of the library", () => {
		for (const def of DEFS) expect(isFokosAnyError(errorOf(def)), def.code).toBe(true);
	});

	it("does not hold for a code that the library does not define, or for a code in the wrong category", () => {
		const [otherCode] = Object.values(defineCodes("FokosConflictError", "caller", 409, { other_conflict: "zzzzzz" }));
		expect(isFokosAnyError(errorOf(otherCode))).toBe(false);
		const wrongCategory = { ...errorOf(CONFLICT_CODES.read_conflict), _tag: "FokosValidationError" };
		expect(isFokosAnyError(wrongCategory)).toBe(false);
	});

	it("narrows to the union, so a switch on _tag narrows the code", () => {
		const e: unknown = errorOf(CONFLICT_CODES.item_locked_by_transaction);
		if (!isFokosAnyError(e)) throw new Error("unreachable");
		switch (e._tag) {
			case "FokosConflictError": {
				const code: keyof typeof CONFLICT_CODES = e.code;
				// @ts-expect-error a conflict error never carries a validation code
				const wrong: "hash_key_empty" = e.code;
				expect([code, wrong]).toEqual(["item_locked_by_transaction", "item_locked_by_transaction"]);
				break;
			}
			default:
				throw new Error("unreachable");
		}
	});
});

describe("an extension in another package", () => {
	// A package adds a code to a category of this module, and a category of its own.
	const SHARD_UNAVAILABLE_CODES = defineCodes("FokosUnavailableError", "service", 503, { shard_migrating: "k3m9xz" });
	const SHARD_CODES = defineCodes("FokosShardError", "internal", 500, { shard_moved: "p7q2rs" });
	class FokosShardError<C extends string = string> extends FokosError<"FokosShardError", C> {
		static readonly tag = "FokosShardError";
	}
	type ShardAnyError = FokosAnyError | FokosUnavailableError<"shard_migrating"> | FokosShardError<"shard_moved">;
	const isShardAnyError = defineErrorGuard<ShardAnyError>(...FOKOS_CODE_TABLES, SHARD_UNAVAILABLE_CODES, SHARD_CODES);

	const migrating = new FokosUnavailableError(SHARD_UNAVAILABLE_CODES.shard_migrating, { message: "shard is migrating" });
	const moved = new FokosShardError(SHARD_CODES.shard_moved, { message: "shard moved" });

	it("raises errors that every guard of this module reads", () => {
		for (const e of [migrating, moved]) expect(FokosError.is(e)).toBe(true);
		expect(FokosUnavailableError.is(migrating)).toBe(true);
		expect(FokosShardError.is(moved)).toBe(true);
		expect([migrating.origin, migrating.httpStatusHint, moved.origin, moved.httpStatusHint]).toEqual(["service", 503, "internal", 500]);
	});

	it("is claimed by the guard of its own union, and not by the guard of this library", () => {
		expect([isFokosAnyError(migrating), isFokosAnyError(moved)]).toEqual([false, false]);
		expect([isShardAnyError(migrating), isShardAnyError(moved)]).toEqual([true, true]);
		expect(isShardAnyError(errorOf(UNAVAILABLE_CODES.partition_migrating))).toBe(true);
	});

	it("narrows a switch over its union to the codes of both packages", () => {
		const e: unknown = migrating;
		if (!isShardAnyError(e)) throw new Error("unreachable");
		switch (e._tag) {
			case "FokosUnavailableError": {
				const code: "partition_over_size" | "partition_migrating" | "coordinator_over_size" | "prepare_unanswered" | "shard_migrating" =
					e.code;
				expect(code).toBe("shard_migrating");
				break;
			}
			case "FokosShardError": {
				const code: "shard_moved" = e.code;
				expect(code).toBe("unreachable");
				break;
			}
			default:
				throw new Error("unreachable");
		}
	});

	it("keeps the tag and the fields of a category that fromWire does not know", () => {
		for (const input of [FokosError.toWire(moved), Object.assign(new Error(moved.message), { ...moved })]) {
			const back = FokosError.fromWire(input);
			expect(contractOf(back)).toEqual(contractOf(moved));
			expect(FokosShardError.is(back)).toBe(true);
			expect(isShardAnyError(back)).toBe(true);
		}
	});
});

describe("FokosError.wrap", () => {
	it("returns a FokosError unchanged", () => {
		const e = errorOf(UNAVAILABLE_CODES.partition_migrating);
		expect(FokosError.wrap(e)).toBe(e);
	});

	it("wraps a foreign error as foreign_error, keeps it as cause, and copies its own properties", () => {
		const foreign = Object.assign(new Error("boom"), { remote: true, detail: { n: 1 } });
		const e = FokosError.wrap(foreign);
		expect(e).toBeInstanceOf(FokosInternalError);
		expect([e.code, e.origin, e.httpStatusHint]).toEqual(["foreign_error", "internal", 500]);
		expect(e.cause).toBe(foreign);
		expect(e.attributes).toEqual({ remote: true, detail: { n: 1 } });
		expect(e.message).toBe("fokos/foreign_error: unexpected error occurred");
	});

	it("drops the properties that cannot cross an RPC hop and keeps the rest", () => {
		const foreign = Object.assign(new Error("boom"), {
			fn: () => 1,
			nested: { deep: { fn: () => 1 } },
			stream: new ReadableStream(),
			bytes: new Uint8Array([1, 2]),
			map: new Map([["k", 1]]),
		});
		const e = FokosError.wrap(foreign);
		expect(e.attributes).toEqual({ bytes: new Uint8Array([1, 2]), map: new Map([["k", 1]]) });
	});

	it("gives a foreign error that the runtime marks retryable the service origin", () => {
		const e = FokosError.wrap(Object.assign(new Error("overloaded"), { retryable: true, overloaded: true }));
		expect([e.code, e.origin, e.httpStatusHint]).toEqual(["foreign_error", "service", 503]);
		expect(e.attributes).toEqual({ retryable: true, overloaded: true });
	});

	it("wraps a value that is not an object", () => {
		const e = FokosError.wrap("a thrown string");
		expect([e.code, e.cause, e.attributes]).toEqual(["foreign_error", "a thrown string", {}]);
	});
});

describe("FokosError.toWire and FokosError.fromWire", () => {
	it("round-trip every category without loss", () => {
		for (const def of DEFS) {
			const e = errorOf(def, { attributes: { hashKey: "hk", n: 1 } });
			const wire = FokosError.toWire(e);
			expect(JSON.parse(JSON.stringify(wire)), def.code).toEqual(wire);
			const back = FokosError.fromWire(wire);
			expect(back).toBeInstanceOf(FOKOS_ERROR_CATEGORIES.get(e._tag)!);
			expect(contractOf(back), def.code).toEqual(contractOf(e));
		}
	});

	it("store the cause as plain data", () => {
		const inner = Object.assign(new Error("inner"), { status: 7 });
		const wire = FokosError.toWire(errorOf(INTERNAL_CODES.partition_fanout_failed, { cause: inner }));
		expect(wire.cause).toEqual({ error: "Error: inner", errorProps: { status: 7 } });
		expect(FokosError.fromWire(wire).cause).toEqual(wire.cause);
	});

	it("store a primitive cause without its characters", () => {
		expect(FokosError.toWire(FokosError.wrap("ab")).cause).toEqual({ error: "ab", errorProps: {} });
	});

	it("wrap a foreign value before it goes on the wire", () => {
		const wire = FokosError.toWire(new Error("boom"));
		expect([wire.name, wire.code, wire.cause?.error]).toEqual(["FokosInternalError", "foreign_error", "Error: boom"]);
	});

	it("rebuild the class from an error that has no prototype", () => {
		const e = errorOf(DEFS.find((def) => def.code === "transaction_undecided")!, { cause: new Error("inner") });
		const copy = Object.assign(new Error(e.message), { ...e, cause: e.cause });
		const back = FokosError.fromWire(copy);
		expect(back).toBeInstanceOf(FOKOS_ERROR_CATEGORIES.get("FokosTransactionPendingError")!);
		expect(contractOf(back)).toEqual(contractOf(e));
		expect(back.cause).toBe(e.cause);
	});
});

describe("isRuntimeRetryableError", () => {
	it("reads the runtime markers on a raw error, and in the attributes after wrap", () => {
		const transient = Object.assign(new Error("transient"), { retryable: true });
		const overloaded = Object.assign(new Error("overloaded"), { retryable: true, overloaded: true });
		expect([isRuntimeRetryableError(transient), isRuntimeRetryableError(FokosError.wrap(transient))]).toEqual([true, true]);
		expect([isRuntimeRetryableError(overloaded), isRuntimeRetryableError(FokosError.wrap(overloaded))]).toEqual([false, false]);
	});

	it("does not hold for an error without the marker, or for a value that is not an object", () => {
		for (const e of [new Error("x"), errorOf(UNAVAILABLE_CODES.partition_migrating), undefined, null, "retryable"]) {
			expect(isRuntimeRetryableError(e)).toBe(false);
		}
	});
});
