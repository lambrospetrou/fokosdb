import { describe, expect, it } from "vitest";
import {
	FOKOS_ERROR_CATEGORIES,
	FOKOS_ERROR_REGISTRY,
	FokosConflictError,
	FokosError,
	FokosInternalError,
	FokosValidationError,
	type FokosAnyError,
	type FokosErrorCode,
	type FokosErrorInit,
} from "./errors.js";

const CODES = Object.keys(FOKOS_ERROR_REGISTRY) as FokosErrorCode[];

/** One error of `code`, built through the class of its category. */
function errorOf(code: FokosErrorCode, init: Partial<FokosErrorInit<string>> = {}): FokosAnyError {
	const Category = FOKOS_ERROR_CATEGORIES.get(FOKOS_ERROR_REGISTRY[code].tag)!;
	return new Category({ code, message: "a fixed phrase", ...init });
}

/** The fields that make up the contract of an error. */
function contractOf(e: FokosAnyError) {
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

describe("the code registry", () => {
	it("gives every code a unique segment", () => {
		const segments = CODES.map((code) => FOKOS_ERROR_REGISTRY[code].segment);
		expect(new Set(segments).size).toBe(segments.length);
	});

	it("takes every segment from the unambiguous alphabet", () => {
		for (const code of CODES) {
			expect(FOKOS_ERROR_REGISTRY[code].segment, code).toMatch(/^[a-hjkmnp-z2-9]{6}$/);
		}
	});

	it("puts every code in a known category, and gives every category a code", () => {
		const tags = new Set(CODES.map((code) => FOKOS_ERROR_REGISTRY[code].tag));
		expect([...tags].sort()).toEqual([...FOKOS_ERROR_CATEGORIES.keys()].sort());
	});
});

describe("the category classes", () => {
	it("declare no prototype member", () => {
		for (const Category of [FokosError, ...FOKOS_ERROR_CATEGORIES.values()]) {
			expect(Object.getOwnPropertyNames(Category.prototype), Category.name).toEqual(["constructor"]);
		}
	});

	it("assign every field as an own property", () => {
		for (const code of CODES) {
			const e = errorOf(code, { cause: new Error("inner") });
			for (const key of ["name", "message", "_tag", "type", "code", "error_id", "origin", "httpStatusHint", "attributes", "cause"]) {
				expect(Object.hasOwn(e, key), `${code}.${key}`).toBe(true);
			}
		}
	});

	it("use the category as name and _tag, and its snake case as type", () => {
		const types = [...FOKOS_ERROR_CATEGORIES.entries()].map(([tag, Category]) => {
			const code = CODES.find((c) => FOKOS_ERROR_REGISTRY[c].tag === tag)!;
			const e = new Category({ code, message: "x" });
			expect(e.name).toBe(tag);
			expect(e._tag).toBe(tag);
			return e.type;
		});
		expect(types).toEqual([
			"validation_error",
			"expression_error",
			"condition_check_error",
			"conflict_error",
			"transaction_cancelled_error",
			"unavailable_error",
			"transaction_pending_error",
			"routing_error",
			"internal_error",
		]);
	});

	it("accept only the codes of their category", () => {
		expect(new FokosValidationError({ code: "hash_key_empty", message: "x" }).code).toBe("hash_key_empty");
		// @ts-expect-error foreign_error is an internal code, not a validation code
		expect(new FokosValidationError({ code: "foreign_error", message: "x" }).code).toBe("foreign_error");
	});

	it("start the message with the code, then the fixed phrase", () => {
		expect(errorOf("hash_key_empty").message).toBe("fokos/hash_key_empty: a fixed phrase");
	});

	it("mint an error_id from the segment of the code", () => {
		const a = errorOf("item_locked_by_transaction");
		const b = errorOf("item_locked_by_transaction");
		expect(a.error_id).toMatch(/^e_vnfeg6_[0-9a-f]{32}$/);
		expect(a.error_id).not.toBe(b.error_id);
	});

	it("keep an error_id that the call site passes", () => {
		expect(errorOf("foreign_error", { error_id: "e_jvufz5_abc" }).error_id).toBe("e_jvufz5_abc");
	});

	it("take the origin and the hint from the registry unless the call site passes others", () => {
		for (const code of CODES) {
			const e = errorOf(code);
			expect(e.origin, code).toBe(FOKOS_ERROR_REGISTRY[code].origin);
			expect(e.httpStatusHint, code).toBe(FOKOS_ERROR_REGISTRY[code].httpStatusHint);
		}
		const e = errorOf("foreign_error", { origin: "service", httpStatusHint: 503 });
		expect([e.origin, e.httpStatusHint]).toEqual(["service", 503]);
	});

	it("give clock_skew the service origin, not the one of its category", () => {
		expect([FOKOS_ERROR_REGISTRY.clock_skew.origin, FOKOS_ERROR_REGISTRY.clock_skew.httpStatusHint]).toEqual(["service", 503]);
		expect([FOKOS_ERROR_REGISTRY.read_conflict.origin, FOKOS_ERROR_REGISTRY.read_conflict.httpStatusHint]).toEqual(["caller", 409]);
	});
});

describe("FokosError.is", () => {
	it("holds for every category on the base class", () => {
		for (const code of CODES) expect(FokosError.is(errorOf(code)), code).toBe(true);
	});

	it("holds for its own category only on a category class", () => {
		expect(FokosConflictError.is(errorOf("item_locked_by_transaction"))).toBe(true);
		expect(FokosConflictError.is(errorOf("hash_key_empty"))).toBe(false);
		expect(FokosValidationError.is(errorOf("hash_key_empty"))).toBe(true);
	});

	it("reads own properties, so a plain copy of an error holds as well", () => {
		const copy = Object.assign(new Error("fokos/read_conflict: x"), { ...errorOf("read_conflict") });
		expect(FokosError.is(copy)).toBe(true);
		expect(FokosConflictError.is(copy)).toBe(true);
	});

	it("does not hold for a foreign error, a wire record, or a value that is not an object", () => {
		expect(FokosError.is(new Error("x"))).toBe(false);
		expect(FokosError.is(Object.assign(new Error("x"), { _tag: "SomeOtherError", code: "x" }))).toBe(false);
		expect(FokosError.is(FokosError.toWire(errorOf("read_conflict")))).toBe(false);
		for (const value of [null, undefined, "FokosConflictError", 42]) expect(FokosError.is(value)).toBe(false);
	});

	it("narrows to the category of the class, and a switch on _tag narrows the code", () => {
		const e: unknown = errorOf("item_locked_by_transaction");
		if (FokosConflictError.is(e)) {
			const code:
				| "item_locked_by_transaction"
				| "timestamp_conflict"
				| "pending_conflict"
				| "read_conflict"
				| "pending_write"
				| "clock_skew" = e.code;
			// @ts-expect-error a conflict error never carries a validation code
			const wrong: "hash_key_empty" = e.code;
			expect([code, wrong]).toEqual(["item_locked_by_transaction", "item_locked_by_transaction"]);
		}
		if (FokosError.is(e)) {
			switch (e._tag) {
				case "FokosConflictError":
					expect(e.code).toBe("item_locked_by_transaction");
					break;
				default:
					throw new Error("unreachable");
			}
		}
	});
});

describe("FokosError.wrap", () => {
	it("returns a FokosError unchanged", () => {
		const e = errorOf("partition_migrating");
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
		for (const code of CODES) {
			const e = errorOf(code, { attributes: { hashKey: "hk", n: 1 } });
			const wire = FokosError.toWire(e);
			expect(JSON.parse(JSON.stringify(wire)), code).toEqual(wire);
			const back = FokosError.fromWire(wire);
			expect(back).toBeInstanceOf(FOKOS_ERROR_CATEGORIES.get(e._tag)!);
			expect(contractOf(back), code).toEqual(contractOf(e));
		}
	});

	it("store the cause as plain data", () => {
		const inner = Object.assign(new Error("inner"), { status: 7 });
		const wire = FokosError.toWire(errorOf("partition_fanout_failed", { cause: inner }));
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
		const e = errorOf("transaction_undecided", { cause: new Error("inner") });
		const copy = Object.assign(new Error(e.message), { ...e, cause: e.cause });
		const back = FokosError.fromWire(copy as FokosAnyError);
		expect(back).toBeInstanceOf(FOKOS_ERROR_CATEGORIES.get("FokosTransactionPendingError")!);
		expect(contractOf(back)).toEqual(contractOf(e));
		expect(back.cause).toBe(e.cause);
	});
});
