import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { FOKOS_CODE_TABLES, FOKOS_ERROR_CATEGORIES, FokosConflictError, FokosError, FokosValidationError } from "../src/shared/errors.js";
import { isFokosAnyError, type FokosAnyError, type FokosErrorCode } from "../src/shared/errors-operations.js";
import { CODE_DEFS } from "./worker-entry.js";

/**
 * What a FokosError keeps when it crosses a Workers RPC boundary.
 *
 * The library classifies on data, never on the prototype. These tests pin the facts that decision rests
 * on: every own property survives the hop, the prototype does not, and the static guards work on the
 * far side because they read own properties only. They must fail if a runtime change stops an own
 * property from crossing, which happens on a compatibility date before 2026-04-21.
 */

function probeStub(name: string) {
	return env.ERROR_PROBE_DO.getByName(name);
}

async function catchOverRpc(name: string, call: (stub: ReturnType<typeof probeStub>) => Promise<never>): Promise<FokosAnyError> {
	try {
		await call(probeStub(name));
	} catch (e) {
		return e as FokosAnyError;
	}
	throw new Error("the probe did not throw");
}

/** One code of each category. */
const ONE_CODE_PER_CATEGORY = FOKOS_CODE_TABLES.map((table) => Object.keys(table)[0] as FokosErrorCode);

describe("a FokosError across an RPC hop", () => {
	it("keeps the category, the code, the error_id, the origin, the hint and the attributes of every category", async () => {
		for (const code of ONE_CODE_PER_CATEGORY) {
			const err = await catchOverRpc(`fields-${code}`, (s) => s.raise(code, { hashKey: "hk", keyBytes: new Uint8Array([1, 2]) }));
			const { tag, segment, origin, httpStatusHint } = CODE_DEFS[code];

			expect(err).toBeInstanceOf(Error);
			for (const key of ["name", "_tag", "type", "code", "error_id", "origin", "httpStatusHint", "attributes"]) {
				expect(Object.hasOwn(err, key), `${code}.${key}`).toBe(true);
			}
			expect(err.name).toBe(tag);
			expect(err._tag).toBe(tag);
			expect(err.code).toBe(code);
			expect(err.error_id).toMatch(new RegExp(`^e_${segment}_[0-9a-f]{32}$`));
			expect([err.origin, err.httpStatusHint]).toEqual([origin, httpStatusHint]);
			expect(err.attributes).toEqual({ hashKey: "hk", keyBytes: new Uint8Array([1, 2]) });
			expect(err.message).toBe(`fokos/${code}: probe failed`);
		}
	});

	it("keeps cause, which is a non-enumerable own property", async () => {
		const err = await catchOverRpc("cause", (s) => s.raiseWithCause());

		expect(err.code).toBe("partition_fanout_failed");
		expect((err.cause as Error).message).toBe("inner");
	});

	it("drops the prototype, so instanceof fails", async () => {
		const err = await catchOverRpc("drops-prototype", (s) => s.raise("item_locked_by_transaction", {}));

		expect(err).toBeInstanceOf(Error);
		expect(err instanceof FokosConflictError).toBe(false);
		expect(err instanceof FokosError).toBe(false);
	});

	it("is still recognised by the static guards, which read own properties", async () => {
		const err = await catchOverRpc("guards", (s) => s.raise("item_locked_by_transaction", {}));

		expect(FokosError.is(err)).toBe(true);
		expect(isFokosAnyError(err)).toBe(true);
		expect(FokosConflictError.is(err)).toBe(true);
		expect(FokosValidationError.is(err)).toBe(false);
	});

	it("is returned unchanged by wrap, so its error_id survives", async () => {
		const err = await catchOverRpc("wrap", (s) => s.raise("partition_migrating", {}));

		expect(FokosError.wrap(err)).toBe(err);
	});

	it("round-trips through toWire and fromWire, and fromWire builds the class again", async () => {
		for (const code of ONE_CODE_PER_CATEGORY) {
			const err = await catchOverRpc(`wire-${code}`, (s) => s.raise(code, { n: 1 }));

			const back = FokosError.fromWire(err);
			expect(back).toBeInstanceOf(FOKOS_ERROR_CATEGORIES.get(err._tag)!);
			const fromRecord = FokosError.fromWire(FokosError.toWire(err));
			for (const e of [back, fromRecord]) {
				expect([e.name, e._tag, e.type, e.code, e.error_id, e.origin, e.httpStatusHint, e.attributes, e.message]).toEqual([
					err.name,
					err._tag,
					err.type,
					err.code,
					err.error_id,
					err.origin,
					err.httpStatusHint,
					err.attributes,
					err.message,
				]);
			}
		}
	});

	// The middle node wraps what it receives. A wrap would mint a foreign_error id, so the read_conflict
	// segment proves that the error crossed both hops as the first node raised it.
	it("crosses a second hop unchanged, with the error_id that the first node minted", async () => {
		const relayed = await catchOverRpc("relay", (s) => s.relay("relay-target", "read_conflict"));

		expect(FokosConflictError.is(relayed)).toBe(true);
		expect(relayed.code).toBe("read_conflict");
		expect(relayed.error_id).toMatch(/^e_wx4mnz_[0-9a-f]{32}$/);
		expect(relayed.cause).toBeUndefined();
	});
});

describe("a foreign error across an RPC hop", () => {
	// The runtime adds `remote: true` to an error that crossed a hop.
	it("keeps the runtime markers as own properties, and wrap moves them into attributes", async () => {
		const raw = await catchOverRpc("foreign", (s) => s.raiseForeign());
		expect(FokosError.is(raw)).toBe(false);

		const err = FokosError.wrap(raw);
		expect([err.code, err.origin, err.httpStatusHint]).toEqual(["foreign_error", "service", 503]);
		expect(err.attributes).toEqual({ retryable: true, overloaded: false, remote: true });
		expect(err.cause).toBe(raw);
	});
});
