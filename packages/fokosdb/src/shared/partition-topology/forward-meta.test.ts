import { describe, expect, it } from "vitest";
import { CONFLICT_CODES, FokosConflictError } from "../errors.js";
import invariant from "../invariant.js";
import { KeyCodec } from "./key-codec.js";
import { forwardedMeta, learnFromErrorMeta, routedError, stampRoutingMeta } from "./forward-meta.js";
import type { PartitionInfoInternal } from "./types.js";

const meta: PartitionInfoInternal = {
	servedByActorId: "actor",
	servedByActorName: "leaf",
	servedByPartitionId: "01",
	forwardCount: 1,
	hashDepth: 2,
	rangeDepth: 0,
	_internal: { rangeAncestors: [{ depth: 1, startBoundary: KeyCodec.encode("a"), endBoundary: KeyCodec.encode("m") }] },
};

const lockedError = () =>
	new FokosConflictError(CONFLICT_CODES.item_locked_by_transaction, { message: "locked", attributes: { hashKey: "hk" } });

describe("forwardedMeta", () => {
	it("adds one forward and keeps every other field", () => {
		expect(forwardedMeta(meta)).toEqual({ ...meta, forwardCount: 2 });
	});

	it("sets the hash depth of the forwarding partition when one is given", () => {
		expect(forwardedMeta(meta, 1)).toEqual({ ...meta, forwardCount: 2, hashDepth: 1 });
	});
});

describe("routedError", () => {
	it("finds the meta that stampRoutingMeta attached, as an own property", () => {
		const err = lockedError();
		stampRoutingMeta(err, meta);

		expect(routedError(err)).toBe(err);
		expect(Object.hasOwn(err, "meta")).toBe(true);
	});

	it("is undefined for an error without a routing meta, and for a value that is not a FokosError", () => {
		for (const e of [lockedError(), new Error("plain"), { meta }, "a string error", undefined]) {
			expect(routedError(e)).toBeUndefined();
		}
	});
});

describe("learnFromErrorMeta", () => {
	it("learns from the routing meta, then applies the same change as the success path", () => {
		const err = stampRoutingMeta(lockedError(), meta);
		const learned: PartitionInfoInternal[] = [];

		learnFromErrorMeta(err, (m) => learned.push(m), 1);

		expect(learned).toEqual([meta]);
		expect(err.meta).toEqual(forwardedMeta(meta, 1));
	});

	it("does nothing for an error without a routing meta", () => {
		const err = lockedError();
		let calls = 0;

		learnFromErrorMeta(err, () => calls++);

		expect(calls).toBe(0);
		expect(Object.hasOwn(err, "meta")).toBe(false);
	});

	it("swallows a failed learning, so the original error keeps its identity and still gets the meta change", () => {
		const err = stampRoutingMeta(lockedError(), meta);
		const { error_id, code, attributes } = err;

		expect(() => learnFromErrorMeta(err, (m) => invariant(m.hashDepth > 5, "the learning trips an invariant"))).not.toThrow();

		expect(err).toMatchObject({ error_id, code, attributes });
		expect(err.meta).toEqual(forwardedMeta(meta));
	});
});
