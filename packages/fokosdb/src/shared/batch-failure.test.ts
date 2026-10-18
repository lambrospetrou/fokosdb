import { describe, expect, it } from "vitest";
import { attachRouting } from "../sharding/envelope.js";
import { SHARDING_UNAVAILABLE_CODES } from "../sharding/errors.js";
import { classifyBatchFailure } from "./batch-failure.js";
import {
	CORE_INTERNAL_CODES,
	EXPRESSION_CODES,
	FokosError,
	FokosExpressionError,
	FokosInternalError,
	FokosUnavailableError,
	FokosValidationError,
	VALIDATION_CODES,
} from "./errors.js";

const ROUTING = {
	servedBy: [
		{
			ref: { partitionId: "00000000", doName: "p~h.0" },
			actorId: "a".repeat(64),
			hashDepth: 0,
			rangeDepth: 0,
			role: "executed" as const,
		},
	],
	forwardCount: 0,
	servedByTruncated: false,
};

const routed = <E extends FokosError>(e: E): E => attachRouting(e, ROUTING) as unknown as E;
const unavailable = () => new FokosUnavailableError(SHARDING_UNAVAILABLE_CODES.partition_migrating, { message: "x" });
const internal = () => new FokosInternalError(CORE_INTERNAL_CODES.invariant_failed, { message: "x" });

describe("classifyBatchFailure", () => {
	it("rejects an RPC value above 32 MiB with batch_request_too_large, also as a cause", () => {
		const rpc = new Error(
			"Serialized RPC arguments or return values are limited to 32MiB, but the size of this value was: 40000000 bytes.",
		);
		for (const error of [rpc, new Error("outer", { cause: rpc })]) {
			const result = classifyBatchFailure(error, { write: true, local: false });
			expect(result).toMatchObject({ outcome: "rejected", code: VALIDATION_CODES.batch_request_too_large.code });
			expect(result.error_id).toMatch(/^e_nhncs4_/);
		}
	});

	it("rejects a validation or an expression error, wherever it lands", () => {
		const validation = new FokosValidationError(VALIDATION_CODES.item_too_large, { message: "x" });
		const expression = new FokosExpressionError(EXPRESSION_CODES.expression_invalid, { message: "x" });
		for (const opts of [
			{ write: true, local: true },
			{ write: true, local: false },
			{ write: false, local: false },
		]) {
			expect(classifyBatchFailure(routed(validation), opts)).toEqual({
				outcome: "rejected",
				code: "item_too_large",
				error_id: validation.error_id,
			});
			expect(classifyBatchFailure(expression, opts).outcome).toBe("rejected");
		}
	});

	it("gives unprocessed to an error of the local part, an internal error included", () => {
		expect(classifyBatchFailure(internal(), { write: true, local: true }).outcome).toBe("unprocessed");
		expect(classifyBatchFailure(new Error("boom"), { write: true, local: true })).toMatchObject({
			outcome: "unprocessed",
			code: CORE_INTERNAL_CODES.foreign_error.code,
		});
	});

	it("gives unprocessed to a remote error with routing, except an internal error of a write", () => {
		expect(classifyBatchFailure(routed(unavailable()), { write: true, local: false })).toMatchObject({
			outcome: "unprocessed",
			code: "partition_migrating",
		});
		expect(classifyBatchFailure(routed(internal()), { write: true, local: false }).outcome).toBe("unknown");
		expect(classifyBatchFailure(routed(internal()), { write: false, local: false }).outcome).toBe("unprocessed");
	});

	it("gives unknown to a write and unprocessed to a read for an error with no routing", () => {
		expect(classifyBatchFailure(new Error("Network connection lost."), { write: true, local: false }).outcome).toBe("unknown");
		expect(classifyBatchFailure(unavailable(), { write: true, local: false }).outcome).toBe("unknown");
		expect(classifyBatchFailure(new Error("Network connection lost."), { write: false, local: false }).outcome).toBe("unprocessed");
	});
});
