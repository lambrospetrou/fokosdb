import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { ProbeError } from "./worker-entry.js";

/**
 * What a tagged error keeps when it crosses a Workers RPC boundary.
 *
 * The error handling design classifies on data, never on the prototype. These tests pin the two facts
 * that decision rests on: every own property survives the hop, and every prototype member does not.
 * A guard that reads `_tag` therefore works on the far side, and `instanceof` never does.
 */

function probeStub(name: string) {
	return env.ERROR_PROBE_DO.getByName(name);
}

async function catchOverRpc(name: string, call: (stub: ReturnType<typeof probeStub>) => Promise<never>): Promise<unknown> {
	try {
		await call(probeStub(name));
	} catch (e) {
		return e;
	}
	throw new Error("the probe did not throw");
}

/** The shape of the guard the library ships. It reads own properties only. */
function isProbeError(e: unknown): boolean {
	return e instanceof Error && (e as { _tag?: unknown })._tag === ProbeError.tag;
}

describe("a tagged error in the same isolate", () => {
	const local = new ProbeError({ message: "probe failed", code: "probe_code", errorId: "e_abc123_deadbeef" });

	it("is a real Error subclass with the right prototype", () => {
		expect(local).toBeInstanceOf(Error);
		expect(local).toBeInstanceOf(ProbeError);
	});

	it("assigns the tag and every field as own properties", () => {
		for (const key of ["name", "_tag", "code", "errorId"]) {
			expect(Object.hasOwn(local, key)).toBe(true);
		}
		expect(local.name).toBe("ProbeError");
		expect(local._tag).toBe("ProbeError");
	});

	it("keeps the wire converter on the prototype, holding no data", () => {
		expect(Object.hasOwn(local, "toWire")).toBe(false);
		expect(local.toWire()).toEqual({ name: "ProbeError", code: "probe_code", errorId: "e_abc123_deadbeef" });
	});
});

describe("a tagged error across an RPC hop", () => {
	it("keeps name, message and every payload field", async () => {
		const err = (await catchOverRpc("keeps-fields", (s) => s.raiseTagged())) as Error & Record<string, unknown>;

		expect(err).toBeInstanceOf(Error);
		expect(err.name).toBe("ProbeError");
		expect(err.message).toBe("probe failed");
		expect(err._tag).toBe("ProbeError");
		expect(err.code).toBe("probe_code");
		expect(err.errorId).toBe("e_abc123_deadbeef");
	});

	it("keeps cause, which is a non-enumerable own property", async () => {
		const err = (await catchOverRpc("cause", (s) => s.raiseWithNestedCause())) as Error;

		expect(err.name).toBe("ProbeError");
		expect((err.cause as Error).message).toBe("inner");
	});

	it("drops the prototype, so instanceof fails", async () => {
		const err = await catchOverRpc("drops-prototype", (s) => s.raiseTagged());

		expect(err).toBeInstanceOf(Error);
		expect(err instanceof ProbeError).toBe(false);
	});

	// Why a results entry must be plain data and never an Error instance: the first hop strips the
	// converter, so a later hop cannot serialize the error again.
	it("drops every prototype member, the wire converter included", async () => {
		const err = (await catchOverRpc("drops-methods", (s) => s.raiseTagged())) as Record<string, unknown>;

		expect(typeof err.toWire).toBe("undefined");
	});

	it("is still recognised by a guard that reads the own tag", async () => {
		const err = await catchOverRpc("tag-guard", (s) => s.raiseTagged());

		expect(isProbeError(err)).toBe(true);
	});
});
