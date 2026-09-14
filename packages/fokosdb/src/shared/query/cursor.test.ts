import { describe, expect, it } from "vitest";
import { KeyCodec } from "../partition-topology/key-codec.js";
import { computeCursorFingerprint } from "./cursor.js";

const queries = () => [
	{
		hashKey: KeyCodec.encode("hk"),
		interval: { lower: { value: KeyCodec.encode("a"), inclusive: true }, upper: { value: KeyCodec.encode("z"), inclusive: false } },
		direction: "asc" as const,
	},
];

describe("computeCursorFingerprint", () => {
	it("produces equal fingerprints for equal queries and equal identities", () => {
		expect(computeCursorFingerprint(queries(), "f", "p")).toBe(computeCursorFingerprint(queries(), "f", "p"));
		expect(computeCursorFingerprint(queries())).toBe(computeCursorFingerprint(queries()));
	});

	it("differs when the projection identity differs", () => {
		expect(computeCursorFingerprint(queries(), null, "p1")).not.toBe(computeCursorFingerprint(queries(), null, "p2"));
	});

	it("differs between an absent filter and an absent projection identity", () => {
		expect(computeCursorFingerprint(queries(), "x", null)).not.toBe(computeCursorFingerprint(queries(), null, "x"));
	});

	it("both null identities keep the legacy fingerprint bytes", () => {
		const legacy = computeCursorFingerprint(queries());
		expect(computeCursorFingerprint(queries(), null, null)).toBe(legacy);
		expect(computeCursorFingerprint(queries(), "f", null)).not.toBe(legacy);
		expect(computeCursorFingerprint(queries(), null, "p")).not.toBe(legacy);
	});
});
