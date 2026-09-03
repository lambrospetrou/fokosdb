import { describe, expect, it } from "vitest";
import { EXPRESSION_LIMITS } from "./limits.js";
import { isParentPath, pathsEqual, validateReadJsonPath, validateWriteJsonPath } from "./path.js";
import { validateUpdateExpression } from "./semantic.js";
import type { UpdateAction } from "./types.js";

describe("write JSON path validator", () => {
	it("parses path segments for members and array indices", () => {
		const segments = validateWriteJsonPath("$.profile.address[0]");
		expect(segments).toEqual([
			{ kind: "member", key: "profile" },
			{ kind: "member", key: "address" },
			{ kind: "index", index: 0 },
		]);
	});

	it("parses quoted member labels with escapes", () => {
		const segments = validateWriteJsonPath('$."user.name"');
		expect(segments).toEqual([{ kind: "member", key: "user.name" }]);
	});

	it("parses reverse array indices", () => {
		const segments = validateWriteJsonPath("$.items[#-2]");
		expect(segments).toEqual([
			{ kind: "member", key: "items" },
			{ kind: "reverseIndex", offset: 2 },
		]);
	});

	it("accepts array append selector only when allowAppend is true", () => {
		expect(() => validateWriteJsonPath("$.items[#]", { allowAppend: true })).not.toThrow();
		const segments = validateWriteJsonPath("$.items[#]", { allowAppend: true });
		expect(segments).toEqual([{ kind: "member", key: "items" }, { kind: "append" }]);

		expect(() => validateWriteJsonPath("$.items[#]", { allowAppend: false })).toThrow(/invalid.*path/i);
		expect(() => validateWriteJsonPath("$.items[#]")).toThrow(/invalid.*path/i);
	});

	it("rejects append selector followed by further selectors", () => {
		expect(() => validateWriteJsonPath("$.items[#].foo", { allowAppend: true })).toThrow(/invalid.*path/i);
	});

	it("keeps read path validator rejecting append selector", () => {
		expect(() => validateReadJsonPath("$.items[#]")).toThrow();
	});

	it("detects parent paths correctly", () => {
		const parent = validateWriteJsonPath("$.a");
		const child = validateWriteJsonPath("$.a.b");
		const sibling = validateWriteJsonPath("$.ab");

		expect(isParentPath(parent, child)).toBe(true);
		expect(isParentPath(child, parent)).toBe(false);
		expect(isParentPath(parent, sibling)).toBe(false);
	});

	it("detects equal paths", () => {
		const pathA = validateWriteJsonPath("$.items[0]");
		const pathB = validateWriteJsonPath("$.items[0]");
		const pathC = validateWriteJsonPath("$.items[1]");

		expect(pathsEqual(pathA, pathB)).toBe(true);
		expect(pathsEqual(pathA, pathC)).toBe(false);
	});
});

describe("update expression validation", () => {
	it("accepts valid set and remove actions", () => {
		const update: readonly UpdateAction[] = [
			{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } },
			{ action: "remove", target: { ref: "data", path: "$.tempToken" } },
		];
		const analysis = validateUpdateExpression(update);
		expect(analysis.requiredColumns).toContain("data_kind");
		expect(analysis.requiredColumns).toContain("data");
	});

	it("accepts append target for set action", () => {
		const update: readonly UpdateAction[] = [{ action: "set", target: { ref: "data", path: "$.tags[#]" }, value: { val: "vip" } }];
		expect(() => validateUpdateExpression(update)).not.toThrow();
	});

	it("accepts sibling paths like $.a and $.ab", () => {
		const update: readonly UpdateAction[] = [
			{ action: "set", target: { ref: "data", path: "$.a" }, value: { val: 1 } },
			{ action: "set", target: { ref: "data", path: "$.ab" }, value: { val: 2 } },
		];
		expect(() => validateUpdateExpression(update)).not.toThrow();
	});

	it("accepts independent array index targets", () => {
		const update: readonly UpdateAction[] = [
			{ action: "set", target: { ref: "data", path: "$.items[0]" }, value: { val: "first" } },
			{ action: "set", target: { ref: "data", path: "$.items[1]" }, value: { val: "second" } },
		];
		expect(() => validateUpdateExpression(update)).not.toThrow();
	});

	it("accepts removal of plain index and set on reverse index under same parent", () => {
		const update: readonly UpdateAction[] = [
			{ action: "remove", target: { ref: "data", path: "$.items[0]" } },
			{ action: "set", target: { ref: "data", path: "$.items[#-1]" }, value: { val: "last" } },
		];
		expect(() => validateUpdateExpression(update)).not.toThrow();
	});

	it("rejects root target path $", () => {
		expect(() => validateUpdateExpression([{ action: "set", target: { ref: "data", path: "$" }, value: { val: 1 } }])).toThrow(/root/i);
	});

	it("rejects duplicate target paths", () => {
		const update: readonly UpdateAction[] = [
			{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "a" } },
			{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "b" } },
		];
		expect(() => validateUpdateExpression(update)).toThrow(/duplicate/i);
	});

	it("rejects overlapping parent and child target paths", () => {
		const update: readonly UpdateAction[] = [
			{ action: "set", target: { ref: "data", path: "$.user" }, value: { val: 1 } },
			{ action: "set", target: { ref: "data", path: "$.user.name" }, value: { val: "Alice" } },
		];
		expect(() => validateUpdateExpression(update)).toThrow(/overlapping/i);
	});

	it("rejects overlapping array parent and element target paths", () => {
		const update: readonly UpdateAction[] = [
			{ action: "set", target: { ref: "data", path: "$.items" }, value: { val: 1 } },
			{ action: "set", target: { ref: "data", path: "$.items[0]" }, value: { val: 1 } },
		];
		expect(() => validateUpdateExpression(update)).toThrow(/overlapping/i);
	});

	it("rejects mixed plain and reverse index removals under same parent", () => {
		const update: readonly UpdateAction[] = [
			{ action: "remove", target: { ref: "data", path: "$.items[0]" } },
			{ action: "remove", target: { ref: "data", path: "$.items[#-1]" } },
		];
		expect(() => validateUpdateExpression(update)).toThrow(/cannot mix plain and reverse index removals/i);
	});

	it("rejects append target for remove action", () => {
		const update = [{ action: "remove", target: { ref: "data", path: "$.tags[#]" } }];
		expect(() => validateUpdateExpression(update)).toThrow(/invalid.*path/i);
	});

	it("rejects an empty update action list", () => {
		expect(() => validateUpdateExpression([])).toThrow(/at least one action/i);
	});

	it("accepts update action count at limit (32)", () => {
		const actions: UpdateAction[] = Array.from({ length: EXPRESSION_LIMITS.updateActions }, (_, i) => ({
			action: "set",
			target: { ref: "data", path: `$.field${i}` },
			value: { val: i },
		}));
		expect(() => validateUpdateExpression(actions)).not.toThrow();
	});

	it("rejects update action count above limit (33)", () => {
		const actions = Array.from({ length: EXPRESSION_LIMITS.updateActions + 1 }, (_, i) => ({
			action: "set",
			target: { ref: "data", path: `$.field${i}` },
			value: { val: i },
		}));
		expect(() => validateUpdateExpression(actions)).toThrow(/action limit exceeded/i);
	});

	it("rejects unknown action kinds", () => {
		const update = [{ action: "add", target: { ref: "data", path: "$.count" }, value: { val: 1 } }];
		expect(() => validateUpdateExpression(update)).toThrow(/unknown update action/i);
	});

	it("rejects extra fields on action node", () => {
		const update = [{ action: "set", target: { ref: "data", path: "$.a" }, value: { val: 1 }, extra: true }];
		expect(() => validateUpdateExpression(update)).toThrow(/invalid.*fields/i);
	});

	it("rejects set action missing value", () => {
		const update = [{ action: "set", target: { ref: "data", path: "$.a" } }];
		expect(() => validateUpdateExpression(update)).toThrow(/requires value/i);
	});

	it("rejects remove action containing value", () => {
		const update = [{ action: "remove", target: { ref: "data", path: "$.a" }, value: { val: 1 } }];
		expect(() => validateUpdateExpression(update)).toThrow(/invalid.*fields/i);
	});

	it("rejects non-data target reference", () => {
		const update = [{ action: "set", target: { ref: "hashKey", path: "$.a" }, value: { val: 1 } }];
		expect(() => validateUpdateExpression(update)).toThrow(/invalid update target/i);
	});

	it("tracks required columns from set values", () => {
		const update: readonly UpdateAction[] = [
			{ action: "set", target: { ref: "data", path: "$.prevVersion" }, value: { ref: "v" } },
			{ action: "set", target: { ref: "data", path: "$.expireAt" }, value: { ref: "ttlAt" } },
		];
		const analysis = validateUpdateExpression(update);
		expect(analysis.requiredColumns).toContain("v");
		expect(analysis.requiredColumns).toContain("ttl_epoch_utc_seconds");
		expect(analysis.requiredColumns).toContain("data_kind");
		expect(analysis.requiredColumns).toContain("data");
	});
});
