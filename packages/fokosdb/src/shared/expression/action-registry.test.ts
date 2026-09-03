import { describe, expect, it } from "vitest";
import { ACTION_REGISTRY, getActionDefinition } from "./action-registry.js";

describe("action registry", () => {
	it("contains set and remove actions", () => {
		expect(ACTION_REGISTRY.size).toBe(2);
		expect(ACTION_REGISTRY.has("set")).toBe(true);
		expect(ACTION_REGISTRY.has("remove")).toBe(true);
	});

	it("declares target rules for set action", () => {
		const setAction = getActionDefinition("set");
		expect(setAction).toBeDefined();
		expect(setAction?.kind).toBe("set");
		expect(setAction?.hasValue).toBe(true);
		expect(setAction?.allowAppend).toBe(true);
		expect(setAction?.targetGuardRequired).toBe(true);
		expect(setAction?.sortRemovals).toBe(false);
	});

	it("declares target rules for remove action", () => {
		const removeAction = getActionDefinition("remove");
		expect(removeAction).toBeDefined();
		expect(removeAction?.kind).toBe("remove");
		expect(removeAction?.hasValue).toBe(false);
		expect(removeAction?.allowAppend).toBe(false);
		expect(removeAction?.targetGuardRequired).toBe(false);
		expect(removeAction?.sortRemovals).toBe(true);
	});

	it("returns undefined for unknown action kinds", () => {
		expect(getActionDefinition("delete")).toBeUndefined();
		expect(getActionDefinition("add")).toBeUndefined();
		expect(getActionDefinition("")).toBeUndefined();
	});
});
