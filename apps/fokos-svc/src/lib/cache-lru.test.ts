import { describe, expect, it } from "vitest";
import { LRUCache } from "./cache-lru.js";

describe("LRUCache", () => {
	it("stores and retrieves values", () => {
		const cache = new LRUCache<string, number>(3);
		cache.set("a", 1);
		cache.set("b", 2);
		expect(cache.get("a")).toBe(1);
		expect(cache.get("b")).toBe(2);
		expect(cache.size).toBe(2);
	});

	it("returns undefined for missing keys", () => {
		const cache = new LRUCache<string, number>(2);
		expect(cache.get("nope")).toBe(undefined);
	});

	it("evicts the least recently used item when capacity is exceeded", () => {
		const cache = new LRUCache<string, number>(3);
		cache.set("a", 1);
		cache.set("b", 2);
		cache.set("c", 3);
		cache.set("d", 4); // evicts "a"
		expect(cache.get("a")).toBe(undefined);
		expect(cache.size).toBe(3);
		expect(cache.get("b")).toBe(2);
		expect(cache.get("c")).toBe(3);
		expect(cache.get("d")).toBe(4);
	});

	it("get() refreshes recency so the item is not evicted", () => {
		const cache = new LRUCache<string, number>(3);
		cache.set("a", 1);
		cache.set("b", 2);
		cache.set("c", 3);
		cache.get("a"); // refresh "a"
		cache.set("d", 4); // evicts "b" (now the oldest)
		expect(cache.get("a")).toBe(1);
		expect(cache.get("b")).toBe(undefined);
	});

	it("set() on an existing key updates the value and refreshes recency", () => {
		const cache = new LRUCache<string, number>(3);
		cache.set("a", 1);
		cache.set("b", 2);
		cache.set("c", 3);
		cache.set("a", 10); // update + refresh "a"
		cache.set("d", 4); // evicts "b"
		expect(cache.get("a")).toBe(10);
		expect(cache.get("b")).toBe(undefined);
	});

	it("has() returns true for present keys, false for absent", () => {
		const cache = new LRUCache<string, number>(2);
		cache.set("x", 42);
		expect(cache.has("x")).toBe(true);
		expect(cache.has("y")).toBe(false);
	});

	it("delete() removes a key and returns whether it existed", () => {
		const cache = new LRUCache<string, number>(3);
		cache.set("a", 1);
		cache.set("b", 2);
		expect(cache.delete("a")).toBe(true);
		expect(cache.delete("a")).toBe(false);
		expect(cache.get("a")).toBe(undefined);
		expect(cache.size).toBe(1);
	});

	it("clear() empties the cache", () => {
		const cache = new LRUCache<string, number>(3);
		cache.set("a", 1);
		cache.set("b", 2);
		cache.clear();
		expect(cache.size).toBe(0);
		expect(cache.get("a")).toBe(undefined);
	});

	it("throws on capacity < 1", () => {
		expect(() => new LRUCache(0)).toThrow();
		expect(() => new LRUCache(-1)).toThrow();
	});

	it("works with capacity of 1", () => {
		const cache = new LRUCache<string, number>(1);
		cache.set("a", 1);
		expect(cache.get("a")).toBe(1);
		cache.set("b", 2);
		expect(cache.get("a")).toBe(undefined);
		expect(cache.get("b")).toBe(2);
		expect(cache.size).toBe(1);
	});
});

describe("LRUCache as a set (null sentinel)", () => {
	it("tracks membership without extra value allocation", () => {
		const seen = new LRUCache<string, null>(4);
		seen.set("alpha", null);
		seen.set("beta", null);
		seen.set("gamma", null);

		expect(seen.has("alpha")).toBe(true);
		expect(seen.has("beta")).toBe(true);
		expect(seen.has("missing")).toBe(false);
		expect(seen.size).toBe(3);
	});

	it("evicts oldest members when the set overflows", () => {
		const seen = new LRUCache<string, null>(3);
		seen.set("a", null);
		seen.set("b", null);
		seen.set("c", null);
		seen.set("d", null); // evicts "a"

		expect(seen.has("a")).toBe(false);
		expect(seen.has("b")).toBe(true);
		expect(seen.has("c")).toBe(true);
		expect(seen.has("d")).toBe(true);
	});

	it("re-adding refreshes recency in set mode", () => {
		const seen = new LRUCache<string, null>(3);
		seen.set("a", null);
		seen.set("b", null);
		seen.set("c", null);
		seen.set("a", null); // refresh "a"
		seen.set("d", null); // evicts "b"

		expect(seen.has("a")).toBe(true);
		expect(seen.has("b")).toBe(false);
	});
});
