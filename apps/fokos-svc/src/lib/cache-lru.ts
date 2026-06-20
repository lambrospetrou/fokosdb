/**
 * A simple LRU cache implementation.
 *
 * Uses a Map to store key-value pairs and maintain the order of insertion.
 * When a key is accessed or added, it is moved to the end of the Map to mark it as recently used.
 * When the cache exceeds its capacity, the least recently used item (the first item in the Map) is removed.
 */
export class LRUCache<K, V> {
	readonly capacity: number;
	private map = new Map<K, V>();

	constructor(capacity: number) {
		if (capacity < 1) throw new Error("LRUCache capacity must be >= 1");
		this.capacity = capacity;
	}

	get size(): number {
		return this.map.size;
	}

	get(key: K): V | undefined {
		const value = this.map.get(key);
		if (value === undefined) return undefined;
		// Move the accessed key to the end to mark it as recently used.
		this.map.delete(key);
		this.map.set(key, value);
		return value;
	}

	has(key: K): boolean {
		return this.map.has(key);
	}

	set(key: K, value: V): void {
		// If the key already exists, delete it so that we can re-insert it at the end.
		this.map.delete(key);
		this.map.set(key, value);
		if (this.map.size > this.capacity) {
			// Remove the least recently used item (the first item in the Map).
			this.map.delete(this.map.keys().next().value!);
		}
	}

	delete(key: K): boolean {
		return this.map.delete(key);
	}

	clear(): void {
		this.map.clear();
	}
}
