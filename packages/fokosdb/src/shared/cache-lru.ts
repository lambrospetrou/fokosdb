import invariant from "./invariant.js";

export type LRUCacheWeightOptions<V> = {
	/** The highest total weight of the entries. */
	maxWeight: number;
	/** The weight of one value. Read when `set` stores the value. */
	weightOf(this: void, value: V): number;
};

/**
 * A simple LRU cache implementation.
 *
 * Uses a Map to store key-value pairs and maintain the order of insertion.
 * When a key is accessed or added, it is moved to the end of the Map to mark it as recently used.
 * When the cache exceeds its capacity, the least recently used item (the first item in the Map) is removed.
 *
 * With `weight`, `set` also removes the least recently used entries until the total weight is at most
 * `maxWeight`. It never removes the entry that it stores.
 */
export class LRUCache<K, V> {
	readonly capacity: number;
	private map = new Map<K, V>();
	private readonly weight: LRUCacheWeightOptions<V> | undefined;
	private readonly weights = new Map<K, number>();
	private totalWeight = 0;

	constructor(capacity: number, weight?: LRUCacheWeightOptions<V>) {
		invariant(capacity >= 1, "LRUCache capacity must be >= 1");
		this.capacity = capacity;
		this.weight = weight;
	}

	get size(): number {
		return this.map.size;
	}

	/** The total weight of the entries. 0 without the weight option. */
	get weightTotal(): number {
		return this.totalWeight;
	}

	get(key: K): V | undefined {
		const value = this.map.get(key);
		if (value === undefined) {
			return undefined;
		}
		// Move the accessed key to the end to mark it as recently used.
		this.map.delete(key);
		this.map.set(key, value);
		return value;
	}

	has(key: K): boolean {
		return this.map.has(key);
	}

	set(key: K, value: V): void {
		// Delete a key that already exists, so that the re-insert puts it at the end.
		this.delete(key);
		this.map.set(key, value);
		if (this.weight) {
			const w = this.weight.weightOf(value);
			this.weights.set(key, w);
			this.totalWeight += w;
			for (const oldest of this.map.keys()) {
				if (this.totalWeight <= this.weight.maxWeight || oldest === key) {
					break;
				}
				this.delete(oldest);
			}
		}
		if (this.map.size > this.capacity) {
			// Remove the least recently used item (the first item in the Map).
			this.delete(this.map.keys().next().value!);
		}
	}

	delete(key: K): boolean {
		this.totalWeight -= this.weights.get(key) ?? 0;
		this.weights.delete(key);
		return this.map.delete(key);
	}

	clear(): void {
		this.map.clear();
		this.weights.clear();
		this.totalWeight = 0;
	}
}
