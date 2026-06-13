import { AddResult, BloomFilter, type BloomFilterSnapshot } from "../bloom-filter.js";

export type PartialRangeTopologySnapshot = {
	version: 1;
	promotedKeysBloom: BloomFilterSnapshot;
};

export class PartialRangeTopology {
	private bloom: BloomFilter;

	private constructor(bloom: BloomFilter) {
		this.bloom = bloom;
	}

	static create(opts: { errorRate?: number; maxSizeBytes: number; initialCapacityN?: number }): PartialRangeTopology {
		return new PartialRangeTopology(BloomFilter.create(opts));
	}

	static fromSnapshot(snapshot: PartialRangeTopologySnapshot): PartialRangeTopology {
		return new PartialRangeTopology(BloomFilter.fromSnapshot(snapshot.promotedKeysBloom));
	}

	/**
	 * @returns True if the key is probably promoted (may be a false positive), false if definitely not promoted.
	 */
	maybePromoted(hashKey: string): boolean {
		return this.bloom.has(hashKey);
	}

	learnPromotedKey(hashKey: string): AddResult {
		return this.bloom.add(hashKey);
	}

	/**
	 * @returns True if any key was newly added to the bloom filter (i.e. the filter was modified).
	 */
	learnPromotedKeys(hashKeys: Iterable<string>): boolean {
		let anyAdded = false;
		for (const key of hashKeys) {
			if (this.bloom.add(key) === AddResult.Added) anyAdded = true;
		}
		return anyAdded;
	}

	toSnapshot(): PartialRangeTopologySnapshot {
		return {
			version: 1,
			promotedKeysBloom: this.bloom.toSnapshot(),
		};
	}

	isEmpty(): boolean {
		return this.bloom.additionsCount() === 0;
	}

	stats(): { bloomAdditionsCount: number; bloomMaxSizeBytes: number } {
		return {
			bloomAdditionsCount: this.bloom.additionsCount(),
			bloomMaxSizeBytes: this.bloom.toSnapshot().maxSizeBytes,
		};
	}
}
