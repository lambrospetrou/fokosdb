import { AddResult, BloomFilter, type BloomFilterSnapshot } from "../shared/bloom-filter.js";
import type { HashPrefixBucket } from "./hash-primitives.js";

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
	 * @returns True if the hashPrefixBucket is probably promoted (may be a false positive), false if
	 * definitely not promoted.
	 */
	maybePromoted(hashPrefixBucket: HashPrefixBucket): boolean {
		return this.bloom.has(hashPrefixBucket);
	}

	learnPromotedHashPrefixBucket(hashPrefixBucket: HashPrefixBucket): AddResult {
		return this.bloom.add(hashPrefixBucket);
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
