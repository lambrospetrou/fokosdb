import { hashChildIndex } from "./hash-primitives.js";

export type HashTopologySnapshot = {
	// Zero-copy Uint8Array view over the used portion of the arena buffer (nextFree * 4 bytes).
	// The actual copy happens inside kv.put (structured clone).
	arena: Uint8Array;
	nextFree: number;
	K: number;
	// Absolute depth of the partition that owns this topology in the hash tree.
	// The arena slot indices are computed from (ownerAbsDepth + relDepth), so restoring the
	// topology without this value would silently route to the wrong children.
	ownerAbsDepth: number;
};

const DEFAULT_BUDGET_BYTES = 1024 * 1024; // 1 MB

/**
 * In-memory cache of the hash partition topology for a partition.
 * The topology is a K-ary tree where each node represents a split in the hash space.
 * The root node represents the entire hash space, and each level of the tree represents a further split of the hash space.
 * The leaf nodes represent the actual partitions that serve requests.
 * The topology is learned over time from forwarded requests that include hints about the actual depth of the target partition.
 */
export class HashTopology {
	// Flat arena allocator. Each node block is K consecutive slots.
	// Slot value 0 = leaf/unknown; non-zero = index of that child's K-slot block.
	// Root block always occupies slots [0, K). New blocks are appended at nextFree.
	private arena: Uint32Array;
	private nextFree: number;
	private K: number;
	private ownerAbsDepth: number;
	private maxDepth: number;
	private maxSlots: number;

	private constructor(arena: Uint32Array, nextFree: number, K: number, ownerAbsDepth: number, maxDepth: number, maxSlots: number) {
		this.arena = arena;
		this.nextFree = nextFree;
		this.K = K;
		this.ownerAbsDepth = ownerAbsDepth;
		this.maxDepth = maxDepth;
		this.maxSlots = maxSlots;
	}

	static create(K: number, ownerAbsDepth: number, opts?: { maxDepth?: number; budgetBytes?: number }): HashTopology {
		const maxSlots = Math.floor((opts?.budgetBytes ?? DEFAULT_BUDGET_BYTES) / 4);
		const maxDepth = opts?.maxDepth ?? defaultMaxDepth(K, maxSlots);
		const arena = new Uint32Array(maxSlots);
		return new HashTopology(arena, K /* root block pre-allocated */, K, ownerAbsDepth, maxDepth, maxSlots);
	}

	static fromSnapshot(snapshot: HashTopologySnapshot, opts?: { maxDepth?: number; budgetBytes?: number }): HashTopology {
		const { K, nextFree, ownerAbsDepth } = snapshot;
		const maxSlots = Math.floor((opts?.budgetBytes ?? DEFAULT_BUDGET_BYTES) / 4);
		const maxDepth = opts?.maxDepth ?? defaultMaxDepth(K, maxSlots);
		const arena = new Uint32Array(maxSlots);
		new Uint8Array(arena.buffer).set(snapshot.arena);
		return new HashTopology(arena, nextFree, K, ownerAbsDepth, maxDepth, maxSlots);
	}

	toSnapshot(): HashTopologySnapshot {
		return {
			arena: new Uint8Array(this.arena.buffer, 0, this.nextFree * 4),
			nextFree: this.nextFree,
			K: this.K,
			ownerAbsDepth: this.ownerAbsDepth,
		};
	}

	/**
	 * Traverse the cache for hashKey. Returns the relative depth of the deepest known descendant.
	 * @return relative depth (0 = immediate child is the target; no deeper split recorded yet)
	 */
	findLeaf(hashKey: string): number {
		let block = 0;
		let relDepth = 0;
		while (true) {
			const childIdx = hashChildIndex(hashKey, this.ownerAbsDepth + relDepth, this.K);
			const ptr = this.arena[block + childIdx];
			if (ptr === 0) break;
			block = ptr;
			relDepth++;
		}
		return relDepth;
	}

	/**
	 * Learn topology from a forwarded response.
	 * @param hashKey The hash key to look up.
	 * @param actualRelDepth The actual relative depth of the target partition that served the request (non-split leaf).
	 * @return true if the cache was modified (caller should persist).
	 */
	updateFromHint(hashKey: string, actualRelDepth: number): boolean {
		let block = 0;
		let updated = false;
		for (let rd = 0; rd < actualRelDepth; rd++) {
			const childIdx = hashChildIndex(hashKey, this.ownerAbsDepth + rd, this.K);
			if (this.arena[block + childIdx] !== 0) {
				// Level already known — descend without allocating.
				block = this.arena[block + childIdx];
				continue;
			}
			// B3 hybrid eviction: depth cap first, then budget cap.
			// rd + 1 is the depth of the block we're about to allocate; stop only when
			// that would exceed maxDepth (inclusive), so maxDepth=3 allows findLeaf to return 3.
			if (rd + 1 > this.maxDepth) break;
			if (this.nextFree + this.K > this.maxSlots) break;
			const newBlock = this.nextFree;
			this.nextFree += this.K;
			this.arena[block + childIdx] = newBlock;
			block = newBlock;
			updated = true;
		}
		return updated;
	}

	/**
	 * True when the root block is allocated but no children have been recorded.
	 * @return boolean indicating if the topology is empty.
	 */
	isEmpty(): boolean {
		return this.nextFree === this.K;
	}

	stats(): {
		usedSlots: number;
		maxSlots: number;
		K: number;
		maxDepth: number;
		ownerAbsDepth: number;
	} {
		return {
			usedSlots: this.nextFree,
			maxSlots: this.maxSlots,
			K: this.K,
			maxDepth: this.maxDepth,
			ownerAbsDepth: this.ownerAbsDepth,
		};
	}
}

/**
 * Heuristic for maxDepth when not explicitly configured.
 * This is a safeguard against unbounded depth growth in cases of very large budgets or small K,
 * which would lead to excessive latency and memory usage without providing practical benefits.
 *
 * **Space**: each split node occupies `K × 4` bytes. The table below shows **worst-case** space (every node at depths 0 through D-1 is split) for different fanout K and depth cap D:
 *
 * 	| K \ D | 5 | 10 | 20 |
 * 	|---|---|---|---|
 * 	| **2** | 248 B | 8 KB | 8 MB |
 * 	| **4** | 5.3 KB | 5.3 MB | ~5.9 TB |
 * 	| **8** | 146 KB | 4.6 GB | — |
 * 	| **16** | 4.3 MB | — | — |
 *
 * 	Maximum depth D that stays within a 1 MB budget (worst case, fully split tree):
 *
 * 	| K | Max D in 1 MB | Max split nodes |
 * 	|---|---|---|
 * 	| 2 | 17 | 131,071 |
 * 	| 4 | 8 | 21,845 |
 * 	| 8 | 5 | 4,681 |
 * 	| 16 | 4 | 4,369 |
 *
 * @param K The fanout of each node in the hash topology.
 * @param maxSlots The maximum number of slots available in the arena.
 * @returns The calculated maximum depth for the hash topology.
 */
function defaultMaxDepth(K: number, maxSlots: number): number {
	// Rough approximation.
	// The exact max depth depends on the shape of the splits, but this gives a reasonable default
	// that prevents pathological cases while allowing sufficient depth for practical use.
	// K^D = maxSlots => D = log_K(maxSlots) = log10(maxSlots) / log10(K)
	const maxDepth = Math.ceil(Math.log10(maxSlots) / Math.log10(K));
	// We add an extra 3 levels as buffer since it's very very rare to have a complete tree with splits.
	return maxDepth + 3;
}
