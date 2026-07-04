import { describe, expect, it } from "vitest";
import type { RangeAncestorInfo } from "../types.js";
import { KeyCodec } from "./key-codec.js";
import { selectRangeAncestors } from "./split-policy.js";

describe("selectRangeAncestors", () => {
	it("reproduces the doc's fromRoot=2,fromLeaf=2 worked table for depths 1..10", () => {
		const chain = buildChain(10, { fromRoot: 2, fromLeaf: 2 });
		expect(chain[1]).toEqual([]);
		expect(chain[2]).toEqual([1]);
		expect(chain[3]).toEqual([1, 2]);
		expect(chain[4]).toEqual([1, 2, 3]);
		expect(chain[5]).toEqual([1, 2, 3, 4]);
		expect(chain[6]).toEqual([1, 2, 4, 5]);
		expect(chain[7]).toEqual([1, 2, 5, 6]);
		expect(chain[8]).toEqual([1, 2, 6, 7]);
		expect(chain[9]).toEqual([1, 2, 7, 8]);
		expect(chain[10]).toEqual([1, 2, 8, 9]);
	});

	it("always returns [] when fromRoot=0 and fromLeaf=0, regardless of depth", () => {
		const chain = buildChain(10, { fromRoot: 0, fromLeaf: 0 });
		for (let d = 1; d <= 10; d++) {
			expect(chain[d]).toEqual([]);
		}
	});

	it("dedups the overlapping shallowest/deepest windows when fromRoot/fromLeaf are large relative to a shallow tree", () => {
		const chain = buildChain(3, { fromRoot: 10, fromLeaf: 10 });
		expect(chain[1]).toEqual([]);
		expect(chain[2]).toEqual([1]);
		expect(chain[3]).toEqual([1, 2]);
	});

	it("returns [] for a depth-1 child of the root regardless of config", () => {
		expect(depths(selectRangeAncestors(0, [], anc(0), { fromRoot: 2, fromLeaf: 2 }))).toEqual([]);
		expect(depths(selectRangeAncestors(0, [], anc(0), { fromRoot: 10, fromLeaf: 10 }))).toEqual([]);
	});
});

// Test-only helper: only `depth` matters for selection; boundaries are opaque passthrough data.
function anc(depth: number): RangeAncestorInfo {
	return { depth, startBoundary: KeyCodec.encode(`b${depth}`), endBoundary: KeyCodec.encode(`e${depth}`) };
}

function depths(list: RangeAncestorInfo[]): number[] {
	return list.map((a) => a.depth);
}

// Builds the full ancestor chain for a linear split tree (each node's only child is the next depth)
// under a fixed config, mirroring how splits happen one depth at a time in production.
function buildChain(maxDepth: number, config: { fromRoot: number; fromLeaf: number }): number[][] {
	const chain: number[][] = [[]]; // index 0 = depth 0 (root), never has ancestors
	let parentAncestors: RangeAncestorInfo[] = [];
	for (let depth = 1; depth <= maxDepth; depth++) {
		const parentDepth = depth - 1;
		const result = selectRangeAncestors(parentDepth, parentAncestors, anc(parentDepth), config);
		chain.push(depths(result));
		parentAncestors = result;
	}
	return chain;
}
