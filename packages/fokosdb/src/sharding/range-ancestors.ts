import type { RangeAncestorInfo } from "./types.js";

/**
 * Selects the bounded ancestor set a splitting range partition passes to its children: the
 * shallowest `fromRoot` and the deepest `fromLeaf` of the parent's own candidate list (the parent's
 * stored ancestors plus the parent itself), deduped by depth. Called once per split, and identical
 * for every child of that split.
 */
export function selectRangeAncestors(
	parentDepth: number,
	parentAncestors: RangeAncestorInfo[],
	parentAsAncestor: RangeAncestorInfo,
	config: { fromRoot: number; fromLeaf: number },
): RangeAncestorInfo[] {
	const candidates = parentDepth === 0 ? [] : [...parentAncestors, parentAsAncestor];

	// Candidates are already sorted by depth ascending (parentAncestors is stored sorted, and
	// parentAsAncestor.depth === parentDepth is strictly greater than every stored ancestor's depth).
	const shallowest = candidates.slice(0, config.fromRoot);
	// Must NOT use candidates.slice(-fromLeaf): slice(-0) === slice(0), which would return the
	// entire array instead of [] when fromLeaf === 0.
	const deepest = candidates.slice(Math.max(0, candidates.length - config.fromLeaf));

	const byDepth = new Map<number, RangeAncestorInfo>();
	for (const c of [...shallowest, ...deepest]) {
		byDepth.set(c.depth, c);
	}
	return [...byDepth.values()].sort((a, b) => a.depth - b.depth);
}
