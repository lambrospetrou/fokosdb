import { xxHash32 } from "js-xxhash";

// Golden Ratio constant for better hash scattering.
// See https://softwareengineering.stackexchange.com/a/402543
export const GOLDEN_RATIO = 0x9e3779b1;

/**
 * Given a hashKey and a parent's absolute depth, compute which child slot (0..K-1)
 * the key routes to. Single source of truth for hash-based child selection.
 *
 * The hash seed is `parentAbsDepth + 1` (the child's own depth), so each tree level uses a
 * distinct seed and siblings never cluster regardless of the key distribution.
 */
export function hashChildIndex(hashKey: string, parentAbsDepth: number, K: number): number {
	return xxHash32(hashKey + (parentAbsDepth + 1), GOLDEN_RATIO) % K;
}

/**
 * Compute the root partition index for a hashKey across rootTreesN root partitions.
 * Separate from hashChildIndex because root selection has no depth suffix.
 */
export function hashRootIndex(hashKey: string, rootTreesN: number): number {
	return xxHash32(hashKey, GOLDEN_RATIO) % rootTreesN;
}
