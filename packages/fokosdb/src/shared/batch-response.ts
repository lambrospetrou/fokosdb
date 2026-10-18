import { MAX_BATCH_GET_RESPONSE_BYTES } from "./transaction-limits.js";

/**
 * Splits the found items of a batch read answer by `maxResponseBytes`, clamped to
 * `MAX_BATCH_GET_RESPONSE_BYTES`. It keeps the items in `opIndex` order while the sum of their `bytes`
 * stays at most the limit. An item that does not fit does not stop a smaller item after it. The item
 * of `opIndex` 0 always stays, so a caller that sends the unprocessed keys again always makes progress.
 * Each partition answer and the result of `batchGetItems` go through this function.
 */
export function fitBatchResponse<T extends { opIndex: number; bytes: number }>(
	found: readonly T[],
	maxResponseBytes: number,
): { kept: T[]; dropped: T[] } {
	const kept: T[] = [];
	const dropped: T[] = [];
	let remaining = Math.min(maxResponseBytes, MAX_BATCH_GET_RESPONSE_BYTES);
	for (const entry of [...found].sort((a, b) => a.opIndex - b.opIndex)) {
		if (entry.opIndex === 0 || entry.bytes <= remaining) {
			remaining -= entry.bytes;
			kept.push(entry);
		} else {
			dropped.push(entry);
		}
	}
	return { kept, dropped };
}
