/**
 * Partition errors that cross the Durable Object RPC boundary.
 *
 * They live apart from the partition implementation because the client recognises them too, and
 * matching on them must not pull the Durable Object classes into the client bundle.
 */

// Durable Object RPC carries only an error's message across the boundary — not its class, not any
// custom property — so the transaction coordinator recognises backpressure by this substring.
// It must stay in the message errExceededDatabaseSize builds.
const OVER_SIZE_SENTINEL = "partition exceeded its limits";

/** Transient: the partition is healthy but past its cap, and a split will bring it back under. */
export function errExceededDatabaseSize(operationName: string): Error {
	return new Error(`fokos/partition: ${OVER_SIZE_SENTINEL}, please retry later (${operationName}).`);
}

/**
 * True for errExceededDatabaseSize, including after it has crossed a DO RPC boundary. Callers use it
 * to skip retries: the partition is over its cap, and that will not change inside a retry budget
 * measured in seconds.
 */
export function isPartitionExceededDatabaseSizeError(e: unknown): boolean {
	return e instanceof Error && e.message.includes(OVER_SIZE_SENTINEL);
}

// Recognised by message substring for the same reason as OVER_SIZE_SENTINEL: DO RPC carries only an
// error's message across the boundary, so a dedicated error class would not survive the hop.
// It must stay in the message errSinglePartitionFastPathFallback builds.
const FAST_PATH_FALLBACK_SENTINEL = "single-partition fast path not applicable";

/**
 * Raised when a single-shot transaction reaches a partition that cannot execute the whole item set
 * alone — the items straddle a split or a promotion boundary below this node. It carries ZERO side
 * effects, so it is safe to raise from any depth of a forwarding chain: it propagates up through the
 * intermediate routers untouched, and the caller runs the two-phase path instead.
 */
export function errSinglePartitionFastPathFallback(operationName: string): Error {
	return new Error(`fokos/partition: ${FAST_PATH_FALLBACK_SENTINEL}, items span more than one partition (${operationName}).`);
}

/**
 * True for errSinglePartitionFastPathFallback, including after it has crossed a DO RPC boundary.
 * A transport failure carries no sentinel, so it is never mistaken for a fallback and never
 * silently retried on the two-phase path.
 */
export function isSinglePartitionFastPathFallbackError(e: unknown): boolean {
	return e instanceof Error && e.message.includes(FAST_PATH_FALLBACK_SENTINEL);
}
