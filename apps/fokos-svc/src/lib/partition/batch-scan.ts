/**
 * The single byte-budgeted, cursor-paged scan used by every paged-streaming endpoint of
 * PartitionDO (item migration batches, pending-transaction metadata, promoted-key inheritance).
 *
 * Crash/resume contract (load-bearing for migration):
 * - The cursor advances past EVERY scanned row, matched or not, so a resumed scan never
 *   re-evaluates rows that were already filtered out.
 * - The first matched row is always included even if it alone exceeds the byte budget,
 *   so a single oversized row cannot stall progress.
 * - Scanning stops when a fetched page is shorter than `pageSize` (the table is exhausted).
 * - `nextCursor` is non-null only when the byte budget stopped the scan; a null `nextCursor`
 *   means the scan is complete.
 */
export type CollectBatchOptions<TRow, TCursor> = {
	/** Fetches the next page of rows strictly after `cursor` (null = from the start). */
	fetchPage: (cursor: TCursor | null, pageSize: number) => TRow[];
	/** Returns the cursor positioned at `row` (resume continues strictly after it). */
	advanceCursor: (row: TRow) => TCursor;
	/** Optional row filter; non-matching rows still advance the cursor. */
	include?: (row: TRow) => boolean;
	/** Estimated bytes a matched row contributes to the batch. */
	estimateBytes: (row: TRow) => number;
	/** Stop collecting once accumulated bytes would exceed this (after the first matched row). */
	budgetBytes: number;
	pageSize: number;
	startCursor: TCursor | null;
};

export type CollectBatchResult<TRow, TCursor> = {
	rows: TRow[];
	nextCursor: TCursor | null;
};

export function collectBatch<TRow, TCursor>(opts: CollectBatchOptions<TRow, TCursor>): CollectBatchResult<TRow, TCursor> {
	const { fetchPage, advanceCursor, include, estimateBytes, budgetBytes, pageSize } = opts;

	const rows: TRow[] = [];
	let totalBytes = 0;
	let cursor = opts.startCursor;
	let reachedLimit = false;

	while (true) {
		const page = fetchPage(cursor, pageSize);
		if (page.length === 0) break;

		for (const row of page) {
			if (!include || include(row)) {
				const rowBytes = estimateBytes(row);
				if (rows.length > 0 && totalBytes + rowBytes > budgetBytes) {
					// Budget reached: do NOT advance the cursor past this row — it is re-fetched
					// as the first row of the next batch.
					reachedLimit = true;
					break;
				}
				rows.push(row);
				totalBytes += rowBytes;
			}
			// Always advance the table cursor regardless of whether the row matched.
			cursor = advanceCursor(row);
		}
		if (reachedLimit) break;

		if (page.length < pageSize) break;
	}

	return { rows, nextCursor: reachedLimit ? cursor : null };
}
