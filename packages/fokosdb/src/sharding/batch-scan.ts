/**
 * The single byte-budgeted, cursor-paged scan used by every paged-streaming endpoint of
 * PartitionDO (item migration batches, pending-transaction metadata, promoted-key inheritance).
 *
 * Crash/resume contract (load-bearing for migration):
 * - The cursor advances past EVERY scanned row, matched or not, so a resumed scan never
 *   re-evaluates rows that were already filtered out.
 * - The first matched row is always included even if it alone exceeds the byte budget,
 *   so a single oversized row cannot stall progress.
 * - Scanning stops when a fetched page is shorter than the page size asked for (the table is exhausted).
 * - `nextCursor` is non-null when the byte budget or the item cap stopped the scan. A null
 *   `nextCursor` means the scan reached the end of the table.
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
	/**
	 * Optional item-count cap. Checked *after* advancing the cursor past an included row, so the
	 * row is never re-emitted on resume (unlike the byte-budget break, which re-fetches the
	 * oversized row). When omitted, only the byte budget bounds the scan.
	 */
	maxItems?: number;
	/**
	 * Optional cap on rows READ from the source, matched or not. The item cap bounds what a page
	 * returns; this bounds the work a page costs, which is what a sparse filter can otherwise run
	 * away with — a slice that matches one row in ten thousand would scan the whole table for one
	 * page. Checked after the cursor advances, so the scan resumes where it stopped.
	 */
	maxScannedRows?: number;
	pageSize: number;
	startCursor: TCursor | null;
};

export type CollectBatchResult<TRow, TCursor> = {
	rows: TRow[];
	nextCursor: TCursor | null;
	/** Sum of `estimateBytes` over the included rows (the bytes that counted against `budgetBytes`). */
	totalBytes: number;
};

export function collectBatch<TRow, TCursor>(opts: CollectBatchOptions<TRow, TCursor>): CollectBatchResult<TRow, TCursor> {
	const { fetchPage, advanceCursor, include, estimateBytes, budgetBytes, maxItems, maxScannedRows, pageSize } = opts;

	const rows: TRow[] = [];
	let totalBytes = 0;
	let scanned = 0;
	let cursor = opts.startCursor;
	let reachedLimit = false;

	while (true) {
		const remainingScan = maxScannedRows === undefined ? pageSize : Math.min(pageSize, maxScannedRows - scanned);
		if (remainingScan <= 0) {
			reachedLimit = true;
			break;
		}
		const page = fetchPage(cursor, remainingScan);
		if (page.length === 0) {
			break;
		}

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
			scanned++;

			// Item-count cap: checked *after* advancing the cursor so the included row is not re-emitted.
			if (maxItems !== undefined && rows.length >= maxItems) {
				reachedLimit = true;
				break;
			}
			if (maxScannedRows !== undefined && scanned >= maxScannedRows) {
				reachedLimit = true;
				break;
			}
		}
		if (reachedLimit) {
			break;
		}

		if (page.length < remainingScan) {
			break;
		}
	}

	return { rows, nextCursor: reachedLimit ? cursor : null, totalBytes };
}
