import { describe, expect, it } from "vitest";
import { collectBatch } from "./batch-scan.js";

type Row = { id: number; bytes: number };
type Cursor = { afterId: number };

// In-memory table keyed by ascending id, paged exactly like the SQL queries:
// strictly after the cursor, ordered, LIMIT pageSize.
function makeTable(rows: Row[]) {
	const sorted = [...rows].sort((a, b) => a.id - b.id);
	const fetchCalls: Array<{ cursor: Cursor | null; pageSize: number }> = [];
	return {
		fetchCalls,
		fetchPage: (cursor: Cursor | null, pageSize: number): Row[] => {
			fetchCalls.push({ cursor, pageSize });
			const start = cursor === null ? 0 : sorted.findIndex((r) => r.id > cursor.afterId);
			if (start === -1) return [];
			return sorted.slice(start, start + pageSize);
		},
	};
}

function rows(...specs: Array<[id: number, bytes: number]>): Row[] {
	return specs.map(([id, bytes]) => ({ id, bytes }));
}

const advanceCursor = (row: Row): Cursor => ({ afterId: row.id });
const estimateBytes = (row: Row) => row.bytes;

describe("collectBatch", () => {
	it("collects everything and returns a null cursor when the table fits the budget", () => {
		const table = makeTable(rows([1, 10], [2, 10], [3, 10]));
		const result = collectBatch({
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			budgetBytes: 1000,
			pageSize: 10,
			startCursor: null,
		});
		expect(result.rows.map((r) => r.id)).toEqual([1, 2, 3]);
		expect(result.nextCursor).toBeNull();
	});

	it("returns empty rows and a null cursor for an empty table", () => {
		const table = makeTable([]);
		const result = collectBatch({
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			budgetBytes: 1000,
			pageSize: 10,
			startCursor: null,
		});
		expect(result.rows).toEqual([]);
		expect(result.nextCursor).toBeNull();
	});

	it("stops at the byte budget and returns a cursor at the last included row", () => {
		const table = makeTable(rows([1, 60], [2, 60], [3, 60]));
		const result = collectBatch({
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			budgetBytes: 100,
			pageSize: 10,
			startCursor: null,
		});
		// Row 1 (60) fits; row 2 would exceed 100 — batch stops with cursor at row 1.
		expect(result.rows.map((r) => r.id)).toEqual([1]);
		expect(result.nextCursor).toEqual({ afterId: 1 });
	});

	it("resumes from the returned cursor without skipping or duplicating rows", () => {
		const table = makeTable(rows([1, 60], [2, 60], [3, 60]));
		const opts = {
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			budgetBytes: 100,
			pageSize: 10,
		};
		const batch1 = collectBatch({ ...opts, startCursor: null });
		const batch2 = collectBatch({ ...opts, startCursor: batch1.nextCursor });
		const batch3 = collectBatch({ ...opts, startCursor: batch2.nextCursor });
		expect(batch1.rows.map((r) => r.id)).toEqual([1]);
		expect(batch2.rows.map((r) => r.id)).toEqual([2]);
		expect(batch3.rows.map((r) => r.id)).toEqual([3]);
		expect(batch3.nextCursor).toBeNull();
	});

	it("re-running with the same cursor is idempotent (crash before checkpoint)", () => {
		const table = makeTable(rows([1, 60], [2, 60], [3, 60]));
		const opts = {
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			budgetBytes: 100,
			pageSize: 10,
		};
		const batch1 = collectBatch({ ...opts, startCursor: null });
		const retry = collectBatch({ ...opts, startCursor: null });
		expect(retry.rows).toEqual(batch1.rows);
		expect(retry.nextCursor).toEqual(batch1.nextCursor);
	});

	it("always includes the first matched row even when it alone exceeds the budget", () => {
		const table = makeTable(rows([1, 500], [2, 10]));
		const result = collectBatch({
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			budgetBytes: 100,
			pageSize: 10,
			startCursor: null,
		});
		expect(result.rows.map((r) => r.id)).toEqual([1]);
		expect(result.nextCursor).toEqual({ afterId: 1 });
	});

	it("advances the cursor past filtered-out rows", () => {
		// Only even ids match; the scan must not get stuck on (or re-scan) odd ids.
		const table = makeTable(rows([1, 60], [2, 60], [3, 60], [4, 60], [5, 60]));
		const opts = {
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			include: (row: Row) => row.id % 2 === 0,
			budgetBytes: 100,
			pageSize: 10,
		};
		const batch1 = collectBatch({ ...opts, startCursor: null });
		// Row 2 fits, row 4 would exceed; the cursor is at row 3 (the last scanned row before 4).
		expect(batch1.rows.map((r) => r.id)).toEqual([2]);
		expect(batch1.nextCursor).toEqual({ afterId: 3 });
		const batch2 = collectBatch({ ...opts, startCursor: batch1.nextCursor });
		expect(batch2.rows.map((r) => r.id)).toEqual([4]);
		expect(batch2.nextCursor).toBeNull();
	});

	it("returns a null cursor when nothing matches the filter", () => {
		const table = makeTable(rows([1, 10], [2, 10], [3, 10]));
		const result = collectBatch({
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			include: () => false,
			budgetBytes: 100,
			pageSize: 2,
			startCursor: null,
		});
		expect(result.rows).toEqual([]);
		expect(result.nextCursor).toBeNull();
	});

	it("stops on a short page without fetching again", () => {
		const table = makeTable(rows([1, 10], [2, 10], [3, 10]));
		collectBatch({
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			budgetBytes: 1000,
			pageSize: 5,
			startCursor: null,
		});
		expect(table.fetchCalls).toHaveLength(1);
	});

	it("fetches the next page when a page is exactly pageSize", () => {
		const table = makeTable(rows([1, 10], [2, 10], [3, 10], [4, 10]));
		const result = collectBatch({
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			budgetBytes: 1000,
			pageSize: 2,
			startCursor: null,
		});
		expect(result.rows.map((r) => r.id)).toEqual([1, 2, 3, 4]);
		expect(result.nextCursor).toBeNull();
		// Pages: [1,2], [3,4], then [] confirms exhaustion.
		expect(table.fetchCalls).toHaveLength(3);
	});

	it("stops mid-page on budget and the cursor excludes the rejected row", () => {
		const table = makeTable(rows([1, 40], [2, 40], [3, 40], [4, 40]));
		const result = collectBatch({
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			budgetBytes: 100,
			pageSize: 10,
			startCursor: null,
		});
		// 40 + 40 = 80 fits; +40 would be 120 > 100. Row 3 is rejected and must be re-fetched next batch.
		expect(result.rows.map((r) => r.id)).toEqual([1, 2]);
		expect(result.nextCursor).toEqual({ afterId: 2 });
	});

	it("starts from a provided startCursor", () => {
		const table = makeTable(rows([1, 10], [2, 10], [3, 10]));
		const result = collectBatch({
			fetchPage: table.fetchPage,
			advanceCursor,
			estimateBytes,
			budgetBytes: 1000,
			pageSize: 10,
			startCursor: { afterId: 1 },
		});
		expect(result.rows.map((r) => r.id)).toEqual([2, 3]);
		expect(result.nextCursor).toBeNull();
	});
});
