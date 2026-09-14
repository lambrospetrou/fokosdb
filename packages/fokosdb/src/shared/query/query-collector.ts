import type { KeyBytes } from "../partition-topology/key-codec.js";
import type { ProjectedWireRow } from "../expression/projection.js";
import type { QueryScanRow, ScanCursor, StoredItem } from "../partition/partition-store.js";
import type { QuerySelect } from "../types.js";
import type { QueryPageBudgetState } from "./page-budget.js";
import invariant from "../invariant.js";

export type QueryCollectionState = {
	items: Array<StoredItem | ProjectedWireRow>;
	count: number;
	scannedCount: number;
	evaluatedBytes: number;
	responseBytes: number;
	rowsReturned: number;
	allowOversizedFirstItem: boolean;
	lastEvaluatedCursor: ScanCursor | null;
	nextCursor: ScanCursor | null;
};

/**
 * Builds one logical query page from a synchronous row stream.
 *
 * Every row that the stream yields counts in `rowsReturned` first. A candidate then enters the page
 * only when the evaluated-item budget has room and its stored size fits the evaluated-byte budget;
 * a matched candidate in projection mode must also fit the response budget, unless
 * `allowOversizedFirstItem` still holds. A candidate that a budget rejects stops the page with an
 * inclusive `nextCursor` at that candidate, so the next page evaluates it. `nextCursor` stays null
 * when the stream drains. The stream is not consumed past the rejected candidate. A candidate the
 * plan's filter rejected (`matched: false`) consumes both evaluated budgets, advances the cursor,
 * and consumes zero response bytes.
 */
export function collectQueryPage(opts: {
	rows: Iterable<QueryScanRow>;
	hashKey: KeyBytes;
	select: QuerySelect;
	budget: Pick<
		QueryPageBudgetState,
		"remainingEvaluatedItems" | "remainingEvaluatedBytes" | "remainingResponseBytes" | "allowOversizedFirstItem"
	>;
	estimateResponseBytes: (item: StoredItem | ProjectedWireRow) => number;
}): QueryCollectionState {
	const { rows, hashKey, select, budget, estimateResponseBytes } = opts;

	const state: QueryCollectionState = {
		items: [],
		count: 0,
		scannedCount: 0,
		evaluatedBytes: 0,
		responseBytes: 0,
		rowsReturned: 0,
		allowOversizedFirstItem: budget.allowOversizedFirstItem,
		lastEvaluatedCursor: null,
		nextCursor: null,
	};
	let remainingItems = budget.remainingEvaluatedItems;
	let remainingEvaluatedBytes = budget.remainingEvaluatedBytes;
	let remainingResponseBytes = budget.remainingResponseBytes;

	// A budget rejects the candidate BEFORE it enters the page: the next page resumes at it.
	const stopBefore = (sk: KeyBytes) => {
		state.nextCursor = { hk: hashKey, sk, inclusive: true };
	};

	for (const row of rows) {
		state.rowsReturned++;
		if (remainingItems <= 0 || row.estRowBytes > remainingEvaluatedBytes) {
			stopBefore(row.sk);
			break;
		}

		let materialized: StoredItem | ProjectedWireRow | null = null;
		let materializedBytes = 0;
		if (row.matched && select === "projection") {
			materialized = row.projected ?? row.item;
			invariant(materialized !== null, "fokos/query-collector: projection scan row has no item");
			materializedBytes = estimateResponseBytes(materialized);
			if (materializedBytes > remainingResponseBytes && !state.allowOversizedFirstItem) {
				stopBefore(row.sk);
				break;
			}
		}

		state.scannedCount++;
		state.evaluatedBytes += row.estRowBytes;
		remainingItems--;
		remainingEvaluatedBytes -= row.estRowBytes;
		state.lastEvaluatedCursor = { hk: hashKey, sk: row.sk };
		if (row.matched) {
			state.count++;
		}
		if (materialized !== null) {
			state.items.push(materialized);
			state.responseBytes += materializedBytes;
			remainingResponseBytes -= materializedBytes;
			state.allowOversizedFirstItem = false;
		}
	}

	return state;
}
