import type { KeyBytes } from "../../sharding/key-codec.js";
import type { ProjectedWireRow } from "../expression/projection.js";
import type { QueryCandidateConsumer, ScanCursor, StoredItem } from "../partition/partition-store.js";
import type { QuerySelect } from "../types.js";
import type { QueryPageBudgetState } from "./page-budget.js";

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
 * Builds one logical query page from the candidates a scan hands to `consume`. The caller runs the
 * scan with `consume` and then reads `state`.
 *
 * Every candidate counts in `rowsReturned` first. A candidate then enters the page only when the
 * evaluated-item budget has room and its stored size fits the evaluated-byte budget; a matched
 * candidate in projection mode must also fit the response budget, unless `allowOversizedFirstItem`
 * still holds. The payload of a candidate is decoded only after the evaluated budgets admitted it,
 * so a candidate that stops the page is never materialized. A candidate that a budget rejects stops
 * the page with an inclusive `nextCursor` at that candidate, so the next page evaluates it.
 * `nextCursor` stays null when the scan drains. The scan does not run past the rejected candidate. A
 * candidate the plan's filter rejected (`matched: false`) consumes both evaluated budgets, advances
 * the cursor, and consumes zero response bytes.
 */
export function createQueryPageCollector(opts: {
	hashKey: KeyBytes;
	select: QuerySelect;
	budget: Pick<
		QueryPageBudgetState,
		"remainingEvaluatedItems" | "remainingEvaluatedBytes" | "remainingResponseBytes" | "allowOversizedFirstItem"
	>;
	estimateResponseBytes: (item: StoredItem | ProjectedWireRow) => number;
}): { consume: QueryCandidateConsumer; state: QueryCollectionState } {
	const { hashKey, select, budget, estimateResponseBytes } = opts;

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
	const stopBefore = (sk: KeyBytes): false => {
		state.nextCursor = { hk: hashKey, sk, inclusive: true };
		return false;
	};

	const consume: QueryCandidateConsumer = (sk, estRowBytes, matched, decodePayload) => {
		state.rowsReturned++;
		if (remainingItems <= 0 || estRowBytes > remainingEvaluatedBytes) return stopBefore(sk);

		let materialized: StoredItem | ProjectedWireRow | null = null;
		let materializedBytes = 0;
		if (matched && select === "projection") {
			materialized = decodePayload();
			materializedBytes = estimateResponseBytes(materialized);
			if (materializedBytes > remainingResponseBytes && !state.allowOversizedFirstItem) return stopBefore(sk);
		}

		state.scannedCount++;
		state.evaluatedBytes += estRowBytes;
		remainingItems--;
		remainingEvaluatedBytes -= estRowBytes;
		state.lastEvaluatedCursor = { hk: hashKey, sk };
		if (matched) {
			state.count++;
		}
		if (materialized !== null) {
			state.items.push(materialized);
			state.responseBytes += materializedBytes;
			remainingResponseBytes -= materializedBytes;
			state.allowOversizedFirstItem = false;
		}
		return true;
	};

	return { consume, state };
}
