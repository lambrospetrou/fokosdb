/**
 * Mutable budget tracker for a single queryItems page. Shared across sub-queries (FokosDB)
 * and across range-tree children (walkRangeChildren). Four counters bound one page: evaluated
 * items, evaluated bytes (the stored bytes of the evaluated candidates), response bytes (the
 * materialized items), and leaf-partition visits. `allowOversizedFirstItem` lets the first
 * materialized item of the page exceed the response budget, so one oversized item cannot stall
 * the cursor. Callers check `budgetExhausted` vs `visitsExhausted` separately because they
 * produce different cursor shapes (last-evaluated cursor vs boundary cursor).
 */
export const DEFAULT_EVALUATED_ITEMS_PER_PAGE = 1_000;
export const MAX_EVALUATED_ITEMS_PER_PAGE = 100_000;
export const MAX_EVALUATED_BYTES_PER_PAGE = 100 * 1024 * 1024;
export const DEFAULT_RESPONSE_BYTES_PER_PAGE = 3 * 1024 * 1024;
export const MAX_RESPONSE_BYTES_PER_PAGE = 16 * 1024 * 1024;
export const MAX_PARTITION_VISITS_PER_PAGE = 100;

export type QueryPageBudgetState = {
	remainingEvaluatedItems: number;
	remainingEvaluatedBytes: number;
	remainingResponseBytes: number;
	remainingPartitionVisits: number;
	allowOversizedFirstItem: boolean;
};

export class QueryPageBudget implements QueryPageBudgetState {
	remainingEvaluatedItems: number;
	remainingEvaluatedBytes: number;
	remainingResponseBytes: number;
	remainingPartitionVisits: number;
	allowOversizedFirstItem: boolean;

	constructor(init: QueryPageBudgetState) {
		this.remainingEvaluatedItems = init.remainingEvaluatedItems;
		this.remainingEvaluatedBytes = init.remainingEvaluatedBytes;
		this.remainingResponseBytes = init.remainingResponseBytes;
		this.remainingPartitionVisits = init.remainingPartitionVisits;
		this.allowOversizedFirstItem = init.allowOversizedFirstItem;
	}

	/** Applies one partition response to the page budget. */
	consume(res: {
		scannedCount: number;
		evaluatedBytes: number;
		responseBytes: number;
		items: readonly unknown[];
		partitionMetas: readonly unknown[];
	}): void {
		this.remainingEvaluatedItems -= res.scannedCount;
		this.remainingEvaluatedBytes -= res.evaluatedBytes;
		this.remainingResponseBytes -= res.responseBytes;
		this.remainingPartitionVisits -= res.partitionMetas.length;
		if (res.items.length > 0) {
			this.allowOversizedFirstItem = false;
		}
	}

	/**
	 * The evaluated-item, evaluated-byte, or response-byte budget is exhausted. These stop the
	 * page at the last evaluated candidate.
	 */
	get budgetExhausted(): boolean {
		return this.remainingEvaluatedItems <= 0 || this.remainingEvaluatedBytes <= 0 || this.remainingResponseBytes <= 0;
	}

	/** The leaf-partition visit budget is exhausted. This stops the page at a child boundary. */
	get visitsExhausted(): boolean {
		return this.remainingPartitionVisits <= 0;
	}

	get exhausted(): boolean {
		return this.budgetExhausted || this.visitsExhausted;
	}
}
