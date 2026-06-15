/**
 * Mutable budget tracker for a single queryItems page. Shared across sub-queries (FokosDB)
 * and across range-tree children (walkRangeChildren). The three counters are independent;
 * callers check `budgetExhausted` vs `visitsExhausted` separately because they produce
 * different cursor shapes (last-item cursor vs boundary cursor).
 */
export class PageBudget {
	remainingBytes: number;
	remainingLimit: number | null;
	remainingVisits: number;

	constructor(budgetBytes: number, limit: number | null, maxVisits: number) {
		this.remainingBytes = budgetBytes;
		this.remainingLimit = limit;
		this.remainingVisits = maxVisits;
	}

	consume(bytesConsumed: number, itemCount: number, partitionsVisited: number): void {
		this.remainingBytes -= bytesConsumed;
		if (this.remainingLimit !== null) this.remainingLimit -= itemCount;
		this.remainingVisits -= partitionsVisited;
	}

	/** Byte budget or item-count cap is exhausted. */
	get budgetExhausted(): boolean {
		return this.remainingBytes <= 0 || this.remainingLimit === 0;
	}

	/** Leaf-partition visit cap is exhausted. */
	get visitsExhausted(): boolean {
		return this.remainingVisits <= 0;
	}

	/** Any of the three budgets is exhausted. */
	get exhausted(): boolean {
		return this.budgetExhausted || this.visitsExhausted;
	}
}
