/**
 * A `TransactionCoordinatorDO` with seams that a test drives through its own RPCs. Each seam is a
 * field of one instance, for the same reason as the seams of `ControlledPartitionDO`.
 *
 * The class adds no behavior of its own: each override only counts one call or replaces one tuning
 * value.
 */
import { TransactionCoordinatorDO } from "../src/server/do-transaction-coordinator.js";

export class ControlledTransactionCoordinatorDO extends TransactionCoordinatorDO {
	#initiateWriteCalls = 0;
	#fanoutBudgetMs: number | null = null;

	override async initiateWrite(request: Parameters<TransactionCoordinatorDO["initiateWrite"]>[0]) {
		this.#initiateWriteCalls++;
		return await super.initiateWrite(request);
	}

	override fokosFanoutRequestBudgetMs(): number {
		return this.#fanoutBudgetMs ?? super.fokosFanoutRequestBudgetMs();
	}

	async testInitiateWriteCalls(): Promise<number> {
		return this.#initiateWriteCalls;
	}

	/** Replaces the fan-out budget of this coordinator. `null` restores the shipped value. */
	async testFanoutBudgetMs(ms: number | null): Promise<void> {
		this.#fanoutBudgetMs = ms;
	}
}
