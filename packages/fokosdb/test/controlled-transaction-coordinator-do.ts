/**
 * A `TransactionCoordinatorDO` with test controls: RPCs that a test calls to count a call or to
 * change a value. Each control is a field of one instance, for the same reason as the test controls
 * of `ControlledPartitionDO`.
 *
 * The class adds no behavior of its own: each override only counts one call or replaces one tuning
 * value.
 */
import { TransactionCoordinatorDO } from "../src/server/do-transaction-coordinator.js";
import type { TransactionCoordinatorDOConfigOverrides } from "../src/server/host-config.js";

export class ControlledTransactionCoordinatorDO extends TransactionCoordinatorDO {
	#initiateWriteCalls = 0;
	#config: TransactionCoordinatorDOConfigOverrides = {};

	override async initiateWrite(...args: Parameters<TransactionCoordinatorDO["initiateWrite"]>) {
		this.#initiateWriteCalls++;
		return await super.initiateWrite(...args);
	}

	protected override fokosConfig(): TransactionCoordinatorDOConfigOverrides {
		return this.#config;
	}

	async testInitiateWriteCalls(): Promise<number> {
		return this.#initiateWriteCalls;
	}

	/** Replaces the setting overrides of this coordinator. `{}` restores the defaults. */
	async testConfig(overrides: TransactionCoordinatorDOConfigOverrides): Promise<void> {
		this.#config = overrides;
	}
}
