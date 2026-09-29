/**
 * A `TransactionCoordinatorDO` with test controls: RPCs that a test calls to hold, count, or answer
 * one call, or to change a value. Each control is a field of one instance, for the same reason as the
 * test controls of `ControlledPartitionDO`.
 *
 * The class adds no behavior of its own: each override only holds, counts, or answers one call, or
 * replaces one tuning value.
 */
import { TransactionCoordinatorDO } from "../src/server/do-transaction-coordinator.js";
import type { TransactionCoordinatorDOConfigOverrides } from "../src/server/host-config.js";
import type { RecoverTransactionRequest, RecoverTransactionResult } from "../src/shared/transaction-wire-types.js";

/**
 * The answer of `recoverTransactionForParticipant` in place of its work. With `hold`, each call waits
 * until `testReleaseRecover` before it answers.
 */
export type RecoverResponseRule = { value: RecoverTransactionResult; hold?: boolean };

export class ControlledTransactionCoordinatorDO extends TransactionCoordinatorDO {
	#initiateWriteCalls = 0;
	#recoverCalls = 0;
	#recoverRule: (RecoverResponseRule & { held: Promise<void>; release: () => void; parked: boolean }) | null = null;
	#config: TransactionCoordinatorDOConfigOverrides = {};

	override async initiateWrite(...args: Parameters<TransactionCoordinatorDO["initiateWrite"]>) {
		this.#initiateWriteCalls++;
		return await super.initiateWrite(...args);
	}

	override async recoverTransactionForParticipant(req: RecoverTransactionRequest): Promise<RecoverTransactionResult> {
		this.#recoverCalls++;
		const rule = this.#recoverRule;
		if (!rule) {
			return await super.recoverTransactionForParticipant(req);
		}
		if (rule.hold) {
			rule.parked = true;
			await rule.held;
		}
		return rule.value;
	}

	protected override fokosConfig(): TransactionCoordinatorDOConfigOverrides {
		return this.#config;
	}

	async testInitiateWriteCalls(): Promise<number> {
		return this.#initiateWriteCalls;
	}

	/** The number of `recoverTransactionForParticipant` calls that this coordinator received. */
	async testRecoverCalls(): Promise<number> {
		return this.#recoverCalls;
	}

	/** Makes each `recoverTransactionForParticipant` answer as `rule` says, until `testReleaseRecover`. */
	async testRecoverResponse(rule: RecoverResponseRule): Promise<void> {
		let release!: () => void;
		const held = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.#recoverRule = { ...rule, held, release, parked: false };
	}

	/** True when a call waits in the hold of `testRecoverResponse`. */
	async testRecoverParked(): Promise<boolean> {
		return this.#recoverRule?.parked ?? false;
	}

	/** Lets each held call answer, and restores the work of `TransactionCoordinatorDO` for later calls. */
	async testReleaseRecover(): Promise<void> {
		this.#recoverRule?.release();
		this.#recoverRule = null;
	}

	/** Replaces the setting overrides of this coordinator. `{}` restores the defaults. */
	async testConfig(overrides: TransactionCoordinatorDOConfigOverrides): Promise<void> {
		this.#config = overrides;
	}
}
