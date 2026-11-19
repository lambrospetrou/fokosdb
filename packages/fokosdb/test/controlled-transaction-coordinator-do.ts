/**
 * A `FokosDBTransactionCoordinatorDO` with test controls: RPCs that a test calls to hold, count, or answer
 * one call, or to change a value. Each control is a field of one instance, for the same reason as the
 * test controls of `ControlledPartitionDO`.
 *
 * The class adds no behavior of its own: each override only holds, counts, or answers one call, or
 * replaces one tuning value.
 */
import { systemPlatform, type FokosPlatform } from "../src/sharding/platform.js";
import { FokosDBTransactionCoordinatorDO } from "../src/server/do-transaction-coordinator.js";
import type { TransactionCoordinatorDOConfigOverrides } from "../src/server/host-config.js";
import type { RecoverTransactionRequest, RecoverTransactionResult } from "../src/shared/transaction-wire-types.js";

/**
 * The answer of `recoverTransactionForParticipant` in place of its work. With `hold`, each call waits
 * until `testReleaseRecover` before it answers.
 */
export type RecoverResponseRule = { value: RecoverTransactionResult; hold?: boolean };

export class ControlledTransactionCoordinatorDO extends FokosDBTransactionCoordinatorDO {
	#initiateWriteCalls = 0;
	#initiateWriteFault: Record<string, unknown> | null = null;
	#recoverCalls = 0;
	#recoverRule: (RecoverResponseRule & { held: Promise<void>; release: () => void; parked: boolean }) | null = null;
	#config: TransactionCoordinatorDOConfigOverrides = {};

	override async initiateWrite(...args: Parameters<FokosDBTransactionCoordinatorDO["initiateWrite"]>) {
		this.#initiateWriteCalls++;
		const fault = this.#initiateWriteFault;
		this.#initiateWriteFault = null;
		const answer = await super.initiateWrite(...args);
		if (fault) {
			throw Object.assign(new Error("simulated runtime fault"), fault);
		}
		return answer;
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

	/** The clock of this instance. A test replaces it with `vi.spyOn(instance, "testNow")`. */
	testNow(): number {
		return Date.now();
	}

	// The constructor of the base class reads it before the fields of this class exist. The clock reads
	// `testNow` at each call, so a spy on one instance changes only that instance.
	protected override fokosPlatform(): FokosPlatform {
		return { ...systemPlatform, now: () => this.testNow() };
	}

	protected override fokosConfig(): TransactionCoordinatorDOConfigOverrides {
		return this.#config;
	}

	async testInitiateWriteCalls(): Promise<number> {
		return this.#initiateWriteCalls;
	}

	/**
	 * Makes the next `initiateWrite` do its work and then throw an error with the own properties
	 * `fault`, as a reply that the runtime loses after the work. For example `{ retryable: true }`.
	 */
	async testInitiateWriteFault(fault: Record<string, unknown>): Promise<void> {
		this.#initiateWriteFault = fault;
	}

	/** Resets this instance with `ctx.abort`. Each open call fails, and the next call starts a new instance. */
	async testReset(): Promise<void> {
		this.ctx.abort("test reset");
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

	/** Lets each held call answer, and restores the work of `FokosDBTransactionCoordinatorDO` for later calls. */
	async testReleaseRecover(): Promise<void> {
		this.#recoverRule?.release();
		this.#recoverRule = null;
	}

	/** Replaces the setting overrides of this coordinator. `{}` restores the defaults. */
	async testConfig(overrides: TransactionCoordinatorDOConfigOverrides): Promise<void> {
		this.#config = overrides;
	}
}
