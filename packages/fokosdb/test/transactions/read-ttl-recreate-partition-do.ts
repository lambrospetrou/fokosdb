import type { FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { ControlledPartitionDO, type TxRequest } from "../controlled-partition-do.js";

type ReadGate = { held: Promise<void>; release: () => void; parked: boolean };

/** A partition with one test-only hold before its next transactional read. */
export class ReadTtlRecreatePartitionDO extends ControlledPartitionDO {
	#beforeRead: ReadGate | null = null;

	override async txReadForTransaction(ctx: FokosDBRouteContext, req: TxRequest<"txReadForTransaction">) {
		const beforeRead = this.#beforeRead;
		if (beforeRead && !beforeRead.parked) {
			beforeRead.parked = true;
			await beforeRead.held;
		}
		return await super.txReadForTransaction(ctx, req);
	}

	async testHoldReadBefore(): Promise<void> {
		let release!: () => void;
		const held = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.#beforeRead = { held, release, parked: false };
	}

	async testReadBeforeParked(): Promise<boolean> {
		return this.#beforeRead?.parked ?? false;
	}

	async testReleaseReadBefore(): Promise<void> {
		this.#beforeRead?.release();
		this.#beforeRead = null;
	}
}
