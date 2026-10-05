import { PartitionDO } from "fokosdb/server";

type ReadSide = "before" | "after";
type Gate = { wait: Promise<void>; release: () => void; parked: boolean };

function makeGate(): Gate {
	let release!: () => void;
	const wait = new Promise<void>((resolve) => {
		release = resolve;
	});
	return { wait, release, parked: false };
}

/** A test-only partition that pauses a transactional read before or after its storage read. */
export class ReproPartitionDO extends PartitionDO {
	#before: Gate | null = null;
	#after: Gate | null = null;

	override async txReadForTransaction(...args: Parameters<PartitionDO["txReadForTransaction"]>) {
		const before = this.#before;
		if (before && !before.parked) {
			before.parked = true;
			await before.wait;
		}
		const response = await super.txReadForTransaction(...args);
		const after = this.#after;
		if (after && !after.parked) {
			after.parked = true;
			await after.wait;
		}
		return response;
	}

	async holdRead(side: ReadSide): Promise<void> {
		if (side === "before") this.#before = makeGate();
		else this.#after = makeGate();
	}

	async readParked(side: ReadSide): Promise<boolean> {
		return (side === "before" ? this.#before : this.#after)?.parked ?? false;
	}

	async releaseRead(side: ReadSide): Promise<void> {
		const gate = side === "before" ? this.#before : this.#after;
		gate?.release();
		if (side === "before") this.#before = null;
		else this.#after = null;
	}
}
