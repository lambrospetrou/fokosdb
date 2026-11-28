/**
 * A `FokosDBPartitionDO` that uses the fake snapshot API of `fake-snapshots.ts`. The class adds no
 * behavior. It applies a scheduled restore before the production constructor runs, and its test
 * controls call the same `ctx.storage` methods that the production code calls.
 */
import { FokosDBPartitionDO } from "../../src/server/do-partition.js";
import { installFakeSnapshots, type FakeSnapshotHandle } from "./fake-snapshots.js";

type SnapshotStorage = {
	snapshot(): Promise<FakeSnapshotHandle>;
	onNextSessionRestore(target: FakeSnapshotHandle, options?: { kvData?: Record<string, unknown> }): Promise<string>;
};

export class SnapshotPartitionDO extends FokosDBPartitionDO {
	constructor(ctx: DurableObjectState, env: Env) {
		installFakeSnapshots(ctx, env);
		super(ctx, env);
	}

	#storage(): SnapshotStorage {
		return this.ctx.storage as unknown as SnapshotStorage;
	}

	async debugSnapshot(): Promise<FakeSnapshotHandle> {
		return await this.#storage().snapshot();
	}

	async debugRestoreFrom(snapshot: FakeSnapshotHandle, kvData?: Record<string, unknown>): Promise<string> {
		return await this.#storage().onNextSessionRestore(snapshot, { kvData });
	}

	/** Evicts this instance, as a restart does. The next call starts a new instance. */
	async debugRestart(): Promise<void> {
		this.ctx.abort("snapshot test restart");
	}
}
