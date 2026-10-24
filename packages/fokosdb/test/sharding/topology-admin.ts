import { FokosShardingAutoAdminDO, type FokosTopologyKeeper, type FokosTopologyKeeperOptions } from "../../src/sharding/index-server.js";

/** The test build delay: short, so a test waits little for a build. */
export const TEST_BUILD_DELAY_MS = 50;

/** `FokosShardingAutoAdminDO` with a short build delay. A test reaches the keeper through `runInDurableObject`. */
export class TestTopologyAdminDO extends FokosShardingAutoAdminDO<Env> {
	protected keeperOptions(): Omit<FokosTopologyKeeperOptions, "scheduleBuild"> {
		return { buildDelayMs: TEST_BUILD_DELAY_MS };
	}

	/** The keeper, for a test that calls it directly inside the object. */
	get testKeeper(): FokosTopologyKeeper {
		return this.keeper;
	}
}
