/**
 * A `PartitionDO` with seams that a test drives through its own RPCs.
 *
 * The RPC dispatcher finds an operation on the class, thus a test cannot put a mock on one DO
 * instance. A mock on the `PartitionDO` prototype reaches every partition of every test in the
 * isolate, and `vi.restoreAllMocks()` of a different test can remove it. Each seam of this class is
 * a field of one instance, and nothing else can reach it.
 *
 * The class adds no behavior of its own: `PartitionDO` does all the work, and each override only
 * holds, counts, truncates, or fails one call.
 */
import { PartitionDO } from "../src/server/do-partition.js";
import type { FokosDbRouteContext } from "../src/shared/partition-context.js";
import type { FokosDbHostPage } from "../src/shared/partition/fokos-migration-host.js";
import type { FokosInitRequest, FokosMigrationPage, FokosMigrationPullRequest } from "../src/sharding/repartition-types.js";

export type MigrationStream = "overrides" | "items" | "pending_tx";

/**
 * The pulls that a gate holds. `target` limits the gate to the pulls of one target. With
 * `afterRead`, the source builds the page before the hold, thus the page carries the state of that
 * moment. Without it, the source builds the page after the release.
 */
export type PullGateSpec = { stream: MigrationStream; target?: string; afterRead?: boolean };

export type PullStats = { calls: number; truncated: number; heldTargets: string[] };

type Gate = { held: Promise<void>; release: () => void };

function gate(): Gate {
	let release!: () => void;
	const held = new Promise<void>((resolve) => {
		release = resolve;
	});
	return { held, release };
}

/** The stream a pull asks for, read from the opaque cursor of the flow. */
export function streamOf(req: FokosMigrationPullRequest): MigrationStream {
	if (req.cursor === null || req.cursor.phase === "overrides") return "overrides";
	return (req.cursor.inner as { stream?: string } | null)?.stream === "pending_tx" ? "pending_tx" : "items";
}

export class ControlledPartitionDO extends PartitionDO {
	#pullGate: (Gate & { spec: PullGateSpec }) | null = null;
	#pullCap: number | null = null;
	#pullStats: PullStats = { calls: 0, truncated: 0, heldTargets: [] };
	#initGate: Gate | null = null;
	#initCalls = 0;
	#failCommits = 0;

	override async fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage> {
		this.#pullStats.calls++;
		const pullGate = this.#pullGate;
		const matches =
			pullGate !== null && streamOf(req) === pullGate.spec.stream && (pullGate.spec.target ?? req.target.doName) === req.target.doName;
		let page: FokosMigrationPage | undefined;
		if (matches) {
			if (pullGate.spec.afterRead) page = await super.fokosMigrationPull(req);
			if (!this.#pullStats.heldTargets.includes(req.target.doName)) this.#pullStats.heldTargets.push(req.target.doName);
			await pullGate.held;
		}
		page ??= await super.fokosMigrationPull(req);
		const capped = this.#pullCap === null ? null : capPage(page, this.#pullCap);
		if (capped) this.#pullStats.truncated++;
		return capped ?? page;
	}

	override async fokosInit(req: FokosInitRequest): Promise<void> {
		await super.fokosInit(req);
		this.#initCalls++;
		if (this.#initGate) await this.#initGate.held;
	}

	override txCommit(ctx: FokosDbRouteContext, req: Parameters<PartitionDO["txCommit"]>[1]): ReturnType<PartitionDO["txCommit"]> {
		if (this.#failCommits > 0) {
			this.#failCommits--;
			return Promise.reject(new Error("simulated child commit failure"));
		}
		return super.txCommit(ctx, req);
	}

	/** Holds each pull that matches `spec` until `testReleasePulls`. */
	async testHoldPulls(spec: PullGateSpec): Promise<void> {
		this.#pullGate = { ...gate(), spec };
	}

	/** Caps each page this source serves at `maxRows` rows, until `testReleasePulls`. */
	async testCapPulls(maxRows: number): Promise<void> {
		this.#pullCap = maxRows;
	}

	/** Releases the held pulls, and removes the gate and the cap. */
	async testReleasePulls(): Promise<void> {
		this.#pullGate?.release();
		this.#pullGate = null;
		this.#pullCap = null;
	}

	async testPullStats(): Promise<PullStats> {
		return this.#pullStats;
	}

	/** Holds each `fokosInit` on this partition after it applies, until `testReleaseInit`. */
	async testHoldInit(): Promise<void> {
		this.#initGate = gate();
	}

	async testReleaseInit(): Promise<void> {
		this.#initGate?.release();
		this.#initGate = null;
	}

	async testInitCalls(): Promise<number> {
		return this.#initCalls;
	}

	/** Makes the next `count` calls of `txCommit` on this partition fail before they apply. */
	async testFailCommits(count: number): Promise<void> {
		this.#failCommits = count;
	}
}

/**
 * Truncates one page to `maxRows` rows and points its cursor at the last row it kept. It returns null
 * when the page already fits. The flow owns the overrides phase and the host owns its own streams, so
 * each one carries its own row shape and its own cursor.
 */
function capPage(page: FokosMigrationPage, maxRows: number): FokosMigrationPage | null {
	if (page.phase === "overrides") {
		if (page.overrides.length <= maxRows) return null;
		const kept = page.overrides.slice(0, maxRows);
		return { phase: "overrides", overrides: kept, nextCursor: { phase: "overrides", inner: { hashKey: kept[maxRows - 1].hashKey } } };
	}
	const hostPage = page.page as FokosDbHostPage;
	if (hostPage.stream === "items") {
		if (hostPage.items.length <= maxRows) return null;
		const kept = hostPage.items.slice(0, maxRows);
		const last = kept[maxRows - 1];
		return {
			phase: "host",
			page: { stream: "items", items: kept },
			nextCursor: { phase: "host", inner: { stream: "items", cursor: { hk: last.hk, sk: last.sk } } },
		};
	}
	if (hostPage.pendingTransactions.length <= maxRows) return null;
	const kept = hostPage.pendingTransactions.slice(0, maxRows);
	const last = kept[maxRows - 1];
	return {
		phase: "host",
		page: { ...hostPage, pendingTransactions: kept },
		nextCursor: {
			phase: "host",
			inner: { stream: "pending_tx", cursor: { hk: last.hk, sk: last.sk, transaction_id: last.transaction_id } },
		},
	};
}
