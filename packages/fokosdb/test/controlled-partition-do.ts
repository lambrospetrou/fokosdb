/**
 * A `PartitionDO` with test controls: RPCs that a test calls to hold, count, answer, or fail one
 * call on one instance.
 *
 * The RPC dispatcher finds an operation on the class, thus a test cannot put a mock on one DO
 * instance. A mock on the `PartitionDO` prototype reaches every partition of every test in the
 * isolate, and `vi.restoreAllMocks()` of a different test can remove it. Each test control of this class is
 * a field of one instance, and nothing else can reach it.
 *
 * The class adds no behavior of its own: `PartitionDO` does all the work, and each override only
 * holds, counts, answers, or fails one call, or replaces one tuning value.
 */
import { PartitionDO } from "../src/server/do-partition.js";
import type { FokosDBRouteContext } from "../src/shared/partition-context.js";
import type { FokosRuntimeConfigOverrides } from "../src/sharding/runtime-config.js";
import type { PartitionDOConfigOverrides } from "../src/server/host-config.js";
import type {
	FokosInitRequest,
	FokosMigrationAckRequest,
	FokosMigrationPage,
	FokosMigrationPullRequest,
} from "../src/sharding/repartition-types.js";

export type MigrationStream = "overrides" | "items" | "pending_tx";

/**
 * The pulls that a gate holds. `target` limits the gate to the pulls of one target. With
 * `afterRead`, the source builds the page before the hold, thus the page carries the state of that
 * moment. Without it, the source builds the page after the release.
 */
export type PullGateSpec = { stream: MigrationStream; target?: string; afterRead?: boolean };

/** One served pull: its target, its stream, when the source received and answered it, and the rows of its page. */
export type PullRecord = { target: string; stream: MigrationStream; receivedAt: number; answeredAt: number; rows: number };

export type PullStats = { calls: number; heldTargets: string[]; pulls: PullRecord[] };

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
	if (req.cursor === null || req.cursor.phase === "overrides") {
		return "overrides";
	}
	return (req.cursor.inner as { stream?: string } | null)?.stream === "pending_tx" ? "pending_tx" : "items";
}

/** The rows of one host page: its items or its locks. */
function pageRows(page: unknown): number {
	const p = page as { items?: unknown[]; pendingTransactions?: unknown[] };
	return (p.items ?? p.pendingTransactions ?? []).length;
}

/** The transaction operations that this class logs, and that a test can make answer or fail. */
export type TxOp = "txReadSnapshot" | "txReadForTransaction" | "txExecuteSingleShot" | "txCommit" | "txCancel";
export type TxRequest<Op extends TxOp> = Parameters<PartitionDO[Op]>[1];
type TxResponse<Op extends TxOp> = Awaited<ReturnType<PartitionDO[Op]>>;

/**
 * What an operation does in place of its work, for `times` calls. `error` fails the call with that
 * message, and `value` answers the call with that value.
 */
export type TxResponseRule<Op extends TxOp> = ({ error: string } | { value: TxResponse<Op> }) & { times?: number };

export class ControlledPartitionDO extends PartitionDO {
	#pullGate: (Gate & { spec: PullGateSpec }) | null = null;
	#pullStats: PullStats = { calls: 0, heldTargets: [], pulls: [] };
	#initGate: Gate | null = null;
	#initCalls = 0;
	#refuseAcks = false;
	#txCalls: { [Op in TxOp]: TxRequest<Op>[] } = {
		txReadSnapshot: [],
		txReadForTransaction: [],
		txExecuteSingleShot: [],
		txCommit: [],
		txCancel: [],
	};
	#txRules: { [Op in TxOp]?: TxResponseRule<Op> } = {};
	#readGate: (Gate & { parked: boolean }) | null = null;
	#prepareGate: (Gate & { parked: boolean }) | null = null;
	#config: PartitionDOConfigOverrides = {};
	#runtimeConfig: FokosRuntimeConfigOverrides = {};

	/**
	 * Logs the call, and applies the rule of `op` when one exists. Else it returns the promise of
	 * `PartitionDO` itself, so a call with no rule has the same timing as on `PartitionDO`.
	 */
	#tx<Op extends TxOp>(op: Op, req: TxRequest<Op>, work: () => Promise<TxResponse<Op>>): Promise<TxResponse<Op>> {
		(this.#txCalls[op] as TxRequest<Op>[]).push(req);
		const rule = this.#txRules[op] as TxResponseRule<Op> | undefined;
		if (!rule) {
			return work();
		}
		if (rule.times !== undefined && --rule.times <= 0) {
			delete this.#txRules[op];
		}
		return "error" in rule ? Promise.reject(new Error(rule.error)) : Promise.resolve(rule.value);
	}

	override txReadSnapshot(ctx: FokosDBRouteContext, req: TxRequest<"txReadSnapshot">) {
		return this.#tx("txReadSnapshot", req, () => super.txReadSnapshot(ctx, req));
	}

	override txReadForTransaction(ctx: FokosDBRouteContext, req: TxRequest<"txReadForTransaction">) {
		return this.#tx("txReadForTransaction", req, async () => {
			const response = await super.txReadForTransaction(ctx, req);
			const readGate = this.#readGate;
			if (readGate && !readGate.parked) {
				readGate.parked = true;
				await readGate.held;
			}
			return response;
		});
	}

	override txExecuteSingleShot(ctx: FokosDBRouteContext, req: TxRequest<"txExecuteSingleShot">) {
		return this.#tx("txExecuteSingleShot", req, () => super.txExecuteSingleShot(ctx, req));
	}

	override async txPrepare(ctx: FokosDBRouteContext, req: Parameters<PartitionDO["txPrepare"]>[1]) {
		const response = await super.txPrepare(ctx, req);
		const prepareGate = this.#prepareGate;
		if (prepareGate && !prepareGate.parked) {
			prepareGate.parked = true;
			await prepareGate.held;
		}
		return response;
	}

	override txCommit(ctx: FokosDBRouteContext, req: TxRequest<"txCommit">) {
		return this.#tx("txCommit", req, () => super.txCommit(ctx, req));
	}

	override txCancel(ctx: FokosDBRouteContext, req: TxRequest<"txCancel">) {
		return this.#tx("txCancel", req, () => super.txCancel(ctx, req));
	}

	// The constructor of PartitionDO reads both methods below, before the fields of this class exist.
	protected override fokosConfig(): PartitionDOConfigOverrides {
		return #config in this ? this.#config : super.fokosConfig();
	}

	protected override fokosRuntimeConfig(): FokosRuntimeConfigOverrides {
		return #runtimeConfig in this ? this.#runtimeConfig : super.fokosRuntimeConfig();
	}

	override async fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage> {
		this.#pullStats.calls++;
		const pullGate = this.#pullGate;
		const matches =
			pullGate !== null && streamOf(req) === pullGate.spec.stream && (pullGate.spec.target ?? req.target.doName) === req.target.doName;
		let page: FokosMigrationPage | undefined;
		if (matches) {
			if (pullGate.spec.afterRead) {
				page = await super.fokosMigrationPull(req);
			}
			if (!this.#pullStats.heldTargets.includes(req.target.doName)) {
				this.#pullStats.heldTargets.push(req.target.doName);
			}
			await pullGate.held;
		}
		const receivedAt = Date.now();
		page ??= await super.fokosMigrationPull(req);
		const rows = page.phase === "overrides" ? page.overrides.length : pageRows(page.page);
		this.#pullStats.pulls.push({ target: req.target.doName, stream: streamOf(req), receivedAt, answeredAt: Date.now(), rows });
		return page;
	}

	override async fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void> {
		if (this.#refuseAcks) {
			throw new Error("the test refuses the acknowledgement");
		}
		return await super.fokosMigrationAck(req);
	}

	override async fokosInit(req: FokosInitRequest): Promise<void> {
		await super.fokosInit(req);
		this.#initCalls++;
		if (this.#initGate) {
			await this.#initGate.held;
		}
	}

	/** Holds each pull that matches `spec` until `testReleasePulls`. */
	async testHoldPulls(spec: PullGateSpec): Promise<void> {
		this.#pullGate = { ...gate(), spec };
	}

	/** Releases the held pulls, and removes the gate. */
	async testReleasePulls(): Promise<void> {
		this.#pullGate?.release();
		this.#pullGate = null;
	}

	async testPullStats(): Promise<PullStats> {
		return this.#pullStats;
	}

	/**
	 * Refuses each acknowledgement that a target sends to this source while `refuse` is true. A
	 * target then stays imported, and the repartition stays in `cutover`.
	 */
	async testRefuseAcks(refuse: boolean): Promise<void> {
		this.#refuseAcks = refuse;
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

	/** The requests of `op` that this partition received, in order of arrival. */
	async testTxCalls<Op extends TxOp>(op: Op): Promise<TxRequest<Op>[]> {
		return this.#txCalls[op];
	}

	/** Makes `op` answer or fail as `rule` says, until `testClearTxResponse` or until `rule.times` calls. */
	async testTxResponse<Op extends TxOp>(op: Op, rule: TxResponseRule<Op>): Promise<void> {
		(this.#txRules as Record<Op, TxResponseRule<Op>>)[op] = { ...rule };
	}

	async testClearTxResponse(op: TxOp): Promise<void> {
		delete this.#txRules[op];
	}

	/**
	 * Holds the next `txReadForTransaction` after it reads, until `testReleaseReadPhase`. The read of a
	 * two-phase transaction is then complete, and its answer has not returned.
	 */
	async testHoldReadPhase(): Promise<void> {
		this.#readGate = { ...gate(), parked: false };
	}

	async testReadPhaseParked(): Promise<boolean> {
		return this.#readGate?.parked ?? false;
	}

	async testReleaseReadPhase(): Promise<void> {
		this.#readGate?.release();
		this.#readGate = null;
	}

	/**
	 * Holds the answer of the next `txPrepare` after the prepare applies, until `testReleasePrepare`.
	 * The lock is then written, and the coordinator still waits for the answer.
	 */
	async testHoldPrepare(): Promise<void> {
		this.#prepareGate = { ...gate(), parked: false };
	}

	async testPrepareParked(): Promise<boolean> {
		return this.#prepareGate?.parked ?? false;
	}

	async testReleasePrepare(): Promise<void> {
		this.#prepareGate?.release();
		this.#prepareGate = null;
	}

	/** Replaces the runtime setting overrides of this partition. `{}` restores the defaults. */
	async testRuntimeConfig(overrides: FokosRuntimeConfigOverrides): Promise<void> {
		this.#runtimeConfig = overrides;
	}

	/** Replaces the setting overrides of this partition. `{}` restores the defaults. */
	async testConfig(overrides: PartitionDOConfigOverrides): Promise<void> {
		this.#config = overrides;
		// `PartitionDO` caches its settings, and its constructor fills the cache before this class has
		// its fields. The next read resolves the settings again, with these overrides.
		(this as unknown as { __cachedConfig?: unknown }).__cachedConfig = undefined;
	}
}
