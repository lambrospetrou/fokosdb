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
import type { FokosDbRouteContext } from "../src/shared/partition-context.js";
import type { FokosInitRequest, FokosMigrationPage, FokosMigrationPullRequest } from "../src/sharding/repartition-types.js";

export type MigrationStream = "overrides" | "items" | "pending_tx";

/**
 * The pulls that a gate holds. `target` limits the gate to the pulls of one target. With
 * `afterRead`, the source builds the page before the hold, thus the page carries the state of that
 * moment. Without it, the source builds the page after the release.
 */
export type PullGateSpec = { stream: MigrationStream; target?: string; afterRead?: boolean };

export type PullStats = { calls: number; heldTargets: string[] };

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
	#pullStats: PullStats = { calls: 0, heldTargets: [] };
	#initGate: Gate | null = null;
	#initCalls = 0;
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
	#staleTransactionMs: number | null = null;

	/**
	 * Logs the call, and applies the rule of `op` when one exists. Else it returns the promise of
	 * `PartitionDO` itself, so a call with no rule has the same timing as on `PartitionDO`.
	 */
	#tx<Op extends TxOp>(op: Op, req: TxRequest<Op>, work: () => Promise<TxResponse<Op>>): Promise<TxResponse<Op>> {
		(this.#txCalls[op] as TxRequest<Op>[]).push(req);
		const rule = this.#txRules[op] as TxResponseRule<Op> | undefined;
		if (!rule) return work();
		if (rule.times !== undefined && --rule.times <= 0) delete this.#txRules[op];
		return "error" in rule ? Promise.reject(new Error(rule.error)) : Promise.resolve(rule.value);
	}

	override txReadSnapshot(ctx: FokosDbRouteContext, req: TxRequest<"txReadSnapshot">) {
		return this.#tx("txReadSnapshot", req, () => super.txReadSnapshot(ctx, req));
	}

	override txReadForTransaction(ctx: FokosDbRouteContext, req: TxRequest<"txReadForTransaction">) {
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

	override txExecuteSingleShot(ctx: FokosDbRouteContext, req: TxRequest<"txExecuteSingleShot">) {
		return this.#tx("txExecuteSingleShot", req, () => super.txExecuteSingleShot(ctx, req));
	}

	override async txPrepare(ctx: FokosDbRouteContext, req: Parameters<PartitionDO["txPrepare"]>[1]) {
		const response = await super.txPrepare(ctx, req);
		const prepareGate = this.#prepareGate;
		if (prepareGate && !prepareGate.parked) {
			prepareGate.parked = true;
			await prepareGate.held;
		}
		return response;
	}

	override txCommit(ctx: FokosDbRouteContext, req: TxRequest<"txCommit">) {
		return this.#tx("txCommit", req, () => super.txCommit(ctx, req));
	}

	override txCancel(ctx: FokosDbRouteContext, req: TxRequest<"txCancel">) {
		return this.#tx("txCancel", req, () => super.txCancel(ctx, req));
	}

	override fokosStaleTransactionMs(): number {
		return this.#staleTransactionMs ?? super.fokosStaleTransactionMs();
	}

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
		return page ?? (await super.fokosMigrationPull(req));
	}

	override async fokosInit(req: FokosInitRequest): Promise<void> {
		await super.fokosInit(req);
		this.#initCalls++;
		if (this.#initGate) await this.#initGate.held;
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

	/** Replaces the stale-transaction time of this partition. `null` restores the shipped value. */
	async testStaleTransactionMs(ms: number | null): Promise<void> {
		this.#staleTransactionMs = ms;
	}
}
