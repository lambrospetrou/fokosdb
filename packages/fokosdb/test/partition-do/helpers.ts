/**
 * Small primitives shared by the PartitionDO suites: isolated partition setup, key encoding,
 * split-status narrowing, and log capture. Split, migration, and promotion drivers live in
 * `partition-harness.ts`.
 */
import { vi } from "vitest";
import invariant from "../../src/shared/invariant.js";
import { testPartitionStub } from "../stub-helpers.js";
import { compileConditionExpression } from "../../src/shared/expression/compiler.js";
import type { ConditionExpression } from "../../src/shared/expression/types.js";
import { PartitionContextCreator } from "../../src/shared/partition-context.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { FokosRouter } from "../../src/sharding/router.js";
import type { PartitionDO, PartitionOps, SplitStatusView } from "../../src/server/do-partition.js";
import type { TransactionItem } from "../../src/shared/transaction-wire-types.js";
import type { OperationMetrics, PartitionInfo } from "../../src/shared/types.js";
import type { FokosEnvelope, FokosPublicRouting } from "../../src/sharding/runtime-types.js";
import { leafPartitionInfo, partitionInfoOf } from "../../src/client/partition-info.js";

export const kb = (s?: string) => KeyCodec.encodeOptional(s);
export const compiledCondition = (condition: ConditionExpression) => compileConditionExpression(condition);

// ─── opening the envelope ─────────────────────────────────────────────────────

type PublicMeta = OperationMetrics & PartitionInfo;

/**
 * A partition response as a test reads it: the value with the routing of its envelope folded into
 * `meta`, and each leaf of `partitionMetas` paired with the route list, as `db.ts` builds the public
 * meta. A response with no `meta` is the value itself.
 */
export type Opened<R> = R extends { partitionMetas: unknown[] }
	? Omit<R, "partitionMetas"> & { meta: PublicMeta; partitionMetas: PublicMeta[] }
	: R extends { meta: OperationMetrics }
		? Omit<R, "meta"> & { meta: PublicMeta }
		: R;

export function opened<R>(envelope: FokosEnvelope<R>): Opened<R> {
	const { value, routing } = envelope;
	const pub: FokosPublicRouting = { servedBy: routing.servedBy, forwardCount: routing.forwardCount };
	if (typeof value !== "object" || value === null) {
		return value as Opened<R>;
	}
	if ("partitionMetas" in value && Array.isArray(value.partitionMetas)) {
		const leaves = value.partitionMetas as Array<OperationMetrics & { partitionId: string }>;
		const metrics = leaves.reduce(
			(sum, leaf) => ({ rowsRead: sum.rowsRead + leaf.rowsRead, rowsWritten: sum.rowsWritten + leaf.rowsWritten, databaseSize: 0 }),
			{ rowsRead: 0, rowsWritten: 0, databaseSize: 0 },
		);
		return {
			...value,
			meta: { ...metrics, ...partitionInfoOf(pub) },
			partitionMetas: leaves.flatMap((leaf) => leafPartitionInfo(leaf, pub) ?? []),
		} as Opened<R>;
	}
	if ("meta" in value) {
		return { ...value, meta: { ...(value.meta as OperationMetrics), ...partitionInfoOf(pub) } } as Opened<R>;
	}
	return value as Opened<R>;
}

/** The node of the partition that executed the request, which an item RPC has exactly one of. */
export function executedBy(envelope: FokosEnvelope<unknown>) {
	const nodes = envelope.routing.servedBy.filter((node) => node.role === "executed");
	invariant(nodes.length === 1, `expected one executing partition, got ${nodes.map((n) => n.ref.doName).join(", ")}`);
	return nodes[0];
}

/** The bounded ancestor boundaries the executing range partition reported, or none. */
export function rangeAncestorsOf(envelope: FokosEnvelope<unknown>) {
	return executedBy(envelope)._rangeAncestors ?? [];
}

/** The RPC surface of a partition stub with every envelope opened. */
export type OpenedPartitionRpc = {
	[K in keyof PartitionOps]: (...args: Parameters<PartitionDO[K]>) => Promise<Opened<PartitionOps[K]["res"]>>;
};

export function openedRpc(stub: DurableObjectStub<PartitionDO>): OpenedPartitionRpc {
	return new Proxy({} as OpenedPartitionRpc, {
		get:
			(_target, op: keyof PartitionOps) =>
			async (...args: unknown[]) =>
				opened(await (stub[op] as (...a: unknown[]) => Promise<FokosEnvelope<unknown>>)(...args)),
	});
}

/**
 * Stamps the request-order opIndex onto prepare and single-shot items, as db.ts and the transaction
 * coordinator do for a real request. Every result carries the index back, so a test that omits it
 * would not exercise the merge the production paths run.
 */
export function withOpIndex(items: Omit<TransactionItem, "opIndex">[]): TransactionItem[] {
	return items.map((item, i) => ({ ...item, opIndex: i }));
}

export type SplitStartedOrCompleted = Extract<SplitStatusView, { status: "split_started" | "split_completed" }>;
export type PartitionOptions = Partial<Parameters<typeof PartitionContextCreator.create>[0]>;

export function makeStub(opts?: PartitionOptions) {
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: `test.${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
		...opts,
	});
	const ctx = new FokosRouter(base.topology, base.rangeConfig, base.policy).rootContext(kb("dummyHashKey"));
	const stub = testPartitionStub(ctx.doName, ctx.policy.ns);
	return { ctx, stub, rpc: openedRpc(stub) };
}

/**
 * Narrows an already-read split status to a started or completed split. Tests that reach for
 * `childPartitionContexts` need that narrowing; failing here reports the status the partition was
 * actually in, instead of surfacing an `undefined` several lines later.
 */
export function expectSplitStatus(status: SplitStatusView | undefined, doName?: string): SplitStartedOrCompleted {
	invariant(
		status?.status === "split_started" || status?.status === "split_completed",
		`${doName ?? "partition"}: expected a started or completed split, got ${status?.status ?? "none"}`,
	);
	return status;
}

/** A structured log entry as the partition DO writes it to console.error. */
export type LoggedEntry = { message?: string; transactionId?: string; [key: string]: unknown };

/**
 * Silences console.error for the test and collects what was written, so a test can assert on the
 * DO's structured logs without the entries reaching the run output.
 */
export function captureConsoleError() {
	const spy = vi.spyOn(console, "error").mockImplementation(() => {});
	return {
		spy,
		/** Every entry logged with this `message`, in the order it was logged. */
		withMessage(message: string): LoggedEntry[] {
			return spy.mock.calls.map(([entry]) => entry as LoggedEntry).filter((log) => log.message === message);
		},
	};
}
