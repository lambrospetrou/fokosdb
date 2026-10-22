/**
 * Small primitives shared by the PartitionDO suites: isolated partition setup, key encoding,
 * split-status narrowing, and log capture. Split, migration, and promotion drivers live in
 * `partition-harness.ts`.
 */
import { runInDurableObject } from "cloudflare:test";
import { vi } from "vitest";
import invariant from "../../src/shared/invariant.js";
import { testPartitionStub } from "../stub-helpers.js";
export { conditionTree } from "../../src/shared/expression/test-fixtures.js";
import { createTableConfig, type FokosTableIdentity, type FokosTableOptions } from "../../src/shared/partition-context.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { FokosRouter } from "../../src/sharding/router.js";
import type { ItemRpcPartitions, LeafMetrics, PartitionDO, PartitionOps, SplitStatusView } from "../../src/server/do-partition.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import type { TransactionItem } from "../../src/shared/transaction-wire-types.js";
import type { QueryItemsResult, SinglePartitionMeta } from "../../src/shared/types.js";
import type { FokosEnvelope } from "../../src/sharding/runtime-types.js";
import { answerMeta } from "../../src/shared/partition-meta.js";

export const kb = (s?: string) => KeyCodec.encodeOptional(s);

// ─── opening the envelope ─────────────────────────────────────────────────────

/**
 * A partition response as a test reads it: the entries of an item or a query response folded into
 * `meta`, with the names from the routing of the envelope, as `db.ts` builds the public meta. A
 * transaction response keeps its wire list, and any other response is the value itself.
 */
export type Opened<R> = R extends { partitions: LeafMetrics[]; nextCursor: unknown }
	? Omit<R, "partitions"> & { meta: QueryItemsResult["meta"] }
	: R extends { partitions: ItemRpcPartitions }
		? Omit<R, "partitions"> & { meta: SinglePartitionMeta }
		: R;

export function opened<R>(envelope: FokosEnvelope<R>): Opened<R> {
	const { value, routing } = envelope;
	if (typeof value !== "object" || value === null || !("partitions" in value)) {
		return value as Opened<R>;
	}
	const { partitions, ...rest } = value as {
		partitions: Array<LeafMetrics | ItemRpcPartitions[number]>;
		nextCursor?: unknown;
		rowsReturned?: number;
	};
	// An item or a query entry has counts. A transaction entry has none.
	if (partitions[0]?.rowsRead === undefined) {
		return value as Opened<R>;
	}
	const meta = answerMeta(partitions, routing);
	if ("nextCursor" in rest) {
		return { ...rest, meta: { ...meta, rowsReturned: rest.rowsReturned } } as Opened<R>;
	}
	return { ...rest, meta } as Opened<R>;
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

/** The keys of the lock rows of one transaction on one partition, as `"<hash>/<sort>"`, in storage order. */
export async function lockKeys(stub: DurableObjectStub<PartitionDO>, transactionId: string): Promise<string[]> {
	return await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) =>
		new PartitionStore(state.storage)
			.listPendingTxItems(transactionId)
			.map((row) => `${new TextDecoder().decode(row.hk)}/${new TextDecoder().decode(row.sk)}`),
	);
}

export type SplitStartedOrCompleted = Extract<SplitStatusView, { status: "split_started" | "split_completed" }>;
/** The options of a test table. The fields of `table` are flat, next to the other options. */
export type PartitionOptions = Partial<
	Omit<FokosTableIdentity, "name" | "coordinatorRootsN"> & Omit<FokosTableOptions, "table"> & { tableName: string }
>;

export function makeStub(opts?: PartitionOptions) {
	const { tableName, ns, nsTx, nsMat, rootTreesN, hashSplitN, jurisdiction, ...rest } = opts ?? {};
	const base = createTableConfig({
		table: {
			name: tableName ?? `test.${crypto.randomUUID()}`,
			ns: ns ?? "PARTITION_DO",
			nsTx: nsTx ?? "TRANSACTION_COORDINATOR_DO",
			rootTreesN: rootTreesN ?? 1,
			hashSplitN: hashSplitN ?? 2,
			jurisdiction,
			nsMat: nsMat ?? (rest.captureChanges ? "MATERIALIZATION_DO" : undefined),
		},
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
		...rest,
	});
	const ctx = new FokosRouter(base.topology, base.rangeConfig, base.policy, base.policyVersion).rootContext(kb("dummyHashKey"));
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
