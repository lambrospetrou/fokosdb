/**
 * Small primitives shared by the PartitionDO suites: isolated partition setup, key encoding,
 * split-status narrowing, and log capture. Split, migration, and promotion drivers live in
 * `partition-harness.ts`.
 */
import { env } from "cloudflare:workers";
import { vi } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import invariant from "../../src/shared/invariant.js";
import { compileConditionExpression } from "../../src/shared/expression/compiler.js";
import type { ConditionExpression } from "../../src/shared/expression/types.js";
import { PartitionContextCreator } from "../../src/shared/partition-topology/partition-context.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import { PartitionTopologyRouterImpl } from "../../src/shared/partition-topology/router.js";
import type { SplitStatusKVItem } from "../../src/shared/partition-topology/split-state.js";

export const kb = (s?: string) => KeyCodec.encodeOptional(s);
export const compiledCondition = (condition: ConditionExpression) => compileConditionExpression(condition);

export type SplitStartedOrCompleted = Extract<SplitStatusKVItem, { status: "split_started" | "split_completed" }>;
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
	const ctx = new PartitionTopologyRouterImpl(base).pickPartition(kb("dummyHashKey")).partitionContext;
	return { ctx, stub: PartitionDO.getByName(env.PARTITION_DO, ctx.doName) };
}

/**
 * Narrows an already-read split status to a started or completed split. Tests that reach for
 * `childPartitionContexts` need that narrowing; failing here reports the status the partition was
 * actually in, instead of surfacing an `undefined` several lines later.
 */
export function expectSplitStatus(status: SplitStatusKVItem | undefined, doName?: string): SplitStartedOrCompleted {
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
