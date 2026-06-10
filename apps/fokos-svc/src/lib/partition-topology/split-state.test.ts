import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../do-partition.js";
import { PartitionContextCreator, type PartitionContextResolved } from "./partition-context.js";
import { PartitionIdHelper } from "./partition-id.js";
import { SplitStateMachine } from "./split-state.js";

// Runs `fn` against a SplitStateMachine over REAL Durable Object KV storage (vitest-pool-workers).
async function withSplitState(fn: (machine: SplitStateMachine, pCtx: PartitionContextResolved) => void | Promise<void>): Promise<void> {
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		databaseName: `splitstate-${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
	});
	const { opaque, doName } = PartitionIdHelper.fromHashIdxs(base, [0]).encode(true);
	const pCtx: PartitionContextResolved = { ...base, doName: doName!, primaryDoIdStr: "", partitionId: opaque };

	const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(doName!));
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		await fn(new SplitStateMachine(state.storage, "__split_status"), pCtx);
	});
}

function makeChildren(pCtx: PartitionContextResolved, n: number): PartitionContextResolved[] {
	return PartitionIdHelper.calculateHashChildPartitionIds(pCtx)
		.slice(0, n)
		.map(({ doName, partitionIdOpaque }) => ({
			...pCtx,
			doName,
			primaryDoIdStr: "",
			partitionId: partitionIdOpaque,
		}));
}

describe("SplitStateMachine", () => {
	it("starts with no status; queueSplit writes split_queued exactly once", async () => {
		await withSplitState((machine, pCtx) => {
			expect(machine.splitStatus()).toBeUndefined();
			expect(machine.childPartitionContexts()).toBeUndefined();

			const first = machine.queueSplit("hash", pCtx);
			expect(first.status).toBe("split_queued");
			expect(first.splitType).toBe("hash");
			expect(first.partitionContext.doName).toBe(pCtx.doName);

			// Re-queue is an idempotent no-op: the original record (createdAt) is preserved.
			const again = machine.queueSplit("hash", pCtx);
			expect(again).toEqual(first);
		});
	});

	it("commitSplitStarted transitions split_queued → split_started with children and history", async () => {
		await withSplitState((machine, pCtx) => {
			const queued = machine.queueSplit("hash", pCtx);
			const children = makeChildren(pCtx, 2);
			machine.commitSplitStarted(children);

			const status = machine.splitStatus();
			expect(status?.status).toBe("split_started");
			if (status?.status !== "split_started") throw new Error("unreachable");
			expect(status.childPartitionContexts.map((c) => c.doName)).toEqual(children.map((c) => c.doName));
			expect(status.migratedChildDoNames).toEqual([]);
			expect(status.history).toEqual([
				{ status: "split_queued", splitType: "hash", createdAt: queued.createdAt, partitionContext: queued.partitionContext },
			]);
			expect(machine.childPartitionContexts()?.map((c) => c.doName)).toEqual(children.map((c) => c.doName));
		});
	});

	it("commitSplitStarted requires an existing status and is idempotent once started", async () => {
		await withSplitState((machine, pCtx) => {
			const children = makeChildren(pCtx, 2);
			expect(() => machine.commitSplitStarted(children)).toThrow(/splitStatus must exist/);

			machine.queueSplit("hash", pCtx);
			machine.commitSplitStarted(children);
			const after = machine.splitStatus();

			// Second commit (concurrent run lost the race) — no-op, no duplicated history.
			machine.commitSplitStarted(children.slice(0, 1));
			expect(machine.splitStatus()).toEqual(after);
		});
	});

	it("acknowledgeChildMigration collects acks and completes when every child acked", async () => {
		await withSplitState((machine, pCtx) => {
			machine.queueSplit("hash", pCtx);
			const children = makeChildren(pCtx, 2);
			machine.commitSplitStarted(children);

			machine.acknowledgeChildMigration(children[0].doName);
			let status = machine.splitStatus();
			expect(status?.status).toBe("split_started");
			if (status?.status === "split_started") {
				expect(status.migratedChildDoNames).toEqual([children[0].doName]);
			}

			machine.acknowledgeChildMigration(children[1].doName);
			status = machine.splitStatus();
			expect(status?.status).toBe("split_completed");
			if (status?.status === "split_completed") {
				expect(status.migratedChildDoNames).toEqual([children[0].doName, children[1].doName]);
				expect(status.history.map((h) => h.status)).toEqual(["split_queued", "split_started"]);
			}
			// childPartitionContexts remains available after completion.
			expect(machine.childPartitionContexts()).toHaveLength(2);
		});
	});

	it("re-acks are idempotent: same child twice does not complete the split", async () => {
		await withSplitState((machine, pCtx) => {
			machine.queueSplit("hash", pCtx);
			const children = makeChildren(pCtx, 2);
			machine.commitSplitStarted(children);

			machine.acknowledgeChildMigration(children[0].doName);
			machine.acknowledgeChildMigration(children[0].doName);
			const status = machine.splitStatus();
			expect(status?.status).toBe("split_started");
			if (status?.status === "split_started") {
				expect(status.migratedChildDoNames).toEqual([children[0].doName]);
			}
		});
	});

	it("acks after completion are idempotent no-ops", async () => {
		await withSplitState((machine, pCtx) => {
			machine.queueSplit("hash", pCtx);
			const children = makeChildren(pCtx, 2);
			machine.commitSplitStarted(children);
			machine.acknowledgeChildMigration(children[0].doName);
			machine.acknowledgeChildMigration(children[1].doName);
			const completed = machine.splitStatus();

			machine.acknowledgeChildMigration(children[0].doName);
			expect(machine.splitStatus()).toEqual(completed);
		});
	});

	it("rejects acks before split_started and from unknown children (more-acks-than-children invariant)", async () => {
		await withSplitState((machine, pCtx) => {
			expect(() => machine.acknowledgeChildMigration("nope")).toThrow(/splitStatus must exist/);

			machine.queueSplit("hash", pCtx);
			expect(() => machine.acknowledgeChildMigration("nope")).toThrow(/cannot acknowledge child migration in status split_queued/);

			const children = makeChildren(pCtx, 2);
			machine.commitSplitStarted(children);
			// Unknown children inflate the ack list without ever matching every(child acked):
			// the third distinct ack must trip the more-acks-than-children invariant.
			machine.acknowledgeChildMigration("unknown-1");
			machine.acknowledgeChildMigration("unknown-2");
			expect(() => machine.acknowledgeChildMigration("unknown-3")).toThrow(/more acks/);
		});
	});
});
