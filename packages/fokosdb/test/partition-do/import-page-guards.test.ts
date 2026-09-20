/**
 * A migration page that outlives the durable state it was built against.
 *
 * A target asks for a page against one snapshot of its import record and applies the page later. The
 * two moments can be far apart, because the alarm and the background timer both drive an import, and
 * a revived instance can hold a page from a previous life. The ingest inserts absent rows only, so a
 * row the user deleted after the import finished looks absent and comes back. Every page therefore
 * re-reads the record inside its own commit transaction, and the target drops the whole page when
 * the record has moved on.
 *
 * These cases need two interleaved import loops, so they drive a real Durable Object. The page sits
 * on the wire while the test moves the durable state of the target under it.
 */
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import type { FokosImportRecord, FokosMigrationPullRequest } from "../../src/sharding/repartition-types.js";
import { REPARTITION_KV_KEYS } from "../../src/sharding/repartition-flow.js";
import { PartitionIdHelper, hashChildIndex } from "../../src/sharding/partition-id.js";
import { kb } from "./helpers.js";
import { makePartition, TestPartition } from "./partition-harness.js";

const IMPORT_KEY = REPARTITION_KV_KEYS.IMPORT;
const HELD_KEY = "alice";

/** The stream a pull is asking for, read back out of the flow's opaque cursor. */
function streamOf(req: FokosMigrationPullRequest): "overrides" | "items" | "pending_tx" {
	if (req.cursor === null || req.cursor.phase === "overrides") return "overrides";
	return (req.cursor.inner as { stream?: string } | null)?.stream === "pending_tx" ? "pending_tx" : "items";
}

/**
 * Holds one target's pull of one stream on the source, after the page is built.
 *
 * The page carries the state the source held when the gate closed. `run` can therefore move the
 * target on and then release the page, which is the interleaving a second import loop produces.
 */
async function withPullHeld(
	source: TestPartition,
	hold: { target: TestPartition; stream: "overrides" | "items" | "pending_tx" },
	run: (gate: { waitForHold: () => Promise<void>; release: () => void }) => Promise<void>,
): Promise<void> {
	let release!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	let held = 0;
	const restore = await runInDurableObject(source.stub, (instance: PartitionDO) => {
		// The spy MUST go on the prototype. The RPC dispatcher rejects a method installed as an own
		// property on the DO instance. The guard keeps the mock on the shared prototype scoped to this
		// one instance, so a background pull from another partition passes through.
		const prototype: PartitionDO = Object.getPrototypeOf(instance);
		const original = prototype.fokosMigrationPull;
		const spy = vi.spyOn(prototype, "fokosMigrationPull").mockImplementation(async function (this: PartitionDO, req) {
			if (this !== instance || req.target.doName !== hold.target.doName || streamOf(req) !== hold.stream) {
				return await original.call(this, req);
			}
			const page = await original.call(this, req);
			held++;
			await gate;
			return page;
		});
		return () => spy.mockRestore();
	});
	try {
		await run({
			waitForHold: async () => {
				await vi.waitFor(() => expect(held).toBeGreaterThan(0), { timeout: 5000, interval: 10 });
			},
			release,
		});
	} finally {
		release();
		restore();
	}
}

/** Rewrites the import record of the held target, as a second loop of the same import leaves it. */
async function moveImportOn(target: TestPartition, change: Partial<FokosImportRecord>): Promise<void> {
	await runInDurableObject(target.stub, (_i: PartitionDO, state: DurableObjectState) => {
		const record = state.storage.kv.get<FokosImportRecord>(IMPORT_KEY);
		expect(record, `${target.doName}: no import record to move on`).toBeDefined();
		state.storage.kv.put<FokosImportRecord>(IMPORT_KEY, { ...record!, ...change });
	});
}

/**
 * The hash child that will own `hashKey`, resolved before the split starts.
 *
 * The hold must go in before the crossing write, so the test cannot wait for the split status to
 * name the children. The child index is deterministic, so the test does not need that status.
 */
function ownerOf(parent: TestPartition, hashKey: string): TestPartition {
	const depth = PartitionIdHelper.depth(Uint8Array.fromHex(parent.ctx.partitionId));
	return parent.hashChildren()[hashChildIndex(kb(hashKey), depth, parent.ctx.hashSplitN)];
}

/** A parent holding one item under `HELD_KEY`, ready to split, plus the child that will own the key. */
async function partitionWithHeldKey(): Promise<{ parent: TestPartition; child: TestPartition }> {
	const parent = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
	await parent.put({ hashKey: kb(HELD_KEY), sortKey: kb("sk"), data: "owned-by-a-child", kind: "text" });
	return { parent, child: ownerOf(parent, HELD_KEY) };
}

describe("PartitionDO — a migration page that outlives its import record", () => {
	it("drops an items page whose import completed while the page was in flight", { timeout: 30_000 }, async ({ expect }) => {
		const { parent, child } = await partitionWithHeldKey();

		await withPullHeld(parent, { target: child, stream: "items" }, async ({ waitForHold, release }) => {
			await parent.triggerHashSplit();
			await waitForHold();
			// Another loop finished the whole import. This page would now re-insert rows for which the
			// active child can already have served a delete.
			await moveImportOn(child, { state: "imported", cursor: null });
			release();
		});

		await parent.awaitSplitCompleted();
		expect(await child.localItemCount(HELD_KEY)).toBe(0);
		expect((await child.status()).migrationStatus).toBe("migration_completed");
	});

	it("drops an items page built from a cursor that has already advanced", { timeout: 30_000 }, async ({ expect }) => {
		const { parent, child } = await partitionWithHeldKey();

		await withPullHeld(parent, { target: child, stream: "items" }, async ({ waitForHold, release }) => {
			await parent.triggerHashSplit();
			await waitForHold();
			// The other loop committed past the whole items stream while this page was on the wire.
			await moveImportOn(child, { cursor: { phase: "host", inner: { stream: "pending_tx", cursor: null } } });
			release();
		});

		// The import finishes from the cursor the other loop left, so the dropped page adds nothing.
		await parent.awaitSplitCompleted();
		expect(await child.localItemCount(HELD_KEY)).toBe(0);
	});

	it("drops a pending-lock page whose import completed while the page was in flight", { timeout: 30_000 }, async ({ expect }) => {
		const { parent, child } = await partitionWithHeldKey();
		// A lock whose transaction resolves before the page lands would leak on the child, where nothing
		// holds the coordinator that can release it.
		await runInDurableObject(parent.stub, (_i: PartitionDO, state: DurableObjectState) => {
			state.storage.sql.exec(
				`INSERT INTO pending_transactions (hk, sk, transaction_id, transaction_ts, created_at, coordinator_do_id, operation)
				 VALUES (?, ?, 'tx-stale', 1, 1000, 'tc-1', 'put')`,
				kb(HELD_KEY),
				kb("sk"),
			);
		});
		await withPullHeld(parent, { target: child, stream: "pending_tx" }, async ({ waitForHold, release }) => {
			await parent.triggerHashSplit();
			await waitForHold();
			await moveImportOn(child, { state: "imported", cursor: null });
			release();
		});

		await parent.awaitSplitCompleted();
		const locks = await runInDurableObject(
			child.stub,
			(_i: PartitionDO, state: DurableObjectState) =>
				state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM pending_transactions`).toArray()[0].n,
		);
		expect(locks).toBe(0);
	});
});
