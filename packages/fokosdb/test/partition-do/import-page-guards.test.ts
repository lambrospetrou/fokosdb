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
import type { PartitionDO } from "../../src/server/do-partition.js";
import type { FokosImportRecord } from "../../src/sharding/repartition-types.js";
import { FOKOS_KV_KEYS } from "../../src/sharding/sharding-store.js";
import { PartitionIdHelper, hashChildIndex } from "../../src/sharding/partition-id.js";
import type { MigrationStream } from "../controlled-partition-do.js";
import { kb } from "./helpers.js";
import { CONTROLLED_NS, makePartition, TestPartition } from "./partition-harness.js";

const IMPORT_KEY = FOKOS_KV_KEYS.IMPORT;
const HELD_KEY = "alice";

/**
 * Holds one target's pull of one stream on the source, after the page is built.
 *
 * The page carries the state the source held when the gate closed. `run` can therefore move the
 * target on and then release the page, which is the interleaving a second import loop produces.
 */
async function withPullHeld(
	source: TestPartition,
	hold: { target: TestPartition; stream: MigrationStream },
	run: (gate: { waitForHold: () => Promise<void>; release: () => Promise<void> }) => Promise<void>,
): Promise<void> {
	const controlled = source.controlled;
	await controlled.testHoldPulls({ stream: hold.stream, target: hold.target.doName, afterRead: true });
	try {
		await run({
			waitForHold: async () => {
				await vi.waitFor(async () => expect((await controlled.testPullStats()).heldTargets).toContain(hold.target.doName), {
					timeout: 5000,
					interval: 10,
				});
			},
			release: async () => await controlled.testReleasePulls(),
		});
	} finally {
		await controlled.testReleasePulls();
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
	return parent.hashChildren()[hashChildIndex(kb(hashKey), depth, parent.ctx.topology.hashSplitN)];
}

/** A parent holding one item under `HELD_KEY`, ready to split, plus the child that will own the key. */
async function partitionWithHeldKey(): Promise<{ parent: TestPartition; child: TestPartition }> {
	const parent = makePartition({ ns: CONTROLLED_NS, hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
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
			await release();
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
			await release();
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
				`INSERT INTO pending_transactions (hk, sk, transaction_id, transaction_ts, created_at, coordinator_json, operation)
				 VALUES (?, ?, 'tx-stale', 1, 1000, '{"v":1,"route":{"doName":"tc-1"},"idempotencyToken":"tok-1"}', 'put')`,
				kb(HELD_KEY),
				kb("sk"),
			);
		});
		await withPullHeld(parent, { target: child, stream: "pending_tx" }, async ({ waitForHold, release }) => {
			await parent.triggerHashSplit();
			await waitForHold();
			await moveImportOn(child, { state: "imported", cursor: null });
			await release();
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
