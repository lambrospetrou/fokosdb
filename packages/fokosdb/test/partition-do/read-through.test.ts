/**
 * `fokosExecuteLocal`: the read a repartition target sends its source while the target is still
 * importing. The source reads local rows only — no forwarding, no lifecycle gate — so the caller
 * check and the slice it resolves are the only things standing between a target and data it does
 * not own.
 */
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import type { GetItemRpcResponse, QueryItemsRpcRequest, QueryItemsRpcResponse } from "../../src/server/do-partition.js";
import { MAX_EVALUATED_BYTES_PER_PAGE, MAX_EVALUATED_ITEMS_PER_PAGE } from "../../src/shared/query/page-budget.js";
import type { StoredItem } from "../../src/shared/partition/partition-store.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { kb } from "./helpers.js";
import { drainUntil, makePartition, makeTriggeredRangeRoot, withMigrationHeld } from "./partition-harness.js";

const queryRequest = (hashKey: string, overrides: Partial<QueryItemsRpcRequest> = {}): QueryItemsRpcRequest => ({
	hashKey: kb(hashKey),
	interval: {},
	direction: "asc",
	remainingEvaluatedItems: MAX_EVALUATED_ITEMS_PER_PAGE,
	remainingEvaluatedBytes: MAX_EVALUATED_BYTES_PER_PAGE,
	remainingResponseBytes: 64 * 1024 * 1024,
	remainingPartitionVisits: 100,
	allowOversizedFirstItem: true,
	cursor: null,
	select: "projection" as const,
	plan: null,
	...overrides,
});

describe("PartitionDO — fokosExecuteLocal", () => {
	it("rejects a caller that is not a target of any repartition it owns", async () => {
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: 1 } });
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" as const });
		const children = await partition.splitHash();

		// A name alone is a value the caller chose, so a real child's name with someone else's identity
		// must not pass either.
		const impostors = [
			{ partitionId: children[0].ctx.partitionId, doName: "not-a-child" },
			{ partitionId: "00ffff00", doName: children[0].doName },
		];
		// runInDurableObject keeps the caught rejection inside the DO's execution context, so it does not
		// leak as an unhandled rejection at the worker level.
		await runInDurableObject(partition.stub, async (instance: PartitionDO) => {
			for (const caller of impostors) {
				await expect(
					instance.fokosExecuteLocal({ op: "getItem", caller, request: { hashKey: kb("hk"), sortKey: kb("sk") } }),
				).rejects.toThrow(fokosErrorWith("repartition_target_unknown", { caller: caller.doName }));
			}
		});
	});

	it("serves a hash child only the keys that hash to it", async () => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await partition.put({ hashKey: kb("alpha"), sortKey: kb("s1"), data: "alpha-value", kind: "text" as const });
		await partition.splitHash();

		const owner = await partition.childOwning("alpha");
		const sibling = (await partition.children()).find((c) => c.doName !== owner.doName);
		expect(sibling, "a two-way split should have a sibling").toBeDefined();

		const mine = (await partition.stub.fokosExecuteLocal({
			op: "getItem",
			caller: { partitionId: owner.ctx.partitionId, doName: owner.doName },
			request: { hashKey: kb("alpha"), sortKey: kb("s1") },
		})) as GetItemRpcResponse;
		expect(mine).toMatchObject({ found: true, item: { data: "alpha-value" } });

		// The sibling asking for the same key is a routing defect, not an empty answer: an empty answer
		// would let it cache "absent" for a key another child owns.
		await runInDurableObject(partition.stub, async (instance: PartitionDO) => {
			await expect(
				instance.fokosExecuteLocal({
					op: "getItem",
					caller: { partitionId: sibling!.ctx.partitionId, doName: sibling!.doName },
					request: { hashKey: kb("alpha"), sortKey: kb("s1") },
				}),
			).rejects.toThrow(fokosErrorWith("partition_misrouted"));
		});
	});

	it("follows a completed promotion to the range tree instead of reading its own stale rows", async () => {
		// The defect: a hash child importing from its parent read a promoted key out of the parent's
		// local rows. Promotion GC makes those rows stale and then deletes them, so the child served a
		// stale value and later an empty answer, for a key whose data lives in the range tree.
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: 1 } });
		await partition.triggerPromotion("alice", (i) => `sk${i + 1}`);
		const rangeRoot = await partition.awaitPromoted("alice");
		await drainUntil(
			[partition, rangeRoot],
			async () => (await partition.localItemCount("alice")) === 0,
			"alice to be garbage-collected from the hash DO",
			8000,
		);

		// Split with the child transaction-metadata responses held: the children have pulled their items
		// but not yet the promoted-key forward pointers, which is exactly the window the defect lives in.
		await withMigrationHeld(partition, async (waitForAllChildRequests) => {
			await partition.triggerHashSplit();
			await partition.awaitSplitStarted();
			await waitForAllChildRequests();

			const child = await partition.childOwning("alice");
			expect(await child.promotedKeyStatus("alice"), "the child must not have inherited the entry yet").toBeUndefined();

			const point = await child.get({ hashKey: kb("alice"), sortKey: kb("sk1") });
			expect(point.found, "a promoted key must stay readable through an importing hash child").toBe(true);
			expect(point.meta.servedByActorName, "the range tree owns the key, not the hash parent").toBe(rangeRoot.doName);

			const page = (await partition.stub.fokosExecuteLocal({
				op: "queryItems",
				caller: { partitionId: child.ctx.partitionId, doName: child.doName },
				request: queryRequest("alice"),
			})) as QueryItemsRpcResponse;
			expect(page.items.length, "a query of a promoted key must reach the range tree too").toBeGreaterThan(0);
			expect(page.meta.servedByActorName).toBe(rangeRoot.doName);
		});
	});

	it("clips a range child's query to the slice it owns", async () => {
		const { root, sks } = await makeTriggeredRangeRoot(2);
		await root.awaitSplitCompleted();
		const children = await root.children();

		// The split source keeps its item rows, so the router can still answer for every one of them —
		// which is exactly why it has to narrow the answer to the caller's slice.
		const caller = children[0];
		const end = caller.ctx.rangePartition!.endBoundary;
		expect(end, "the leftmost child must have a bounded upper edge").not.toBeNull();

		const page = (await root.stub.fokosExecuteLocal({
			op: "queryItems",
			caller: { partitionId: caller.ctx.partitionId, doName: caller.doName },
			// The caller asks for the whole key; the source narrows it to what this child owns.
			request: queryRequest("alice"),
		})) as QueryItemsRpcResponse;

		const owned = [...sks].sort().filter((sk) => KeyCodec.compare(kb(sk), end!) < 0);
		expect(owned.length, "the leftmost child should own part of the seeded range").toBeGreaterThan(0);
		expect(owned.length, "the leftmost child should NOT own the whole range").toBeLessThan(sks.length);
		expect(page.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual(owned);
		expect(page.meta.forwardCount, "a read-through answer is local; it never fans out").toBe(0);

		// A cursor that sits in a sibling's slice is a routing defect, not an empty page.
		await runInDurableObject(root.stub, async (instance: PartitionDO) => {
			await expect(
				instance.fokosExecuteLocal({
					op: "queryItems",
					caller: { partitionId: caller.ctx.partitionId, doName: caller.doName },
					request: queryRequest("alice", { cursor: { hk: kb("alice"), sk: end! } }),
				}),
			).rejects.toThrow(fokosErrorWith("partition_misrouted"));
		});
	});
});
