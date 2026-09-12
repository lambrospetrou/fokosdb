import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { PartitionDO, QueryItemsRpcRequest } from "../../src/server/do-partition.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import invariant from "../../src/shared/invariant.js";
import { MAX_ITEM_BYTES } from "../../src/shared/transaction-limits.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { EST_ROW_BYTES_K } from "../../src/shared/partition/item-size.js";
import { kb, makeStub } from "./helpers.js";
import { type TestPartition, makePartition, makeRangeRoot, makeTriggeredRangeRoot, withMigrationHeld } from "./partition-harness.js";

describe("PartitionDO — range split", () => {
	describe("queryItems leaf batching", () => {
		const request = (direction: "asc" | "desc", overrides: Partial<QueryItemsRpcRequest> = {}) => ({
			hashKey: kb("alice"),
			interval: {},
			direction,
			budgetBytes: 64 * 1024 * 1024,
			remainingLimit: null,
			maxPartitionVisits: 100,
			cursor: null,
			...overrides,
		});

		it("fetches at most 20 rows from SQLite and continues until the result page is full", async () => {
			const { ctx, stub } = makeStub();
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				const store = new PartitionStore(state.storage);
				for (let i = 0; i < 45; i++) {
					store.upsertItem({
						hk: kb("alice"),
						sk: kb(String(i).padStart(3, "0")),
						data: "x",
						kind: "text",
						ttlAt: null,
						lastTransactionTs: 0,
					});
				}
				const fetchSpy = vi.spyOn(PartitionStore.prototype, "queryRangeItemsPage");
				try {
					const result = await instance.apiQueryItems(ctx, request("asc"));
					expect(result.items.map((item) => KeyCodec.decode(item.sk))).toEqual(
						Array.from({ length: 45 }, (_, i) => String(i).padStart(3, "0")),
					);
					expect(fetchSpy.mock.calls.map(([opts]) => opts.limit)).toEqual([20, 20, 20]);
				} finally {
					fetchSpy.mockRestore();
				}
			});
		});

		it.each(["asc", "desc"] as const)("pages 400 KiB items without gaps or duplicates in %s order", async (direction) => {
			const { ctx, stub } = makeStub();
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				const store = new PartitionStore(state.storage);
				for (const sk of ["a", "b", "c"]) {
					const dataBytes = MAX_ITEM_BYTES - kb("alice").byteLength - kb(sk).byteLength - EST_ROW_BYTES_K;
					store.upsertItem({
						hk: kb("alice"),
						sk: kb(sk),
						data: new Uint8Array(dataBytes),
						kind: "bytes",
						ttlAt: null,
						lastTransactionTs: 0,
					});
				}

				const seen: string[] = [];
				let cursor: QueryItemsRpcRequest["cursor"] = null;
				for (;;) {
					const result = await instance.apiQueryItems(
						ctx,
						request(direction, { budgetBytes: MAX_ITEM_BYTES + 100, remainingLimit: 2, cursor }),
					);
					seen.push(...result.items.map((item) => KeyCodec.decode(item.sk) as string));
					if (result.nextCursor === null) break;
					cursor = result.nextCursor;
				}
				const expected = direction === "asc" ? ["a", "b", "c"] : ["c", "b", "a"];
				expect(seen).toEqual(expected);
				expect(new Set(seen).size).toBe(seen.length);
			});
		});
	});

	describe("queryItems across the split range tree", () => {
		// Build a promoted range root, populate it, and complete its split into N leaf children.
		const buildSplitTree = async (N: number) => {
			const { root, sks } = await makeTriggeredRangeRoot(N);
			expect(sks.length).toBeGreaterThanOrEqual(N);
			await root.awaitSplitCompleted();
			return { root, sks };
		};

		const queryPage = (root: TestPartition, overrides: Partial<QueryItemsRpcRequest> = {}) =>
			root.stub.apiQueryItems(root.ctx, {
				hashKey: kb("alice"),
				interval: {}, // whole hashKey
				direction: "asc",
				budgetBytes: 64 * 1024 * 1024,
				remainingLimit: null,
				maxPartitionVisits: 1000,
				cursor: null,
				...overrides,
			});

		// Page through the whole result set, accumulating decoded sort keys and the set of leaf DOs touched.
		const collect = async (root: TestPartition, overrides: Partial<QueryItemsRpcRequest> = {}) => {
			const out: Array<string | Uint8Array> = [];
			const leaves = new Set<string>();
			let cursor: QueryItemsRpcRequest["cursor"] = null;
			let pages = 0;
			for (;;) {
				const res = await queryPage(root, { ...overrides, cursor });
				pages++;
				for (const it of res.items) out.push(KeyCodec.decode(it.sk));
				for (const m of res.partitionMetas) leaves.add(m.servedByActorName);
				if (res.nextCursor === null) break;
				cursor = res.nextCursor;
				invariant(pages < 1000, "queryItems pagination did not terminate");
			}
			return { sks: out, leaves, pages };
		};

		it("returns every item across all N leaves in a single page (regression: must not stop at the leftmost leaf)", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			const res = await queryPage(root);
			expect(res.nextCursor).toBeNull();
			expect(res.items.map((it) => KeyCodec.decode(it.sk))).toEqual([...sks].sort());

			// The fan-out actually touched every leaf — before the fix it routed by the sentinel sort key
			// to the single leftmost leaf and silently dropped the rest.
			const leaves = new Set(res.partitionMetas.map((m) => m.servedByActorName));
			expect(leaves.size).toBe(N);
			// partitionMetas is leaf-only: N leaves, the router contributes no entry but is counted in forwardCount.
			expect(res.partitionMetas).toHaveLength(N);
			expect(res.meta.forwardCount).toBe(N);
		});

		it("paginates across leaves under a tight byte budget without dropping or duplicating items", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			const { sks: got, leaves, pages } = await collect(root, { budgetBytes: 130 * 1024 });
			expect(pages).toBeGreaterThan(1); // genuinely multi-page
			expect(leaves.size).toBe(N); // every leaf eventually visited
			expect(got).toEqual([...sks].sort()); // complete and ordered
			expect(new Set(got.map(String)).size).toBe(got.length); // no duplicates
		});

		it("walks leaves in descending order for scanIndexForward=false", async () => {
			const { root, sks } = await buildSplitTree(4);

			const { sks: got, leaves } = await collect(root, { direction: "desc", budgetBytes: 130 * 1024 });
			expect(leaves.size).toBe(4);
			expect(got).toEqual([...sks].sort().reverse());
		});

		it("honors remainingLimit across the walk (stops mid-fan-out with a resumable cursor)", async () => {
			const { root, sks } = await buildSplitTree(4);
			expect(sks.length).toBeGreaterThanOrEqual(6);

			const res = await queryPage(root, { remainingLimit: 5 });
			expect(res.items.map((it) => KeyCodec.decode(it.sk))).toEqual([...sks].sort().slice(0, 5));
			expect(res.nextCursor).not.toBeNull();
		});

		it("caps the fan-out per page (maxPartitionVisits) and resumes via a boundary cursor without gaps or duplicates", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			// One leaf per page forces the boundary continuation cursor on every page but the last; a
			// generous byte/limit budget ensures only the partition-visit cap drives pagination.
			const { sks: got, leaves, pages } = await collect(root, { maxPartitionVisits: 1 });
			expect(pages).toBeGreaterThanOrEqual(N); // one leaf per page → at least N pages
			expect(leaves.size).toBe(N);
			expect(got).toEqual([...sks].sort());
			expect(new Set(got.map(String)).size).toBe(got.length); // no duplicates (boundary key not dropped or repeated)
		});

		it("caps the fan-out per page for descending scans too", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			const { sks: got, leaves, pages } = await collect(root, { direction: "desc", maxPartitionVisits: 1 });
			expect(pages).toBeGreaterThanOrEqual(N);
			expect(leaves.size).toBe(N);
			expect(got).toEqual([...sks].sort().reverse());
			expect(new Set(got.map(String)).size).toBe(got.length);
		});

		it("queryItemsDirect reads from the router's own local rows, never fanning out to children (regression: infinite loop when children are migrating)", async () => {
			// Scenario: a migrating range child calls parent.queryItemsDirect(). Before the fix,
			// queryItemsDirect on a range router called queryItemsAsRangeNode → walkRangeChildren →
			// child.queryItems() → child detects it's still migrating → parent.queryItemsDirect() → …
			// (infinite loop until the subrequest depth limit is hit).
			//
			// queryItemsDirect always calls queryItemsLocal and bypasses the child routing. forwardCount=0
			// asserts that: a walk of the children would report one forward per child, migrated or not.
			const N = 2;
			const { root, sks } = await makeRangeRoot(N);

			// Start the real split with child transaction-metadata responses held at the parent.
			// Check the migration state instead of assuming that child alarms have not run.
			await withMigrationHeld(root, async (waitForAllChildRequests) => {
				const start = sks.length;
				sks.push(...(await root.triggerRangeSplit((i) => `sk${String(i + start).padStart(3, "0")}-${crypto.randomUUID()}`)));
				await root.awaitSplitStarted();
				await waitForAllChildRequests();
				for (const child of await root.children()) expect((await child.status()).migrationStatus).toBe("migration_migrating");

				const result = await root.stub.internalQueryItemsDirect({
					hashKey: kb("alice"),
					interval: {},
					direction: "asc",
					budgetBytes: 64 * 1024 * 1024,
					remainingLimit: null,
					maxPartitionVisits: 1000,
					cursor: null,
				});

				// The router's own DB still holds all items (parent rows are never deleted during split).
				expect(result.items.map((it) => KeyCodec.decode(it.sk))).toEqual([...sks].sort());
				// Local read only: no forwarding to children.
				expect(result.meta.forwardCount).toBe(0);
			});

			// Drain pending child migrations so their background work doesn't outlive the test.
			await root.awaitSplitCompleted();
		});

		// A cursor promises more rows, so neither budget exit emits one after the walk covers every child
		// that can contribute. A cursor there costs the client a round trip that returns nothing.
		// `db.ts:queryItems` follows the same rule: it emits a cursor only when a later sub-query remains.
		it("emits no cursor when the byte budget lands on zero at the last leaf", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			// The exact bytes the whole scan consumes. Replayed as the budget, every leaf still drains
			// itself (each reports no cursor of its own) and the router's remaining bytes reach zero as the
			// LAST leaf finishes — the one case where "budget exhausted" does not mean "more rows exist".
			const full = await queryPage(root);
			expect(full.nextCursor).toBeNull();
			expect(full.bytesConsumed).toBeGreaterThan(0);

			const res = await queryPage(root, { budgetBytes: full.bytesConsumed });
			expect(res.items.map((it) => KeyCodec.decode(it.sk))).toEqual([...sks].sort());
			expect(res.nextCursor).toBeNull();
		});

		it("emits no cursor when the partition-visit cap is reached but every remaining child is outside the interval", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);
			const children = (await root.splitStatus()).childPartitionContexts;
			// Children are in ascending boundary order, so an exclusive upper bound at the third child's
			// start boundary leaves exactly the first two intersecting the query.
			const upper = children[2].rangePartition!.startBoundary!;

			// The visit cap is spent by those two leaves. The two children beyond the bound are skipped by
			// the interval, so there is nothing left to resume into — the old code counted them anyway and
			// emitted a boundary cursor.
			const res = await queryPage(root, {
				interval: { upper: { value: upper, inclusive: false } },
				maxPartitionVisits: 2,
			});
			expect(res.partitionMetas).toHaveLength(2);
			const expected = [...sks].sort().filter((sk) => KeyCodec.compare(KeyCodec.encode(sk), upper) < 0);
			expect(expected.length).toBeGreaterThan(0);
			expect(res.items.map((it) => KeyCodec.decode(it.sk))).toEqual(expected);
			expect(res.nextCursor).toBeNull();
		});
	});
});
