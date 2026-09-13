import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO, QueryItemsRpcRequest, QueryItemsRpcResponse } from "../../src/server/do-partition.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import invariant from "../../src/shared/invariant.js";
import { MAX_ITEM_BYTES } from "../../src/shared/transaction-limits.js";
import { MAX_EVALUATED_BYTES_PER_PAGE, MAX_EVALUATED_ITEMS_PER_PAGE } from "../../src/shared/query/page-budget.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { EST_ROW_BYTES_K } from "../../src/shared/partition/item-size.js";
import { kb, makeStub } from "./helpers.js";
import {
	type TestPartition,
	makePartition,
	makeRangeRoot,
	makeTriggeredRangeRoot,
	PROMOTION_TEST_MAX_SIZE_MB,
	withMigrationHeld,
} from "./partition-harness.js";

describe("PartitionDO — range split", () => {
	// One request with every budget wide open; tests override the budget they exercise.
	const fullRequest = (overrides: Partial<QueryItemsRpcRequest> = {}): QueryItemsRpcRequest => ({
		hashKey: kb("alice"),
		interval: {},
		direction: "asc",
		remainingEvaluatedItems: MAX_EVALUATED_ITEMS_PER_PAGE,
		remainingEvaluatedBytes: MAX_EVALUATED_BYTES_PER_PAGE,
		remainingResponseBytes: 64 * 1024 * 1024,
		remainingPartitionVisits: 100,
		allowOversizedFirstItem: true,
		cursor: null,
		select: "projection" as const,
		...overrides,
	});

	describe("queryItems leaf pages", () => {
		const request = (direction: "asc" | "desc", overrides: Partial<QueryItemsRpcRequest> = {}) => fullRequest({ direction, ...overrides });

		const seed45 = (state: DurableObjectState) => {
			const store = new PartitionStore(state.storage);
			for (let i = 0; i < 45; i++) {
				store.upsertItem({
					hk: kb("alice"),
					sk: kb(String(i).padStart(3, "0")),
					data: "x",
					kind: "text",
					ttlAt: null,
					txOrderTs: 0,
				});
			}
		};

		it("reads one candidate beyond a full evaluated-item budget and returns an inclusive cursor", async () => {
			const { ctx, stub } = makeStub();
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				seed45(state);

				const result = await instance.apiQueryItems(ctx, request("asc", { remainingEvaluatedItems: 10 }));

				expect(result.items).toHaveLength(10);
				expect(result.count).toBe(10);
				expect(result.scannedCount).toBe(10);
				expect(result.rowsReturned).toBe(11);
				expect(result.nextCursor?.inclusive).toBe(true);
				expect(KeyCodec.decode(result.nextCursor!.sk)).toBe("010");
			});
		});

		it("returns no cursor when the evaluated-item budget ends on the last candidate", async () => {
			const { ctx, stub } = makeStub();
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				seed45(state);

				const result = await instance.apiQueryItems(ctx, request("asc", { remainingEvaluatedItems: 45 }));

				expect(result.items).toHaveLength(45);
				expect(result.count).toBe(45);
				expect(result.rowsReturned).toBe(45);
				expect(result.nextCursor).toBeNull();
			});
		});

		it("count mode returns no items, zero response bytes, and the same page counters", async () => {
			const { ctx, stub } = makeStub();
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				seed45(state);

				const result = await instance.apiQueryItems(ctx, request("asc", { select: "count", remainingEvaluatedItems: 10 }));

				expect(result.items).toEqual([]);
				expect(result.responseBytes).toBe(0);
				expect(result.count).toBe(10);
				expect(result.scannedCount).toBe(10);
				expect(result.rowsReturned).toBe(11);
				expect(result.nextCursor?.inclusive).toBe(true);
				expect(KeyCodec.decode(result.nextCursor!.sk)).toBe("010");
				expect(result.meta.rowsRead).toBeGreaterThan(0);
			});
		});

		it("count and projection pages can stop at different positions and exchange cursors", async () => {
			const { ctx, stub } = makeStub();
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				const store = new PartitionStore(state.storage);
				for (let i = 0; i < 6; i++) {
					store.upsertItem({
						hk: kb("alice"),
						sk: kb(`big${i}`),
						data: new Uint8Array(100 * 1024),
						kind: "bytes",
						ttlAt: null,
						txOrderTs: 0,
					});
				}

				// Projection stops when the response budget rejects the third item.
				const proj = await instance.apiQueryItems(ctx, request("asc", { remainingResponseBytes: 250 * 1024 }));
				expect(proj.items).toHaveLength(2);
				expect(proj.nextCursor).not.toBeNull();

				// Count ignores the response budget and drains the same interval.
				const cnt = await instance.apiQueryItems(ctx, request("asc", { select: "count", remainingResponseBytes: 250 * 1024 }));
				expect(cnt.items).toEqual([]);
				expect(cnt.count).toBe(6);
				expect(cnt.nextCursor).toBeNull();

				// The projection cursor resumes under count at the rejected candidate.
				const cntResume = await instance.apiQueryItems(ctx, request("asc", { select: "count", cursor: proj.nextCursor }));
				expect(cntResume.count).toBe(4);
				expect(cntResume.nextCursor).toBeNull();

				// The count cursor resumes under projection and materializes the rest.
				const cnt3 = await instance.apiQueryItems(ctx, request("asc", { select: "count", remainingEvaluatedItems: 3 }));
				expect(cnt3.count).toBe(3);
				expect(cnt3.nextCursor).not.toBeNull();
				const projResume = await instance.apiQueryItems(ctx, request("asc", { cursor: cnt3.nextCursor }));
				expect(projResume.items).toHaveLength(3);
				expect(projResume.items.map((it) => KeyCodec.decode(it.sk))).toEqual(["big3", "big4", "big5"]);
				expect(projResume.nextCursor).toBeNull();
			});
		});

		it("physical rowsRead is reported from SQLite and is not derived from the logical counters", async () => {
			const { ctx, stub } = makeStub();
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				seed45(state);

				const result = await instance.apiQueryItems(ctx, request("asc", { remainingEvaluatedItems: 10 }));

				expect(result.meta.rowsRead).toBeGreaterThan(0);
				expect(result.partitionMetas[0].rowsRead).toBe(result.meta.rowsRead);
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
						txOrderTs: 0,
					});
				}

				const seen: string[] = [];
				let cursor: QueryItemsRpcRequest["cursor"] = null;
				for (;;) {
					const result = await instance.apiQueryItems(
						ctx,
						request(direction, { remainingResponseBytes: MAX_ITEM_BYTES + 100, remainingEvaluatedItems: 2, cursor }),
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

		// Children in ascending boundary order; the leftmost child has a null start boundary.
		const byBoundary = (children: TestPartition[]) =>
			[...children].sort((a, b) =>
				KeyCodec.compare(
					a.ctx.rangePartition!.startBoundary ?? KeyCodec.encodeOptional(undefined),
					b.ctx.rangePartition!.startBoundary ?? KeyCodec.encodeOptional(undefined),
				),
			);

		const queryPage = (root: TestPartition, overrides: Partial<QueryItemsRpcRequest> = {}) =>
			root.stub.apiQueryItems(root.ctx, fullRequest(overrides));

		// Page through the whole result set, accumulating decoded sort keys, the summed page counters,
		// and the set of leaf DOs touched. `onPage` observes each raw page (count pages carry no items).
		const collect = async (
			root: TestPartition,
			overrides: Partial<QueryItemsRpcRequest> = {},
			onPage?: (res: QueryItemsRpcResponse) => void,
		) => {
			const out: Array<string | Uint8Array> = [];
			const leaves = new Set<string>();
			let count = 0;
			let scannedCount = 0;
			let rowsReturned = 0;
			let cursor: QueryItemsRpcRequest["cursor"] = null;
			let pages = 0;
			for (;;) {
				const res = await queryPage(root, { ...overrides, cursor });
				onPage?.(res);
				pages++;
				count += res.count;
				scannedCount += res.scannedCount;
				rowsReturned += res.rowsReturned;
				for (const it of res.items) out.push(KeyCodec.decode(it.sk));
				for (const m of res.partitionMetas) leaves.add(m.servedByActorName);
				if (res.nextCursor === null) break;
				cursor = res.nextCursor;
				invariant(pages < 1000, "queryItems pagination did not terminate");
			}
			return { sks: out, leaves, pages, count, scannedCount, rowsReturned };
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

			const { sks: got, leaves, pages } = await collect(root, { remainingResponseBytes: 130 * 1024 });
			expect(pages).toBeGreaterThan(1); // genuinely multi-page
			expect(leaves.size).toBe(N); // every leaf eventually visited
			expect(got).toEqual([...sks].sort()); // complete and ordered
			expect(new Set(got.map(String)).size).toBe(got.length); // no duplicates
		});

		it("walks leaves in descending order for scanIndexForward=false", async () => {
			const { root, sks } = await buildSplitTree(4);

			const { sks: got, leaves } = await collect(root, { direction: "desc", remainingResponseBytes: 130 * 1024 });
			expect(leaves.size).toBe(4);
			expect(got).toEqual([...sks].sort().reverse());
		});

		it("honors remainingEvaluatedItems across the walk (stops mid-fan-out with a resumable cursor)", async () => {
			const { root, sks } = await buildSplitTree(4);
			expect(sks.length).toBeGreaterThanOrEqual(6);

			const res = await queryPage(root, { remainingEvaluatedItems: 5 });
			expect(res.items.map((it) => KeyCodec.decode(it.sk))).toEqual([...sks].sort().slice(0, 5));
			expect(res.nextCursor).not.toBeNull();
		});

		it("caps the fan-out per page (remainingPartitionVisits) and resumes via a boundary cursor without gaps or duplicates", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			// One leaf per page forces the boundary continuation cursor on every page but the last; a
			// generous byte/limit budget ensures only the partition-visit cap drives pagination.
			const { sks: got, leaves, pages } = await collect(root, { remainingPartitionVisits: 1 });
			expect(pages).toBeGreaterThanOrEqual(N); // one leaf per page → at least N pages
			expect(leaves.size).toBe(N);
			expect(got).toEqual([...sks].sort());
			expect(new Set(got.map(String)).size).toBe(got.length); // no duplicates (boundary key not dropped or repeated)
		});

		it("caps the fan-out per page for descending scans too", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			const { sks: got, leaves, pages } = await collect(root, { direction: "desc", remainingPartitionVisits: 1 });
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

				const result = await root.stub.internalQueryItemsDirect(fullRequest());

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
			expect(full.responseBytes).toBeGreaterThan(0);

			const res = await queryPage(root, { remainingResponseBytes: full.responseBytes });
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
				remainingPartitionVisits: 2,
			});
			expect(res.partitionMetas).toHaveLength(2);
			const expected = [...sks].sort().filter((sk) => KeyCodec.compare(KeyCodec.encode(sk), upper) < 0);
			expect(expected.length).toBeGreaterThan(0);
			expect(res.items.map((it) => KeyCodec.decode(it.sk))).toEqual(expected);
			expect(res.nextCursor).toBeNull();
		});

		it.each(["asc", "desc"] as const)("count mode walks every leaf in %s order and returns no items", async (direction) => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			const { count, scannedCount, leaves } = await collect(root, { select: "count", direction }, (res) => {
				expect(res.items).toHaveLength(0);
				expect(res.responseBytes).toBe(0);
			});
			expect(count).toBe(sks.length);
			expect(scannedCount).toBe(count);
			expect(leaves.size).toBe(N);
		});

		it("the evaluated-byte budget paginates across leaves without gaps or duplicates", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			const full = await queryPage(root);
			expect(full.evaluatedBytes).toBeGreaterThan(0);

			const { sks: got, pages } = await collect(root, { remainingEvaluatedBytes: Math.ceil(full.evaluatedBytes / 3) });
			expect(pages).toBeGreaterThan(1);
			expect(got).toEqual([...sks].sort());
			expect(new Set(got.map(String)).size).toBe(got.length);
		});

		it("the evaluated-item budget that lands on zero at the last leaf emits no cursor", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			const res = await queryPage(root, { remainingEvaluatedItems: sks.length });
			expect(res.items.map((it) => KeyCodec.decode(it.sk))).toEqual([...sks].sort());
			expect(res.nextCursor).toBeNull();

			const cnt = await queryPage(root, { select: "count", remainingEvaluatedItems: sks.length });
			expect(cnt.count).toBe(sks.length);
			expect(cnt.nextCursor).toBeNull();
		});

		it("the first-item exception applies once per page, not once per leaf", async () => {
			const N = 4;
			const { root } = await buildSplitTree(N);
			const children = byBoundary(await root.children());
			const c0 = children[0];
			const c1 = children[1];

			// The leaf scans its own rows regardless of clipping, so the whole-range interval is fine.
			const leaf0 = await c0.stub.apiQueryItems(c0.ctx, fullRequest());
			const leaf1 = await c1.stub.apiQueryItems(c1.ctx, fullRequest());
			expect(leaf0.items.length).toBeGreaterThan(0);
			expect(leaf1.items.length).toBeGreaterThan(0);

			// One byte past leaf 0's response leaves no room for leaf 1's first item, and the first-item
			// exception was already spent by leaf 0 — the second leaf must be visited and reject its row.
			const res = await queryPage(root, { remainingResponseBytes: leaf0.responseBytes + 1 });
			expect(res.items.map((it) => KeyCodec.decode(it.sk))).toEqual(leaf0.items.map((it) => KeyCodec.decode(it.sk)));
			expect(res.partitionMetas).toHaveLength(2);
			expect(res.rowsReturned).toBe(leaf0.rowsReturned + 1);
			expect(res.nextCursor?.inclusive).toBe(true);
			expect(KeyCodec.compare(res.nextCursor!.sk, leaf1.items[0].sk)).toBe(0);

			// The same oversized first item is admitted when it starts the page.
			const res2 = await queryPage(root, {
				remainingResponseBytes: 1,
				interval: { lower: { value: c1.ctx.rangePartition!.startBoundary!, inclusive: true } },
			});
			expect(res2.items).toHaveLength(1);
		});

		it("a descending page that stops on a child start boundary evaluates the boundary item on the next page", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);
			const children = byBoundary(await root.children());
			const B = children[2].ctx.rangePartition!.startBoundary!;
			// Boundaries are separators between keys, not keys: an item exactly on the boundary is new.
			await root.put({ hashKey: kb("alice"), sortKey: B, data: "x", kind: "text" });
			sks.push(KeyCodec.decode(B) as string);

			// Exactly the items above the boundary fill the page; the boundary item is the extra
			// candidate the leaf reads and rejects, so the cursor resumes inclusively at it.
			const K = sks.filter((sk) => KeyCodec.compare(kb(sk), B) > 0).length;
			const p1 = await queryPage(root, { direction: "desc", remainingEvaluatedItems: K });
			expect(p1.items).toHaveLength(K);
			expect(p1.nextCursor?.inclusive).toBe(true);
			expect(KeyCodec.compare(p1.nextCursor!.sk, B)).toBe(0);

			const p2 = await queryPage(root, { direction: "desc", cursor: p1.nextCursor });
			expect(KeyCodec.compare(p2.items[0].sk, B)).toBe(0);

			const { sks: got } = await collect(root, { direction: "desc", remainingEvaluatedItems: K });
			expect(got).toEqual([...sks].sort().reverse());
			expect(new Set(got.map(String)).size).toBe(got.length);
		});

		it("a count page that spends the visit budget on empty leaves returns count 0 with a cursor", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);
			const children = byBoundary(await root.children());
			const owns = (child: TestPartition, sk: string) => {
				const start = child.ctx.rangePartition!.startBoundary ?? KeyCodec.encodeOptional(undefined);
				const end = child.ctx.rangePartition!.endBoundary;
				return KeyCodec.compare(kb(sk), start) >= 0 && (end === null || KeyCodec.compare(kb(sk), end) < 0);
			};

			let remaining = 0;
			for (const sk of sks) {
				if (owns(children[0], sk) || owns(children[1], sk)) {
					await root.stub.apiDeleteItem(root.ctx, { hashKey: kb("alice"), sortKey: kb(sk) });
				} else {
					remaining++;
				}
			}

			const p = await queryPage(root, { select: "count", remainingPartitionVisits: 1 });
			expect(p.count).toBe(0);
			expect(p.items).toHaveLength(0);
			expect(p.partitionMetas).toHaveLength(1);
			expect(p.nextCursor).not.toBeNull();

			const { count } = await collect(root, { select: "count", remainingPartitionVisits: 1 });
			expect(count).toBe(remaining);
		});

		it("a nested range router keeps the last non-null child lastEvaluatedCursor when the last child drains empty", async () => {
			const { root } = await makeTriggeredRangeRoot(2);
			await root.awaitSplitCompleted();
			const children = await root.children();
			const left = children.find((c) => c.ctx.rangePartition!.startBoundary === null)!;
			const grandchildren = byBoundary(await left.splitRange("aa"));
			const g2Start = grandchildren[1].ctx.rangePartition!.startBoundary!;

			// Delete every item the right grandchild owns; it must then drain empty on the next page.
			const under = await left.stub.apiQueryItems(left.ctx, fullRequest());
			const leftSks = under.items.map((it) => it.sk);
			for (const sk of leftSks) {
				if (KeyCodec.compare(sk, g2Start) >= 0) {
					await root.stub.apiDeleteItem(root.ctx, { hashKey: kb("alice"), sortKey: sk });
				}
			}
			const g1Sks = leftSks.filter((sk) => KeyCodec.compare(sk, g2Start) < 0);
			expect(g1Sks.length).toBeGreaterThan(0);

			const r = await left.stub.apiQueryItems(left.ctx, fullRequest());
			expect(r.nextCursor).toBeNull();
			expect(r.lastEvaluatedCursor).not.toBeNull();
			expect(KeyCodec.compare(r.lastEvaluatedCursor!.sk, g1Sks[g1Sks.length - 1])).toBe(0);
			expect(r.partitionMetas).toHaveLength(2);
			expect(r.items.length).toBe(g1Sks.length);
			expect(r.count).toBe(g1Sks.length);
			expect(r.scannedCount).toBe(g1Sks.length);
		}, 30_000);

		it("a migrating range child answers a count query from its parent", async () => {
			const N = 2;
			const { root, sks } = await makeRangeRoot(N);
			await withMigrationHeld(root, async (waitForAllChildRequests) => {
				const start = sks.length;
				sks.push(...(await root.triggerRangeSplit((i) => `sk${String(i + start).padStart(3, "0")}-${crypto.randomUUID()}`)));
				await root.awaitSplitStarted();
				await waitForAllChildRequests();

				const child = (await root.children())[0];
				const res = await child.stub.apiQueryItems(child.ctx, fullRequest({ select: "count" }));
				expect(res.items).toHaveLength(0);
				// The parent still holds every row of the key, so the count covers the whole range.
				expect(res.count).toBe(sks.length);
				expect(res.meta.forwardCount).toBe(0);
				expect(res.partitionMetas[0].servedByActorName).toBe(root.doName);
			});

			// Drain pending child migrations so their background work doesn't outlive the test.
			await root.awaitSplitCompleted();
		});

		it("sums SQL result rows across range leaves", async () => {
			const N = 4;
			const { root, sks } = await buildSplitTree(N);

			const res = await queryPage(root);
			// Every leaf drains its interval, so its one extra read returns no row.
			expect(res.rowsReturned).toBe(sks.length);
			expect(res.partitionMetas.reduce((s, m) => s + m.rowsRead, 0)).toBeGreaterThanOrEqual(sks.length);
			// The router itself reads no rows.
			expect(res.meta.rowsRead).toBe(0);
		});
	});

	describe("queryItems through a hash split", () => {
		it("count mode reports the forwarded leaf's counters", async () => {
			const root = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
			const writes = await root.triggerHashSplit();
			await root.awaitSplitCompleted();

			const hk = writes[0].hashKey;
			const expected = writes.filter((w) => KeyCodec.compare(w.hashKey, hk) === 0).length;
			const res = await root.stub.apiQueryItems(root.ctx, fullRequest({ hashKey: hk, select: "count" }));

			expect(res.count).toBe(expected);
			expect(res.items).toHaveLength(0);
			expect(res.meta.forwardCount).toBe(1);
			expect(res.partitionMetas).toHaveLength(1);
		});
	});
});
