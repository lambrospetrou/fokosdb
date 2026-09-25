import { runInDurableObject } from "cloudflare:test";
import { beforeAll, describe, expect, it } from "vitest";
import {
	PartitionDO,
	QueryItemsRpcRequest,
	QueryItemsRpcResponse,
	type ProjectedWireRow,
	type PutItemRpcRequest,
} from "../../src/server/do-partition.js";
import { KeyCodec, type KeyBytes } from "../../src/sharding/key-codec.js";
import { clipToChildRange } from "../../src/sharding/sk-interval.js";
import invariant from "../../src/shared/invariant.js";
import { MAX_ITEM_BYTES } from "../../src/shared/transaction-limits.js";
import { MAX_EVALUATED_BYTES_PER_PAGE, MAX_EVALUATED_ITEMS_PER_PAGE } from "../../src/shared/query/page-budget.js";
import { estimateProjectedRowBytes, PartitionStore, type StoredItem } from "../../src/shared/partition/partition-store.js";
import { compileQueryExpression } from "../../src/shared/expression/compiler.js";
import { EXPRESSION_LIMITS } from "../../src/shared/expression/limits.js";
import type { ProjectionExpression } from "../../src/shared/expression/types.js";
import { EST_ROW_BYTES_K } from "../../src/shared/partition/item-size.js";
import type { FokosEnvelope } from "../../src/sharding/runtime-types.js";
import { kb, makeStub, opened, type Opened } from "./helpers.js";
import {
	type TestPartition,
	CONTROLLED_NS,
	makePartition,
	makeRangeRoot,
	makeTriggeredRangeRoot,
	PROMOTION_TEST_MAX_SIZE_MB,
	withMigrationHeld,
	rangeOf,
} from "./partition-harness.js";

// The tests in this file operate one after the other. Some tests hold a migration open on a
// partition of `ControlledPartitionDO`. The hold is a field of that one instance, thus a test that
// operates at the same time cannot remove it.
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
		plan: null,
		...overrides,
	});

	/**
	 * The sort keys of `sks` that fall in `child`'s immutable [start, end) slice. A migrating child
	 * reads through its parent, which still holds every row of the key and answers only for this slice.
	 */
	const ownedByChild = (sks: string[], child: TestPartition): string[] => {
		const { startBoundary, endBoundary } = rangeOf(child.ctx);
		return [...sks]
			.sort()
			.filter(
				(sk) =>
					(startBoundary === null || KeyCodec.compare(kb(sk), startBoundary) >= 0) &&
					(endBoundary === null || KeyCodec.compare(kb(sk), endBoundary) < 0),
			);
	};

	describe("queryItems leaf pages", () => {
		const request = (direction: "asc" | "desc", overrides: Partial<QueryItemsRpcRequest> = {}) => fullRequest({ direction, ...overrides });

		// Every test reads a hash key of its own on one partition, so the partition is built once.
		let leaf: ReturnType<typeof makeStub>;
		beforeAll(() => {
			leaf = makeStub();
		});

		const seed45 = (state: DurableObjectState, hk: KeyBytes) => {
			const store = new PartitionStore(state.storage);
			for (let i = 0; i < 45; i++) {
				store.upsertItem({
					hk,
					sk: kb(String(i).padStart(3, "0")),
					data: "x",
					kind: "text",
					ttlAt: null,
					txOrderTs: 0,
				});
			}
		};

		it("reads one candidate beyond a full evaluated-item budget and returns an inclusive cursor", async () => {
			const { ctx, stub } = leaf;
			const hashKey = kb("lp-eval");
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				seed45(state, hashKey);

				const result = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, remainingEvaluatedItems: 10 })));

				expect(result.items).toHaveLength(10);
				expect(result.count).toBe(10);
				expect(result.scannedCount).toBe(10);
				expect(result.rowsReturned).toBe(11);
				expect(result.nextCursor?.inclusive).toBe(true);
				expect(KeyCodec.decode(result.nextCursor!.sk)).toBe("010");
			});
		});

		it("returns no cursor when the evaluated-item budget ends on the last candidate", async () => {
			const { ctx, stub } = leaf;
			const hashKey = kb("lp-last");
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				seed45(state, hashKey);

				const result = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, remainingEvaluatedItems: 45 })));

				expect(result.items).toHaveLength(45);
				expect(result.count).toBe(45);
				expect(result.rowsReturned).toBe(45);
				expect(result.nextCursor).toBeNull();
			});
		});

		it("count mode returns no items, zero response bytes, and the same page counters", async () => {
			const { ctx, stub } = leaf;
			const hashKey = kb("lp-count");
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				seed45(state, hashKey);

				const result = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, select: "count", remainingEvaluatedItems: 10 })));

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

		it("count mode with a filter returns the matched count below scannedCount", async () => {
			const { ctx, stub } = leaf;
			const hashKey = kb("lp-count-filter");
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				seed45(state, hashKey);
				const plan = compileQueryExpression({ filter: { op: "gte", args: [{ ref: "sortKey" }, { val: "040" }] } });

				const result = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, select: "count", plan })));

				expect(result.items).toEqual([]);
				expect(result.responseBytes).toBe(0);
				expect(result.count).toBe(5);
				expect(result.scannedCount).toBe(45);
				expect(result.rowsReturned).toBe(45);
				expect(result.nextCursor).toBeNull();
			});
		});

		it("a rejected candidate consumes the evaluated budgets and advances the cursor", async () => {
			const { ctx, stub } = leaf;
			const hashKey = kb("lp-rejected");
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				seed45(state, hashKey);
				const plan = compileQueryExpression({ filter: { op: "eq", args: [{ ref: "sortKey" }, { val: "044" }] } });

				const first = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, plan, remainingEvaluatedItems: 10 })));
				expect(first.count).toBe(0);
				expect(first.items).toEqual([]);
				expect(first.scannedCount).toBe(10);
				expect(first.rowsReturned).toBe(11);
				expect(first.nextCursor?.inclusive).toBe(true);
				expect(KeyCodec.decode(first.nextCursor!.sk)).toBe("010");

				let count = first.count;
				let scannedCount = first.scannedCount;
				const seen: string[] = [];
				let cursor = first.nextCursor;
				let pages = 0;
				while (cursor !== null) {
					const res = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, plan, remainingEvaluatedItems: 10, cursor })));
					count += res.count;
					scannedCount += res.scannedCount;
					seen.push(...res.items.map((it) => KeyCodec.decode((it as StoredItem).sk) as string));
					cursor = res.nextCursor;
					invariant(++pages < 50, "queryItems pagination did not terminate");
				}
				expect(count).toBe(1);
				expect(scannedCount).toBe(45);
				expect(seen).toEqual(["044"]);
			});
		});

		it("a rejected candidate spends zero response bytes", async () => {
			const { ctx, stub } = leaf;
			const hashKey = kb("lp-bigzero");
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				const store = new PartitionStore(state.storage);
				for (let i = 0; i < 6; i++) {
					store.upsertItem({
						hk: hashKey,
						sk: kb(`big${i}`),
						data: new Uint8Array(100 * 1024),
						kind: "bytes",
						ttlAt: null,
						txOrderTs: 0,
					});
				}
				const plan = compileQueryExpression({ filter: { op: "eq", args: [{ ref: "sortKey" }, { val: "big5" }] } });

				// 250 KiB admits two 100 KiB items but not six. The page drains only because the five
				// rejected candidates charged nothing to the response budget.
				const res = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, plan, remainingResponseBytes: 250 * 1024 })));
				expect(res.nextCursor).toBeNull();
				expect(res.scannedCount).toBe(6);
				expect(res.items).toHaveLength(1);
				expect(KeyCodec.decode((res.items[0] as StoredItem).sk)).toBe("big5");
			});
		});

		it("count and projection pages can stop at different positions and exchange cursors", async () => {
			const { ctx, stub } = leaf;
			const hashKey = kb("lp-exchange");
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				const store = new PartitionStore(state.storage);
				for (let i = 0; i < 6; i++) {
					store.upsertItem({
						hk: hashKey,
						sk: kb(`big${i}`),
						data: new Uint8Array(100 * 1024),
						kind: "bytes",
						ttlAt: null,
						txOrderTs: 0,
					});
				}

				// Projection stops when the response budget rejects the third item.
				const proj = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, remainingResponseBytes: 250 * 1024 })));
				expect(proj.items).toHaveLength(2);
				expect(proj.nextCursor).not.toBeNull();

				// Count ignores the response budget and drains the same interval.
				const cnt = opened(
					await instance.apiQueryItems(ctx, request("asc", { hashKey, select: "count", remainingResponseBytes: 250 * 1024 })),
				);
				expect(cnt.items).toEqual([]);
				expect(cnt.count).toBe(6);
				expect(cnt.nextCursor).toBeNull();

				// The projection cursor resumes under count at the rejected candidate.
				const cntResume = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, select: "count", cursor: proj.nextCursor })));
				expect(cntResume.count).toBe(4);
				expect(cntResume.nextCursor).toBeNull();

				// The count cursor resumes under projection and materializes the rest.
				const cnt3 = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, select: "count", remainingEvaluatedItems: 3 })));
				expect(cnt3.count).toBe(3);
				expect(cnt3.nextCursor).not.toBeNull();
				const projResume = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, cursor: cnt3.nextCursor })));
				expect(projResume.items).toHaveLength(3);
				expect(projResume.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual(["big3", "big4", "big5"]);
				expect(projResume.nextCursor).toBeNull();
			});
		});

		it("physical rowsRead is reported from SQLite and is not derived from the logical counters", async () => {
			const { ctx, stub } = leaf;
			const hashKey = kb("lp-rowsread");
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				seed45(state, hashKey);

				const result = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, remainingEvaluatedItems: 10 })));

				expect(result.meta.rowsRead).toBeGreaterThan(0);
				expect(result.partitionMetas[0].rowsRead).toBe(result.meta.rowsRead);
			});
		});

		it.each(["asc", "desc"] as const)(
			"a projection returns positional rows in %s order with the complete-item counters",
			async (direction) => {
				const { ctx, stub } = leaf;
				const hashKey = kb(`lp-projection-${direction}`);
				await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
					seed45(state, hashKey);
					const plan = compileQueryExpression({
						projection: [{ expr: { ref: "sortKey" } }, { expr: { ref: "v" } }],
					});

					const result = opened(await instance.apiQueryItems(ctx, request(direction, { hashKey, plan, remainingEvaluatedItems: 10 })));

					const expected = Array.from({ length: 10 }, (_, i) => [String(direction === "asc" ? i : 44 - i).padStart(3, "0"), 1]);
					expect(result.items).toEqual(expected);
					expect(result.count).toBe(10);
					expect(result.scannedCount).toBe(10);
					expect(result.rowsReturned).toBe(11);
					expect(result.nextCursor?.inclusive).toBe(true);
					expect(KeyCodec.decode(result.nextCursor!.sk)).toBe(direction === "asc" ? "010" : "034");
				});
			},
		);

		it("a projection page stops on the response budget over the projected rows", async () => {
			const { ctx, stub } = leaf;
			const hashKey = kb("lp-projbudget");
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				const store = new PartitionStore(state.storage);
				// A 60 KiB text cell estimates at 64 + 120 KiB, so two rows fit a 250 KiB page.
				const doc = JSON.stringify({ big: "x".repeat(60 * 1024) });
				for (let i = 0; i < 6; i++) {
					store.upsertItem({ hk: hashKey, sk: kb(`big${i}`), data: doc, kind: "json", ttlAt: null, txOrderTs: 0 });
				}
				const plan = compileQueryExpression({ projection: [{ expr: { ref: "data", path: "$.big" } }] });

				const res = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, plan, remainingResponseBytes: 250 * 1024 })));
				expect(res.items).toHaveLength(2);
				expect(res.count).toBe(2);
				expect(res.nextCursor?.inclusive).toBe(true);
				expect(KeyCodec.decode(res.nextCursor!.sk)).toBe("big2");
				// The estimate is exact: it charges the envelope and the cells, never est_row_bytes.
				expect(res.responseBytes).toBe(res.items.reduce((sum, item) => sum + estimateProjectedRowBytes(item as ProjectedWireRow), 0));

				// The first oversized projected row of a page is still admitted once.
				const oversized = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, plan, remainingResponseBytes: 1 })));
				expect(oversized.items).toHaveLength(1);
				expect(oversized.responseBytes).toBe(estimateProjectedRowBytes(oversized.items[0] as ProjectedWireRow));
				expect(oversized.nextCursor?.inclusive).toBe(true);
				expect(KeyCodec.decode(oversized.nextCursor!.sk)).toBe("big1");
			});
		});

		it("keeps undefined projected cells across the RPC hop", async () => {
			const { ctx, stub, rpc } = leaf;
			const hashKey = kb("lp-undefined");
			await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
				const store = new PartitionStore(state.storage);
				for (let i = 0; i < 6; i++) {
					// `opt` exists on every other item, so every other wire row has a missing first cell.
					const doc = i % 2 === 0 ? { opt: i } : { other: i };
					store.upsertItem({
						hk: hashKey,
						sk: kb(String(i).padStart(3, "0")),
						data: JSON.stringify(doc),
						kind: "json",
						ttlAt: null,
						txOrderTs: 0,
					});
				}
			});

			// Calling the stub directly crosses a real RPC boundary, which runInDurableObject does not.
			const plan = compileQueryExpression({
				projection: [{ expr: { ref: "data", path: "$.opt" } }, { expr: { ref: "sortKey" } }],
			});
			const result = await rpc.apiQueryItems(ctx, request("asc", { hashKey, plan }));

			expect(result.items).toHaveLength(6);
			for (const [i, item] of result.items.entries()) {
				const row = item as ProjectedWireRow;
				const sk = String(i).padStart(3, "0");
				if (i % 2 === 0) {
					expect(row).toEqual([i, sk]);
				} else {
					expect(row).toEqual([undefined, sk]);
					// A missing cell stays an own array element, not a hole a serializer dropped.
					expect(row).toHaveLength(2);
					expect(Object.hasOwn(row, "0")).toBe(true);
				}
			}
		});

		it("runs a projection at the entry limit through the leaf", async () => {
			const { ctx, stub } = leaf;
			const hashKey = kb("lp-limit");
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				const store = new PartitionStore(state.storage);
				const doc = Object.fromEntries(Array.from({ length: EXPRESSION_LIMITS.projectionEntries }, (_, k) => [`f${k}`, k]));
				store.upsertItem({ hk: hashKey, sk: kb("s"), data: JSON.stringify(doc), kind: "json", ttlAt: null, txOrderTs: 0 });
				const projection: ProjectionExpression[] = Array.from({ length: EXPRESSION_LIMITS.projectionEntries }, (_, k) => ({
					expr: { ref: "data", path: `$.f${k}` },
				}));
				const plan = compileQueryExpression({ projection });

				const res = opened(await instance.apiQueryItems(ctx, request("asc", { hashKey, plan })));
				expect(res.items).toHaveLength(1);
				expect(res.items[0]).toHaveLength(EXPRESSION_LIMITS.projectionEntries);
				expect((res.items[0] as ProjectedWireRow)[17]).toBe(17);
			});
		});

		it.each(["asc", "desc"] as const)("pages 400 KiB items without gaps or duplicates in %s order", async (direction) => {
			const { ctx, stub } = leaf;
			const hashKey = kb(`lp-huge-${direction}`);
			await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
				const store = new PartitionStore(state.storage);
				for (const sk of ["a", "b", "c"]) {
					const dataBytes = MAX_ITEM_BYTES - hashKey.byteLength - kb(sk).byteLength - EST_ROW_BYTES_K;
					store.upsertItem({
						hk: hashKey,
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
					const result: Opened<QueryItemsRpcResponse> = opened(
						await instance.apiQueryItems(
							ctx,
							request(direction, { hashKey, remainingResponseBytes: MAX_ITEM_BYTES + 100, remainingEvaluatedItems: 2, cursor }),
						),
					);
					seen.push(...result.items.map((item) => KeyCodec.decode((item as StoredItem).sk) as string));
					if (result.nextCursor === null) {
						break;
					}
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

		// Every read-only test walks the same settled N=4 tree, so it is built once. A test that
		// writes items or freezes a migration mid-flight builds a tree of its own.
		let sharedTree: { root: TestPartition; sks: string[] };
		beforeAll(async () => {
			sharedTree = await buildSplitTree(4);
		});

		// Children in ascending boundary order; the leftmost child has a null start boundary.
		const byBoundary = (children: TestPartition[]) =>
			[...children].sort((a, b) =>
				KeyCodec.compare(
					rangeOf(a.ctx).startBoundary ?? KeyCodec.encodeOptional(undefined),
					rangeOf(b.ctx).startBoundary ?? KeyCodec.encodeOptional(undefined),
				),
			);

		const queryPage = (root: TestPartition, overrides: Partial<QueryItemsRpcRequest> = {}) =>
			root.rpc.apiQueryItems(root.ctx, fullRequest(overrides));

		// A range partition serves only an interval inside its own, so a request to one is clipped to it first.
		const leafEnvelope = (leaf: TestPartition, overrides: Partial<QueryItemsRpcRequest> = {}) => {
			const { startBoundary, endBoundary } = rangeOf(leaf.ctx);
			return leaf.stub.apiQueryItems(
				leaf.ctx,
				fullRequest({ ...overrides, interval: clipToChildRange(overrides.interval ?? {}, startBoundary, endBoundary) }),
			);
		};

		const leafPage = async (leaf: TestPartition, overrides: Partial<QueryItemsRpcRequest> = {}) =>
			opened(await leafEnvelope(leaf, overrides));

		// Page through the whole result set, accumulating decoded sort keys, the summed page counters,
		// and the set of leaf DOs touched. `onPage` observes each raw page (count pages carry no items).
		const collect = async (
			root: TestPartition,
			overrides: Partial<QueryItemsRpcRequest> = {},
			onPage?: (res: Opened<QueryItemsRpcResponse>) => void,
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
				for (const it of res.items) {
					out.push(KeyCodec.decode((it as StoredItem).sk));
				}
				for (const m of res.partitionMetas) {
					leaves.add(m.servedByActorName);
				}
				if (res.nextCursor === null) {
					break;
				}
				cursor = res.nextCursor;
				invariant(pages < 1000, "queryItems pagination did not terminate");
			}
			return { sks: out, leaves, pages, count, scannedCount, rowsReturned };
		};

		it("returns every item across all N leaves in a single page (regression: must not stop at the leftmost leaf)", async () => {
			const N = 4;
			const { root, sks } = sharedTree;

			const res = await queryPage(root);
			expect(res.nextCursor).toBeNull();
			expect(res.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual([...sks].sort());

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
			const { root, sks } = sharedTree;

			const { sks: got, leaves, pages } = await collect(root, { remainingResponseBytes: 130 * 1024 });
			expect(pages).toBeGreaterThan(1); // genuinely multi-page
			expect(leaves.size).toBe(N); // every leaf eventually visited
			expect(got).toEqual([...sks].sort()); // complete and ordered
			expect(new Set(got.map(String)).size).toBe(got.length); // no duplicates
		});

		it("walks leaves in descending order for scanIndexForward=false", async () => {
			const { root, sks } = sharedTree;

			const { sks: got, leaves } = await collect(root, { direction: "desc", remainingResponseBytes: 130 * 1024 });
			expect(leaves.size).toBe(4);
			expect(got).toEqual([...sks].sort().reverse());
		});

		it("honors remainingEvaluatedItems across the walk (stops mid-fan-out with a resumable cursor)", async () => {
			const { root, sks } = sharedTree;
			expect(sks.length).toBeGreaterThanOrEqual(6);

			const res = await queryPage(root, { remainingEvaluatedItems: 5 });
			expect(res.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual([...sks].sort().slice(0, 5));
			expect(res.nextCursor).not.toBeNull();
		});

		it("caps the fan-out per page (remainingPartitionVisits) and resumes via a boundary cursor without gaps or duplicates", async () => {
			const N = 4;
			const { root, sks } = sharedTree;

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
			const { root, sks } = sharedTree;

			const { sks: got, leaves, pages } = await collect(root, { direction: "desc", remainingPartitionVisits: 1 });
			expect(pages).toBeGreaterThanOrEqual(N);
			expect(leaves.size).toBe(N);
			expect(got).toEqual([...sks].sort().reverse());
			expect(new Set(got.map(String)).size).toBe(got.length);
		});

		it(
			"fokosExecuteLocal reads the router's own local rows and never fans out to children (regression: infinite loop when children are migrating)",
			{ concurrent: false },
			async () => {
				// Scenario: a migrating range child reads through its parent. Before the fix, the direct-read
				// RPC on a range router called queryItemsAsRangeNode → walkRangeChildren → child.queryItems()
				// → child detects it's still migrating → parent direct read → … (infinite loop until the
				// subrequest depth limit is hit).
				//
				// fokosExecuteLocal always calls queryItemsLocal and bypasses the child routing. forwardCount=0
				// asserts that: a walk of the children would report one forward per child, migrated or not.
				const N = 2;
				const { root, sks } = await makeRangeRoot(N, { ns: CONTROLLED_NS });

				// Start the real split with child transaction-metadata responses held at the parent.
				// Check the migration state instead of assuming that child alarms have not run.
				await withMigrationHeld(root, async (waitForAllChildRequests) => {
					const start = sks.length;
					sks.push(...(await root.triggerRangeSplit((i) => `sk${String(i + start).padStart(3, "0")}-${crypto.randomUUID()}`)));
					await root.awaitSplitStarted();
					await waitForAllChildRequests();
					const children = await root.children();
					for (const child of children) {
						expect((await child.status()).migrationStatus).toBe("migration_migrating");
					}

					const caller = children[0];
					const result = opened(
						await Promise.resolve(
							root.stub.fokosExecuteLocal({
								op: "apiQueryItems",
								repartitionId: await root.splitRepartitionId(),
								caller: { partitionId: caller.ctx.partitionId, doName: caller.doName },
								request: fullRequest(),
							}) as FokosEnvelope<QueryItemsRpcResponse>,
						),
					);

					// The router's own DB still holds every item (parent rows are never deleted during a split),
					// but it answers only for the slice the calling child owns.
					const end = rangeOf(caller.ctx).endBoundary;
					const ownedByCaller = [...sks].sort().filter((sk) => end === null || KeyCodec.compare(kb(sk), end) < 0);
					expect(ownedByCaller.length, "the leftmost child should own part of the seeded range").toBeGreaterThan(0);
					expect(result.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual(ownedByCaller);
					// Local read only: no forwarding to children.
					expect(result.meta.forwardCount).toBe(0);
				});

				// Drain pending child migrations so their background work does not outlive the test.
				await root.awaitSplitCompleted();
			},
		);

		// A cursor promises more rows, so neither budget exit emits one after the walk covers every child
		// that can contribute. A cursor there costs the client a round trip that returns nothing.
		// `db.ts:queryItems` follows the same rule: it emits a cursor only when a later sub-query remains.
		it("emits no cursor when the byte budget lands on zero at the last leaf", async () => {
			const { root, sks } = sharedTree;

			// The exact bytes the whole scan consumes. Replayed as the budget, every leaf still drains
			// itself (each reports no cursor of its own) and the router's remaining bytes reach zero as the
			// LAST leaf finishes — the one case where "budget exhausted" does not mean "more rows exist".
			const full = await queryPage(root);
			expect(full.nextCursor).toBeNull();
			expect(full.responseBytes).toBeGreaterThan(0);

			const res = await queryPage(root, { remainingResponseBytes: full.responseBytes });
			expect(res.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual([...sks].sort());
			expect(res.nextCursor).toBeNull();
		});

		it("emits no cursor when the partition-visit cap is reached but every remaining child is outside the interval", async () => {
			const { root, sks } = sharedTree;
			const children = (await root.splitStatus()).childPartitionContexts;
			// Children are in ascending boundary order, so an exclusive upper bound at the third child's
			// start boundary leaves exactly the first two intersecting the query.
			const upper = rangeOf(children[2]).startBoundary!;

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
			expect(res.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual(expected);
			expect(res.nextCursor).toBeNull();
		});

		it.each(["asc", "desc"] as const)("count mode walks every leaf in %s order and returns no items", async (direction) => {
			const N = 4;
			const { root, sks } = sharedTree;

			const { count, scannedCount, leaves } = await collect(root, { select: "count", direction }, (res) => {
				expect(res.items).toHaveLength(0);
				expect(res.responseBytes).toBe(0);
			});
			expect(count).toBe(sks.length);
			expect(scannedCount).toBe(count);
			expect(leaves.size).toBe(N);
		});

		it("the evaluated-byte budget paginates across leaves without gaps or duplicates", async () => {
			const { root, sks } = sharedTree;

			const full = await queryPage(root);
			expect(full.evaluatedBytes).toBeGreaterThan(0);

			const { sks: got, pages } = await collect(root, { remainingEvaluatedBytes: Math.ceil(full.evaluatedBytes / 3) });
			expect(pages).toBeGreaterThan(1);
			expect(got).toEqual([...sks].sort());
			expect(new Set(got.map(String)).size).toBe(got.length);
		});

		it("the evaluated-item budget that lands on zero at the last leaf emits no cursor", async () => {
			const { root, sks } = sharedTree;

			const res = await queryPage(root, { remainingEvaluatedItems: sks.length });
			expect(res.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual([...sks].sort());
			expect(res.nextCursor).toBeNull();

			const cnt = await queryPage(root, { select: "count", remainingEvaluatedItems: sks.length });
			expect(cnt.count).toBe(sks.length);
			expect(cnt.nextCursor).toBeNull();
		});

		it("the first-item exception applies once per page, not once per leaf", async () => {
			const { root } = sharedTree;
			const children = byBoundary(await root.children());
			const c0 = children[0];
			const c1 = children[1];

			const leaf0 = await leafPage(c0);
			const leaf1 = await leafPage(c1);
			expect(leaf0.items.length).toBeGreaterThan(0);
			expect(leaf1.items.length).toBeGreaterThan(0);

			// One byte past leaf 0's response leaves no room for leaf 1's first item, and the first-item
			// exception was already spent by leaf 0 — the second leaf must be visited and reject its row.
			const res = await queryPage(root, { remainingResponseBytes: leaf0.responseBytes + 1 });
			expect(res.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual(
				leaf0.items.map((it) => KeyCodec.decode((it as StoredItem).sk)),
			);
			expect(res.partitionMetas).toHaveLength(2);
			expect(res.rowsReturned).toBe(leaf0.rowsReturned + 1);
			expect(res.nextCursor?.inclusive).toBe(true);
			expect(KeyCodec.compare(res.nextCursor!.sk, (leaf1.items[0] as StoredItem).sk)).toBe(0);

			// The same oversized first item is admitted when it starts the page.
			const res2 = await queryPage(root, {
				remainingResponseBytes: 1,
				interval: { lower: { value: rangeOf(c1.ctx).startBoundary!, inclusive: true } },
			});
			expect(res2.items).toHaveLength(1);
		});

		it("a descending page that stops on a child start boundary evaluates the boundary item on the next page", async () => {
			const N = 4;
			// Writes its own boundary item, so it cannot use the shared tree.
			const { root, sks } = await buildSplitTree(N);
			const children = byBoundary(await root.children());
			const B = rangeOf(children[2].ctx).startBoundary!;
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
			expect(KeyCodec.compare((p2.items[0] as StoredItem).sk, B)).toBe(0);

			const { sks: got } = await collect(root, { direction: "desc", remainingEvaluatedItems: K });
			expect(got).toEqual([...sks].sort().reverse());
			expect(new Set(got.map(String)).size).toBe(got.length);
		});

		it("a count page that spends the visit budget on empty leaves returns count 0 with a cursor", async () => {
			const N = 4;
			// Deletes most items, so it cannot use the shared tree.
			const { root, sks } = await buildSplitTree(N);
			const children = byBoundary(await root.children());
			const owns = (child: TestPartition, sk: string) => {
				const start = rangeOf(child.ctx).startBoundary ?? KeyCodec.encodeOptional(undefined);
				const end = rangeOf(child.ctx).endBoundary;
				return KeyCodec.compare(kb(sk), start) >= 0 && (end === null || KeyCodec.compare(kb(sk), end) < 0);
			};

			let remaining = 0;
			for (const sk of sks) {
				if (owns(children[0], sk) || owns(children[1], sk)) {
					await root.rpc.apiDeleteItem(root.ctx, { hashKey: kb("alice"), sortKey: kb(sk) });
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
			const left = children.find((c) => rangeOf(c.ctx).startBoundary === null)!;
			const grandchildren = byBoundary(await left.splitRange("aa"));
			const g2Start = rangeOf(grandchildren[1].ctx).startBoundary!;

			// Delete every item the right grandchild owns; it must then drain empty on the next page.
			const under = await leafPage(left);
			const leftSks = under.items.map((it) => (it as StoredItem).sk);
			for (const sk of leftSks) {
				if (KeyCodec.compare(sk, g2Start) >= 0) {
					await root.rpc.apiDeleteItem(root.ctx, { hashKey: kb("alice"), sortKey: sk });
				}
			}
			const g1Sks = leftSks.filter((sk) => KeyCodec.compare(sk, g2Start) < 0);
			expect(g1Sks.length).toBeGreaterThan(0);

			const r = await leafPage(left);
			expect(r.nextCursor).toBeNull();
			expect(r.lastEvaluatedCursor).not.toBeNull();
			expect(KeyCodec.compare(r.lastEvaluatedCursor!.sk, g1Sks[g1Sks.length - 1])).toBe(0);
			expect(r.partitionMetas).toHaveLength(2);
			expect(r.items.length).toBe(g1Sks.length);
			expect(r.count).toBe(g1Sks.length);
			expect(r.scannedCount).toBe(g1Sks.length);
		}, 30_000);

		it("a migrating range child answers a count query from its parent", { concurrent: false }, async () => {
			const N = 2;
			const { root, sks } = await makeRangeRoot(N, { ns: CONTROLLED_NS });
			await withMigrationHeld(root, async (waitForAllChildRequests) => {
				const start = sks.length;
				sks.push(...(await root.triggerRangeSplit((i) => `sk${String(i + start).padStart(3, "0")}-${crypto.randomUUID()}`)));
				await root.awaitSplitStarted();
				await waitForAllChildRequests();

				const child = (await root.children())[0];
				const envelope = await leafEnvelope(child, { select: "count" });
				const res = opened(envelope);
				expect(res.items).toHaveLength(0);
				// The parent still holds every row of the key, but it answers a read-through only for the
				// slice the calling child owns — counting the whole range here would count a sibling's rows.
				const owned = ownedByChild(sks, child);
				expect(owned.length).toBeGreaterThan(0);
				expect(owned.length).toBeLessThan(sks.length);
				expect(res.count).toBe(owned.length);
				// One RPC to the source, which executed the scan; the child is listed as the owner it read through for.
				expect(res.meta.forwardCount).toBe(1);
				expect(res.meta.servedByActorName).toBe(root.doName);
				expect(res.partitionMetas[0].servedByActorName).toBe(root.doName);
				expect(envelope.routing.servedBy.map((n) => [n.ref.doName, n.role])).toEqual([
					[child.doName, "read_through"],
					[root.doName, "executed"],
				]);
			});

			// Drain pending child migrations so their background work does not outlive the test.
			await root.awaitSplitCompleted();
		});

		it("returns projected rows from every leaf, with missing cells intact", async () => {
			const N = 4;
			const { root, sks } = sharedTree;
			// The seeded items are text, so the $.opt cell is missing on every row.
			const plan = compileQueryExpression({
				projection: [{ expr: { ref: "data", path: "$.opt" } }, { expr: { ref: "sortKey" } }],
			});

			const res = await queryPage(root, { plan });
			expect(res.nextCursor).toBeNull();
			const expected = [...sks].sort();
			expect(res.items).toHaveLength(expected.length);
			for (const [i, item] of res.items.entries()) {
				const row = item as ProjectedWireRow;
				expect(row).toEqual([undefined, expected[i]]);
				expect(row).toHaveLength(2);
				expect(Object.hasOwn(row, "0")).toBe(true);
			}
			// The fan-out touched every leaf.
			expect(new Set(res.partitionMetas.map((m) => m.servedByActorName)).size).toBe(N);
		});

		it("a filter marks only matched rows across every leaf", async () => {
			const N = 4;
			const { root, sks } = sharedTree;
			const sorted = [...sks].sort();
			const median = sorted[Math.floor(sorted.length / 2)];
			const plan = compileQueryExpression({ filter: { op: "gte", args: [{ ref: "sortKey" }, { val: median }] } });

			const res = await queryPage(root, { plan });
			const expected = sorted.filter((sk) => sk >= median);
			expect(res.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual(expected);
			expect(res.count).toBe(expected.length);
			expect(res.scannedCount).toBe(sks.length);
			expect(res.nextCursor).toBeNull();
			// The fan-out still touched every leaf: the filter rejects rows, not partitions.
			expect(res.partitionMetas).toHaveLength(N);
		});

		it("a migrating range child answers a projected query from its parent", { concurrent: false }, async () => {
			const N = 2;
			const { root, sks } = await makeRangeRoot(N, { ns: CONTROLLED_NS });
			await withMigrationHeld(root, async (waitForAllChildRequests) => {
				const start = sks.length;
				sks.push(...(await root.triggerRangeSplit((i) => `sk${String(i + start).padStart(3, "0")}-${crypto.randomUUID()}`)));
				await root.awaitSplitStarted();
				await waitForAllChildRequests();

				const child = (await root.children())[0];
				const plan = compileQueryExpression({ projection: [{ expr: { ref: "sortKey" } }] });
				const res = await leafPage(child, { plan });
				// The parent still holds every row of the key, but it clips the page to the calling child's
				// slice, so the child never serves rows a sibling owns.
				const owned = ownedByChild(sks, child);
				expect(owned.length).toBeGreaterThan(0);
				expect(owned.length).toBeLessThan(sks.length);
				expect(res.items.map((item) => (item as ProjectedWireRow)[0])).toEqual(owned);
				expect(res.count).toBe(owned.length);
				expect(res.meta.forwardCount).toBe(1);
				expect(res.partitionMetas[0].servedByActorName).toBe(root.doName);
			});

			// Drain pending child migrations so their background work does not outlive the test.
			await root.awaitSplitCompleted();
		});

		it("a migrating range child answers a filtered query from its parent", { concurrent: false }, async () => {
			const N = 2;
			const { root, sks } = await makeRangeRoot(N, { ns: CONTROLLED_NS });
			await withMigrationHeld(root, async (waitForAllChildRequests) => {
				const start = sks.length;
				sks.push(...(await root.triggerRangeSplit((i) => `sk${String(i + start).padStart(3, "0")}-${crypto.randomUUID()}`)));
				await root.awaitSplitStarted();
				await waitForAllChildRequests();

				const sorted = [...sks].sort();
				const median = sorted[Math.floor(sorted.length / 2)];
				const plan = compileQueryExpression({
					filter: { op: "gte", args: [{ ref: "sortKey" }, { val: median }] },
					projection: [{ expr: { ref: "sortKey" } }],
				});
				// The upper child owns the matching half; the filter and the slice clip independently, so the
				// page is the intersection and the scan covers only the clipped interval.
				const child = (await root.children())[1];
				const res = await leafPage(child, { plan });
				const owned = ownedByChild(sks, child);
				const expected = owned.filter((sk) => sk >= median);
				expect(expected.length).toBeGreaterThan(0);
				expect(res.items.map((item) => (item as ProjectedWireRow)[0])).toEqual(expected);
				expect(res.count).toBe(expected.length);
				expect(res.scannedCount).toBe(owned.length);
				expect(res.meta.forwardCount).toBe(1);
				expect(res.partitionMetas[0].servedByActorName).toBe(root.doName);
			});

			// Drain pending child migrations so their background work does not outlive the test.
			await root.awaitSplitCompleted();
		});

		it("sums SQL result rows across range leaves", async () => {
			const { root, sks } = sharedTree;

			const res = await queryPage(root);
			// Every leaf drains its interval, so its one extra read returns no row.
			expect(res.rowsReturned).toBe(sks.length);
			expect(res.partitionMetas.reduce((s, m) => s + m.rowsRead, 0)).toBeGreaterThanOrEqual(sks.length);
			// The router itself reads no rows, so it is not a leaf of the page.
			expect(res.partitionMetas.map((m) => m.servedByActorName)).not.toContain(root.doName);
		});
	});

	// A point read and a range request share one learned-boundary table: an `apiGetItem` that reaches a
	// range partition answers with its ancestors, and the hash partition stores them. Only the range
	// planner's reader keeps the two apart, because it selects a learned slice per segment and only
	// when the slice contains that segment whole. These tests drive that cross-feed from outside, so a
	// reader that entered by a single key instead would answer for one slice and lose the rest.
	//
	// The LEFT edge is the case that matters. A slice with an unbounded start is the only one a
	// byte-minimum key matches, so a request that carries no sort key lands there and nowhere else.
	describe("queryItems after a point read taught the range hierarchy", () => {
		// A promoted key whose range tree has a deeper left edge than the rest: the root splits into N
		// leaves, and the leftmost leaf — the one with the unbounded start — then splits again.
		const buildDeepLeftEdge = async (N: number) => {
			const { root, sks, hashPartition } = await makeTriggeredRangeRoot(N);
			await root.awaitSplitCompleted();

			const leftmost = (await root.children()).find((c) => rangeOf(c.ctx).startBoundary === null);
			invariant(leftmost, "the range root has a child with an unbounded start");
			// "sa" sorts below every "sk" filler, so these land inside the leftmost leaf whatever its
			// end boundary is. triggerRangeSplit asserts that for each key it writes.
			const deepSks = await leftmost.triggerRangeSplit((i) => `sa${String(i).padStart(4, "0")}`);
			await leftmost.awaitSplitCompleted();

			return { hashPartition, root, all: [...sks, ...deepSks].sort(), deepSks };
		};

		// Both tests read through the same deep tree and only warm its route cache, so it is built once.
		let sharedDeep: Awaited<ReturnType<typeof buildDeepLeftEdge>>;
		beforeAll(async () => {
			sharedDeep = await buildDeepLeftEdge(4);
		});

		it("returns every item after a point read of the left edge taught a deep slice", async () => {
			const { hashPartition, all, deepSks } = sharedDeep;

			// The point read teaches the hash partition the deep left-edge slice that holds this key.
			const read = await hashPartition.get({ hashKey: kb("alice"), sortKey: kb(deepSks[0]) });
			expect(read.found).toBe(true);

			// The query must still cover the whole key, not the one slice the point read taught.
			const res = await hashPartition.rpc.apiQueryItems(hashPartition.ctx, fullRequest());
			expect(res.nextCursor).toBeNull();
			expect(res.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual(all);
		});

		it("returns every item when a point read of each left-edge slice taught the whole left chain", async () => {
			const { hashPartition, all, deepSks } = sharedDeep;

			// Teach both sides of the deepest split, so the cache holds the entire left chain and not
			// just its outermost slice.
			for (const sk of [deepSks[0], deepSks[deepSks.length - 1]]) {
				expect((await hashPartition.get({ hashKey: kb("alice"), sortKey: kb(sk) })).found).toBe(true);
			}

			const res = await hashPartition.rpc.apiQueryItems(hashPartition.ctx, fullRequest());
			expect(res.nextCursor).toBeNull();
			expect(res.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual(all);
		});
	});

	describe("queryItems through a hash split", () => {
		// All three tests query the same completed two-child split; only the query differs.
		let shared: { root: TestPartition; writes: PutItemRpcRequest[] };
		beforeAll(async () => {
			const root = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
			const writes = await root.triggerHashSplit();
			await root.awaitSplitCompleted();
			shared = { root, writes };
		});

		it("count mode reports the forwarded leaf's counters", async () => {
			const { root, writes } = shared;
			const hk = writes[0].hashKey;
			const expected = writes.filter((w) => KeyCodec.compare(w.hashKey, hk) === 0).length;
			const res = await root.rpc.apiQueryItems(root.ctx, fullRequest({ hashKey: hk, select: "count" }));

			expect(res.count).toBe(expected);
			expect(res.items).toHaveLength(0);
			expect(res.meta.forwardCount).toBe(1);
			expect(res.partitionMetas).toHaveLength(1);
		});

		it("a filter that matches nothing still reports the forwarded leaf's evaluated count", async () => {
			const { root, writes } = shared;
			const hk = writes[0].hashKey;
			const expected = writes.filter((w) => KeyCodec.compare(w.hashKey, hk) === 0).length;
			// The seeded rows carry no TTL, so exists(ttlAt) matches no candidate.
			const plan = compileQueryExpression({ filter: { op: "exists", args: [{ ref: "ttlAt" }] } });
			const res = await root.rpc.apiQueryItems(root.ctx, fullRequest({ hashKey: hk, plan }));

			expect(res.count).toBe(0);
			expect(res.items).toHaveLength(0);
			expect(res.scannedCount).toBe(expected);
			expect(res.meta.forwardCount).toBe(1);
			expect(res.partitionMetas).toHaveLength(1);
		});

		it("projection mode returns the forwarded leaf's projected rows", async () => {
			const { root, writes } = shared;
			const hk = writes[0].hashKey;
			const expected = writes.filter((w) => KeyCodec.compare(w.hashKey, hk) === 0).length;
			const plan = compileQueryExpression({ projection: [{ expr: { ref: "sortKey" } }, { expr: { ref: "v" } }] });
			const res = await root.rpc.apiQueryItems(root.ctx, fullRequest({ hashKey: hk, plan }));

			expect(res.count).toBe(expected);
			expect(res.items).toEqual(Array.from({ length: expected }, () => ["sk", 1]));
			expect(res.meta.forwardCount).toBe(1);
			expect(res.partitionMetas).toHaveLength(1);
		});
	});

	describe("queryItems through a promoted key", () => {
		// Both tests run read-only queries on the same promoted range root, so it is built once.
		let shared: { partition: TestPartition; writes: PutItemRpcRequest[]; rangeRoot: TestPartition };
		beforeAll(async () => {
			const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
			const writes = await partition.triggerPromotion("alice", (i) => `sk${String(i).padStart(3, "0")}`);
			const rangeRoot = await partition.awaitPromoted("alice");
			shared = { partition, writes, rangeRoot };
		});

		it("serves a filtered page from the range root", async () => {
			const { partition, writes, rangeRoot } = shared;
			const sorted = writes.map((w) => KeyCodec.decode(w.sortKey!) as string).sort();
			const median = sorted[Math.floor(sorted.length / 2)];
			const plan = compileQueryExpression({ filter: { op: "gte", args: [{ ref: "sortKey" }, { val: median }] } });
			const res = await partition.rpc.apiQueryItems(partition.ctx, fullRequest({ plan }));

			const expected = sorted.filter((sk) => sk >= median);
			expect(res.items.map((it) => KeyCodec.decode((it as StoredItem).sk))).toEqual(expected);
			expect(res.count).toBe(expected.length);
			expect(res.scannedCount).toBe(writes.length);
			expect(res.partitionMetas[0].servedByActorName).toBe(rangeRoot.doName);
		});

		it("serves a projected page from the range root", async () => {
			const { partition, writes, rangeRoot } = shared;
			const plan = compileQueryExpression({ projection: [{ expr: { ref: "sortKey" } }] });
			const res = await partition.rpc.apiQueryItems(partition.ctx, fullRequest({ plan }));

			const expected = writes.map((w) => KeyCodec.decode(w.sortKey!) as string).sort();
			expect(res.items.map((item) => (item as ProjectedWireRow)[0])).toEqual(expected);
			expect(res.count).toBe(writes.length);
			expect(res.meta.forwardCount).toBeGreaterThanOrEqual(1);
			expect(res.partitionMetas[0].servedByActorName).toBe(rangeRoot.doName);
		});
	});
});
