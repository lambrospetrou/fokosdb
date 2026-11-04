import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBPartitionDO } from "../../src/server/do-partition.js";
import { asRoutingHash, compareRoutingHashes, routingHashInterval, routingHashOf } from "../../src/sharding/hash-primitives.js";
import { KeyCodec, type KeyBytes } from "../../src/sharding/key-codec.js";
import { PartitionIdHelper, resolveRangePartitionContext } from "../../src/sharding/partition-id.js";
import { hashStorageInterval, rangeStorageInterval } from "../../src/sharding/repartition-slice.js";
import { FokosRouter } from "../../src/sharding/router.js";
import type { FokosStorageInterval } from "../../src/sharding/runtime-types.js";
import type { QueryItemsRpcRequest } from "../../src/server/do-partition.js";
import invariant from "../../src/shared/invariant.js";
import type { StoredItem } from "../../src/shared/partition/partition-store.js";
import { MAX_EVALUATED_BYTES_PER_PAGE, MAX_EVALUATED_ITEMS_PER_PAGE } from "../../src/shared/query/page-budget.js";
import { collidingHashKeys } from "../hash-digits.js";
import { kb, makeStub } from "./helpers.js";
import { drainUntil, makePartition, TestPartition } from "./partition-harness.js";

/** A query of every row of `hashKey`, with every budget wide open. */
const queryAll = (hashKey: KeyBytes): QueryItemsRpcRequest => ({
	hashKey,
	interval: {},
	direction: "asc",
	remainingEvaluatedItems: MAX_EVALUATED_ITEMS_PER_PAGE,
	remainingEvaluatedBytes: MAX_EVALUATED_BYTES_PER_PAGE,
	remainingResponseBytes: 64 * 1024 * 1024,
	remainingPartitionVisits: 100,
	allowOversizedFirstItem: true,
	cursor: null,
	select: "projection",
	plan: null,
});

type Intervals = { scope: FokosStorageInterval; owned: FokosStorageInterval | null };

async function intervalsOf(partition: TestPartition): Promise<Intervals> {
	return await runInDurableObject(partition.stub, (instance: FokosDBPartitionDO) => ({
		scope: instance.fokos.storageInterval(),
		owned: instance.fokos.ownedStorageInterval(),
	}));
}

/** The storage interval of the hash node at `idxs`: the root index, then one child index for each level. */
function nodeInterval(partition: TestPartition, idxs: number[]): FokosStorageInterval {
	const { start, end } = routingHashInterval(partition.ctx.topology.rootTreesN, partition.ctx.topology.hashSplitN, idxs[0], idxs.slice(1));
	return hashStorageInterval(start, end);
}

/** The count of item rows of a partition, and whether each row is inside `interval`. */
async function rowsOf(partition: TestPartition, interval: FokosStorageInterval): Promise<{ rows: number; outside: number }> {
	return await runInDurableObject(partition.stub, (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
		const count = (where: string, ...params: unknown[]) =>
			state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items ${where}`, ...params).one().n;
		const rows = count("");
		const inside = interval.end
			? count(`WHERE routing_hash >= ? AND routing_hash < ?`, interval.start.prefix, interval.end.prefix)
			: count(`WHERE routing_hash >= ?`, interval.start.prefix);
		return { rows, outside: rows - inside };
	});
}

/** The item rows of one hash key in the storage of a partition. */
async function localRows(partition: TestPartition, hashKey: KeyBytes): Promise<number> {
	return await runInDurableObject(
		partition.stub,
		(_instance: FokosDBPartitionDO, state: DurableObjectState) =>
			state.storage.sql
				.exec<{
					n: number;
				}>(
					`SELECT COUNT(*) AS n FROM items WHERE routing_hash = ? AND hk = ?`,
					routingHashOf(hashKey, partition.ctx.topology.shardGroup),
					hashKey,
				)
				.one().n,
	);
}

/** Splits `parent` and checks that each child holds exactly the rows of the parent inside its own interval. */
async function splitAndCheckChildren(parent: TestPartition): Promise<TestPartition[]> {
	// A root gets its identity from its first request.
	await parent.status();
	const before = await intervalsOf(parent);
	const children = await parent.splitHash();
	// A router keeps its scope, but owns no rows in it.
	expect(await intervalsOf(parent)).toEqual({ scope: before.scope, owned: null });
	let childRows = 0;
	for (const child of children) {
		const id = Uint8Array.fromHex(child.ctx.partitionId);
		const idxs = [PartitionIdHelper.rootIdx(id), ...id.subarray(4, 4 + PartitionIdHelper.depth(id))];
		const interval = nodeInterval(child, idxs);
		expect(await intervalsOf(child)).toEqual({ scope: interval, owned: interval });
		const { rows, outside } = await rowsOf(child, interval);
		expect(outside).toBe(0);
		// A split keeps its rows, so the parent still holds the rows of each child.
		expect((await rowsOf(parent, interval)).rows - (await rowsOf(parent, interval)).outside).toBe(rows);
		childRows += rows;
	}
	expect(childRows).toBe((await rowsOf(parent, before.scope)).rows);
	return children;
}

describe("FokosDBPartitionDO — storage intervals", () => {
	it(
		"a hash child imports the rows of its own interval, under each root and below a parent that is not a root",
		{ timeout: 120_000 },
		async () => {
			const { ctx } = makeStub({ rootTreesN: 2, hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			const [root0, root1] = new FokosRouter(ctx.topology, ctx.rangeConfig, ctx.policy, ctx.policyVersion)
				.allRoots()
				.map((root) => TestPartition.at(root));
			const [child00] = await splitAndCheckChildren(root0);
			const [child10] = await splitAndCheckChildren(root1);
			// The same slice fields, child 0 at depth 1, under two roots: two intervals.
			expect((await intervalsOf(child00)).scope).toEqual(nodeInterval(child00, [0, 0]));
			expect((await intervalsOf(child10)).scope).toEqual(nodeInterval(child10, [1, 0]));
			expect((await intervalsOf(child00)).scope).not.toEqual((await intervalsOf(child10)).scope);
			await splitAndCheckChildren(child10);
		},
	);

	it("a range partition has the interval of the rows of its one routingHash", async () => {
		const partition = makePartition();
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
		const rangeRoot = await partition.awaitPromoted("alice");
		const routingHash = routingHashOf(kb("alice"), partition.ctx.topology.shardGroup);
		const interval = rangeStorageInterval(routingHash, null, null);
		expect(await intervalsOf(rangeRoot)).toEqual({ scope: interval, owned: interval });
		// The interval starts at the routingHash and ends at the start of the next routingHash.
		expect(interval.start).toEqual({ prefix: routingHash });
		// The end bound is the start of the next routingHash, which is a routingHash value itself.
		expect(interval.end === null || compareRoutingHashes(asRoutingHash(interval.end.prefix), routingHash) > 0).toBe(true);
	});

	// Two hash keys can have one routingHash. A promotion moves the whole routingHash, so both keys
	// move to the range tree. A range split can then put a boundary between the two keys.
	it(
		"promotes two hash keys of one routingHash together, and a range split between them separates their rows",
		{ timeout: 120_000 },
		async () => {
			const partition = makePartition({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 1 } });
			const group = partition.ctx.topology.shardGroup;
			const [lower, upper] = collidingHashKeys(group)
				.map((raw) => KeyCodec.encode(raw))
				.sort(KeyCodec.compare);
			const routingHash = routingHashOf(lower, group);
			expect(routingHashOf(upper, group)).toEqual(routingHash);
			for (const hashKey of [lower, upper]) {
				for (const sk of ["s1", "s2", "s3"]) {
					await partition.put({ hashKey, sortKey: kb(sk), data: "d", kind: "text" });
				}
			}
			await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: lower });
			const rangeRoot = TestPartition.at(resolveRangePartitionContext(partition.ctx, routingHash, null, null));
			await drainUntil(
				[partition, rangeRoot],
				async () =>
					(await localRows(partition, lower)) + (await localRows(partition, upper)) === 0 &&
					(await localRows(rangeRoot, lower)) === 3 &&
					(await localRows(rangeRoot, upper)) === 3,
				"the promotion to move the rows of both hash keys",
			);
			await rangeRoot.awaitMigrationCompleted();

			// Write one row of each key in turn, until the root starts a split. The rows of the lower key are
			// a little smaller, so the byte middle falls on the first row of the upper key, and the
			// boundary is between the two keys.
			const rows = { lower: 3, upper: 3 };
			for (let i = 0; !(await rangeRoot.status()).splitStatus; i++) {
				invariant(i < 100, "no range split");
				const hashKey = i % 2 === 0 ? upper : lower;
				const data = "x".repeat(i % 2 === 0 ? 50 * 1024 : 50 * 1024 - 100);
				await rangeRoot.put({ hashKey, sortKey: kb(`f${String(i).padStart(3, "0")}`), data, kind: "text" });
				rows[hashKey === upper ? "upper" : "lower"]++;
			}
			await rangeRoot.awaitSplitCompleted();
			const [left, right] = await rangeRoot.children();
			for (const [child, own, other] of [
				[left, lower, upper],
				[right, upper, lower],
			] as const) {
				expect(await localRows(child, own)).toBe(own === lower ? rows.lower : rows.upper);
				expect(await localRows(child, other)).toBe(0);
			}

			// A query of each key from the hash partition returns only the rows of that key.
			for (const [hashKey, count] of [
				[lower, rows.lower],
				[upper, rows.upper],
			] as const) {
				const res = await partition.rpc.apiQueryItems(partition.ctx, queryAll(hashKey));
				expect(res.nextCursor).toBeNull();
				expect(res.items).toHaveLength(count);
				expect(res.items.every((it) => KeyCodec.compare((it as StoredItem).hk, hashKey) === 0)).toBe(true);
			}
		},
	);
});
