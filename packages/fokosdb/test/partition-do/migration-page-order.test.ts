/**
 * The target refuses an items page whose rows are not in storage order after the page cursor.
 */
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBPartitionDO } from "../../src/server/do-partition.js";
import { FokosMigrationHost, type FokosDBHostPage } from "../../src/shared/partition/fokos-migration-host.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import type { RouteKey } from "../../src/sharding/repartition-types.js";
import { kb } from "./helpers.js";
import { makePartition } from "./partition-harness.js";
import { ALL_ROWS, partitionRoutingHashOf } from "../routing-hash.js";

const OUT_OF_ORDER = expect.objectContaining({ attributes: { detail: expect.stringContaining("out of storage order") } });

describe("FokosMigrationHost.validatePage", () => {
	it("refuses an items page with a row out of storage order, or at or below its cursor", async () => {
		const p = makePartition();
		for (const sk of ["s1", "s2", "s3"]) {
			await p.put({ hashKey: kb("a"), sortKey: kb(sk), data: sk, kind: "text" });
		}
		await runInDurableObject(p.stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
			const routingHashForKey = partitionRoutingHashOf(state);
			const host = new FokosMigrationHost({
				store: new PartitionStore(state.storage),
				logParams: () => ({}),
				captureChanges: () => false,
				selfId: () => "self",
				routingHashForKey,
			});
			const slice = { kind: "promoted_routing_hash" as const, routingHash: routingHashForKey(kb("a")) };
			const inSlice = (key: RouteKey) => KeyCodec.compare(key.hashKey, kb("a")) === 0;
			const budget = { pageBytes: 1 << 20, pageRows: 10, scanRows: 100 };
			const { page, nextCursor } = host.buildPage(null, slice, inSlice, budget, ALL_ROWS);
			const items = (page as Extract<FokosDBHostPage, { stream: "items" }>).items;
			expect(items.map((item) => KeyCodec.decode(item.sk))).toEqual(["s1", "s2", "s3"]);
			host.validatePage(null, page, nextCursor, inSlice);

			const swapped = { stream: "items", items: [items[0], items[2], items[1]] };
			expect(() => host.validatePage(null, swapped, nextCursor, inSlice)).toThrow(OUT_OF_ORDER);
			const repeated = { stream: "items", items: [items[0], items[0], items[2]] };
			expect(() => host.validatePage(null, repeated, nextCursor, inSlice)).toThrow(OUT_OF_ORDER);
			const first = items[0];
			const atCursor = { stream: "items", cursor: { routingHash: first.routingHash, hk: first.hk, sk: first.sk } };
			expect(() => host.validatePage(atCursor, page, nextCursor, inSlice)).toThrow(OUT_OF_ORDER);
		});
	});
});
