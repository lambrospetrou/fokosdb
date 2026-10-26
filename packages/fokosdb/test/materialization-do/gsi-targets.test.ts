import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { FokosDBMaterializationDO } from "../../src/server/materialization/do-materialization.js";
import type { FokosDBPartitionDO } from "../../src/server/do-partition.js";
import { partitionStubByName } from "../../src/shared/do-stubs.js";
import { decodeGsiSortKey } from "../../src/shared/gsi/sort-key.js";
import { gsiTableConfig, type FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { FokosRouter } from "../../src/sharding/router.js";
import { KeyCodec, type KeyBytes } from "../../src/sharding/key-codec.js";
import { kb, makeStub, materializationOf, TARGET_WAIT } from "../partition-do/helpers.js";

type Entry = { sortKey?: string; baseHashKey: string; baseSortKey?: string; data: unknown };

const text = (key: KeyBytes) => (key.length === 0 ? undefined : (KeyCodec.decode(key) as string));

/** The entries of one GSI hash key, from the GSI root partition that owns it. */
async function gsiEntries(base: FokosDBRouteContext, indexName: string, hashKey: string): Promise<Entry[]> {
	const table = gsiTableConfig(
		{ topology: base.topology, rangeConfig: base.rangeConfig, policy: base.policy, policyVersion: base.policyVersion },
		indexName,
	);
	const ctx = new FokosRouter(table.topology, table.rangeConfig, table.policy, table.policyVersion).rootContext(kb(hashKey));
	const stub = partitionStubByName(env, ctx, ctx.doName);
	const rows = await runInDurableObject(stub, (_instance: FokosDBPartitionDO, state: DurableObjectState) =>
		state.storage.sql
			.exec<{ sk: ArrayBuffer; data: string }>(`SELECT sk, json(data) AS data FROM items WHERE hk = ? ORDER BY sk`, kb(hashKey))
			.toArray(),
	);
	return rows.map((row) => {
		const parts = decodeGsiSortKey(KeyCodec.decode(KeyCodec.asKeyBytes(new Uint8Array(row.sk))) as Uint8Array);
		const sortKey = text(parts.gsiSK);
		const baseSortKey = text(parts.baseSK);
		return {
			...(sortKey === undefined ? {} : { sortKey }),
			baseHashKey: text(parts.baseHK) as string,
			...(baseSortKey === undefined ? {} : { baseSortKey }),
			data: JSON.parse(row.data) as unknown,
		};
	});
}

async function materializationRows(stub: DurableObjectStub<FokosDBMaterializationDO>) {
	return await runInDurableObject(stub, (_instance: FokosDBMaterializationDO, state: DurableObjectState) => {
		const count = (table: string) => state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table}`).one().n;
		return {
			items: count("latest_items"),
			keySets: count("target_index_keys"),
			retries: state.storage.sql.exec(`SELECT target_id, kind, last_error FROM target_retry`).toArray(),
		};
	});
}

function gsiTable() {
	const t = makeStub({ captureChanges: true, nsMat: "GSI_MATERIALIZATION_DO" });
	return {
		...t,
		put: (hashKey: string, data: object) =>
			t.rpc.apiPutItem(t.ctx, { hashKey: kb(hashKey), sortKey: kb("s"), data: JSON.stringify(data), kind: "json" }),
		remove: (hashKey: string) => t.rpc.apiDeleteItem(t.ctx, { hashKey: kb(hashKey), sortKey: kb("s") }),
	};
}

describe("FokosDBMaterializationDO — GSI targets", () => {
	it("writes the entry of each GSI, moves it when the GSI key changes, and deletes it with the base item", async () => {
		const t = gsiTable();
		const mat = materializationOf(t.ctx, "o1");
		await t.put("o1", { status: "OPEN", at: "t1", owner: "u1" });
		await vi.waitFor(async () => {
			expect(await gsiEntries(t.ctx, "byStatus", "OPEN")).toEqual([
				{ sortKey: "t1", baseHashKey: "o1", baseSortKey: "s", data: { status: "OPEN", at: "t1", owner: "u1" } },
			]);
			expect(await gsiEntries(t.ctx, "byOwner", "u1")).toEqual([{ baseHashKey: "o1", baseSortKey: "s", data: { status: "OPEN" } }]);
		}, TARGET_WAIT);

		await t.put("o1", { status: "CLOSED", at: "t2", owner: "u1" });
		await vi.waitFor(async () => {
			expect(await gsiEntries(t.ctx, "byStatus", "OPEN")).toEqual([]);
			expect(await gsiEntries(t.ctx, "byStatus", "CLOSED")).toMatchObject([{ sortKey: "t2", baseHashKey: "o1" }]);
			expect(await gsiEntries(t.ctx, "byOwner", "u1")).toMatchObject([{ data: { status: "CLOSED" } }]);
		}, TARGET_WAIT);
		expect(await materializationRows(mat)).toMatchObject({ keySets: 2 });

		await t.remove("o1");
		await vi.waitFor(async () => {
			expect(await gsiEntries(t.ctx, "byStatus", "CLOSED")).toEqual([]);
			expect(await gsiEntries(t.ctx, "byOwner", "u1")).toEqual([]);
			// No key set is left, so the cleanup deletes the tombstone.
			expect(await materializationRows(mat)).toEqual({ items: 0, keySets: 0, retries: [] });
		}, TARGET_WAIT);
	});

	it("an item with a missing GSI key attribute has no entry in that GSI", async () => {
		const t = gsiTable();
		const mat = materializationOf(t.ctx, "o2");
		await t.put("o2", { status: "OPEN", at: "t1", owner: "u2" });
		await vi.waitFor(async () => expect(await gsiEntries(t.ctx, "byStatus", "OPEN")).toHaveLength(1), TARGET_WAIT);
		// The sort key attribute goes, so the entry goes too.
		await t.put("o2", { status: "OPEN", owner: "u2" });
		await vi.waitFor(async () => {
			expect(await gsiEntries(t.ctx, "byStatus", "OPEN")).toEqual([]);
			expect(await gsiEntries(t.ctx, "byOwner", "u2")).toHaveLength(1);
		}, TARGET_WAIT);
		expect(await materializationRows(mat)).toMatchObject({ keySets: 1, retries: [] });
	});

	it("a GSI key of a type that is not valid is a violation, and deletes the earlier entry", async () => {
		const t = gsiTable();
		const mat = materializationOf(t.ctx, "o3");
		await t.put("o3", { status: "OPEN", at: "t1", owner: "u3" });
		await vi.waitFor(async () => expect(await gsiEntries(t.ctx, "byStatus", "OPEN")).toHaveLength(1), TARGET_WAIT);
		await t.put("o3", { status: 7, at: "t1", owner: "u3" });
		await vi.waitFor(async () => {
			expect(await gsiEntries(t.ctx, "byStatus", "OPEN")).toEqual([]);
			expect(await materializationRows(mat)).toMatchObject({
				keySets: 1,
				retries: [{ target_id: "byStatus", kind: "violation", last_error: "gsi_key_type_invalid" }],
			});
		}, TARGET_WAIT);
		// A newer valid image applies, and its application deletes the violation row.
		await t.put("o3", { status: "DONE", at: "t3", owner: "u3" });
		await vi.waitFor(async () => {
			expect(await gsiEntries(t.ctx, "byStatus", "DONE")).toHaveLength(1);
			expect(await materializationRows(mat)).toMatchObject({ retries: [] });
		}, TARGET_WAIT);
	});
});
