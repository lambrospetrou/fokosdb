/**
 * The fake snapshot API on partitions of 1 MiB, 100 MiB and 1 GiB, in each backend.
 *
 * A partition gets three items through its API, and then a `filler` table of 64 KiB random blobs
 * until it has the size. The test takes a snapshot, restores it into a DO with a new name, and
 * overwrites the identity of the copy with `kvData`. The restored DO must hold the same rows, and it
 * must serve the items under its new name.
 *
 * `pnpm test` runs 1 MiB in each backend and 100 MiB in the memory backend.
 * `FOKOS_SNAPSHOT_SIZES=all` runs every size in every backend. It takes about 2 minutes and 1.5 GB of memory.
 */
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, it } from "vitest";
import { env } from "cloudflare:workers";
import { FOKOS_KV_KEYS } from "../../src/sharding/sharding-store.js";
import type { FokosPartitionIdentity } from "../../src/sharding/route-context.js";
import { kb, makeStub, openedRpc } from "../partition-do/helpers.js";
import { clearFakeSnapshots, setFakeSnapshotBackend, type FakeSnapshotBackend } from "./fake-snapshots.js";
import type { SnapshotPartitionDO } from "./snapshot-partition-do.js";

// `vitest.config.ts` substitutes this constant into this module when it builds it.
declare const __FOKOS_SNAPSHOT_SIZES__: string;

const MiB = 1024 * 1024;
const FILLER_ROW_BYTES = 64 * 1024;

type Case = { label: string; bytes: number; backend: FakeSnapshotBackend };
const BACKENDS: FakeSnapshotBackend[] = ["memory", "do", "host"];
const SIZES = [
	{ label: "1 MiB", bytes: MiB },
	{ label: "100 MiB", bytes: 100 * MiB },
	{ label: "1 GiB", bytes: 1024 * MiB },
];
const CASES: Case[] = SIZES.flatMap((size) => BACKENDS.map((backend) => ({ ...size, backend }))).filter(
	(c) => __FOKOS_SNAPSHOT_SIZES__ === "all" || c.bytes === MiB || (c.bytes === 100 * MiB && c.backend === "memory"),
);

type Fingerprint = {
	/** For each user table: the row count and a SHA-256 over all its values, in primary-key order. */
	tables: Record<string, { rows: number; sha256: string }>;
	kv: Record<string, unknown>;
	databaseSize: number;
};

function hex(buffer: ArrayBuffer): string {
	return new Uint8Array(buffer).toHex();
}

/**
 * A fingerprint of all the storage of a DO. Each blob is hashed alone, so the test does not keep a
 * second copy of a large DO in memory.
 */
async function fingerprint(stub: DurableObjectStub<SnapshotPartitionDO>): Promise<Fingerprint> {
	return await runInDurableObject(stub, async (_i: SnapshotPartitionDO, state: DurableObjectState) => {
		const sql = state.storage.sql;
		const names = sql
			.exec<{ name: string }>(
				`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name`,
			)
			.toArray()
			.map((r) => r.name);
		const tables: Fingerprint["tables"] = {};
		const encoder = new TextEncoder();
		for (const name of names) {
			const parts: string[] = [];
			let rows = 0;
			for (const row of sql.exec(`SELECT * FROM "${name}"`).raw()) {
				rows++;
				for (const v of row) {
					parts.push(v instanceof ArrayBuffer ? `b:${hex(await crypto.subtle.digest("SHA-256", v))}` : `v:${JSON.stringify(v)}`);
				}
			}
			tables[name] = { rows, sha256: hex(await crypto.subtle.digest("SHA-256", encoder.encode(parts.join("\n")))) };
		}
		return { tables, kv: Object.fromEntries(state.storage.kv.list()), databaseSize: sql.databaseSize };
	});
}

function snapshotStub(name: string): DurableObjectStub<SnapshotPartitionDO> {
	return env.SNAPSHOT_PARTITION_DO.getByName(name);
}

/** Restarts the DO and gives a new stub, because a stub of an aborted DO stays broken. */
async function restart(name: string): Promise<DurableObjectStub<SnapshotPartitionDO>> {
	await snapshotStub(name)
		.debugRestart()
		.catch(() => {});
	return snapshotStub(name);
}

/** Adds 64 KiB random blobs until the database has `bytes`. */
async function fill(stub: DurableObjectStub<SnapshotPartitionDO>, bytes: number): Promise<number> {
	return await runInDurableObject(stub, (_i: SnapshotPartitionDO, state: DurableObjectState) => {
		const sql = state.storage.sql;
		sql.exec(`CREATE TABLE filler (id INTEGER PRIMARY KEY, data BLOB NOT NULL) STRICT`);
		const rows = Math.max(1, Math.ceil((bytes - sql.databaseSize) / FILLER_ROW_BYTES));
		for (let start = 0; start < rows; start += 512) {
			sql.exec(
				`WITH RECURSIVE s(i) AS (SELECT ? UNION ALL SELECT i + 1 FROM s WHERE i + 1 < ?) INSERT INTO filler SELECT i, randomblob(?) FROM s`,
				start,
				Math.min(rows, start + 512),
				FILLER_ROW_BYTES,
			);
		}
		return sql.databaseSize;
	});
}

/**
 * The DOs of the current test. The storage of a test file stays until the file ends, thus each DO is
 * emptied after its test. The restart stops the instance, so that no background work of it reads a
 * deleted table.
 */
const used: string[] = [];

afterEach(async () => {
	for (const name of used.splice(0)) {
		await runInDurableObject(snapshotStub(name), (_i: SnapshotPartitionDO, state: DurableObjectState) => state.storage.deleteAll());
		await restart(name);
	}
	await clearFakeSnapshots();
});

describe("fake Durable Object snapshots of large partitions", () => {
	it.for(CASES)(
		"copies a $label partition in the $backend backend, and kvData overwrites its identity",
		{ timeout: 600_000 },
		async (c, { expect }) => {
			setFakeSnapshotBackend(c.backend);
			// The size cap must be above the size of the test, or the partition refuses the late write.
			const { ctx, rpc } = makeStub({ ns: "SNAPSHOT_PARTITION_DO", hashSplitConditions: { maxSizeMb: 4096 } });
			const targetName = `${ctx.doName}~copy`;
			used.push(ctx.doName, targetName);
			for (const sk of ["a", "b", "c"]) {
				await rpc.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb(sk), data: `value-${sk}`, kind: "text" });
			}
			const source = snapshotStub(ctx.doName);
			expect(await fill(source, c.bytes)).toBeGreaterThanOrEqual(c.bytes);
			const atSnapshot = await fingerprint(source);

			const handle = await source.debugSnapshot();
			// A write after the snapshot must not reach the copy.
			await rpc.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("late"), data: "late", kind: "text" });

			// The copy holds the identity of the source. kvData replaces it with the identity of the
			// target, as a split by snapshot must do.
			const identity = atSnapshot.kv[FOKOS_KV_KEYS.IDENTITY] as FokosPartitionIdentity;
			const targetIdentity: FokosPartitionIdentity = { ...identity, ref: { ...identity.ref, doName: targetName } };
			await snapshotStub(targetName).debugRestoreFrom(handle, {
				[FOKOS_KV_KEYS.IDENTITY]: targetIdentity,
				"__test/restored-from": ctx.doName,
			});
			const target = await restart(targetName);
			const restored = await fingerprint(target);

			expect(restored.tables).toEqual(atSnapshot.tables);
			const { [FOKOS_KV_KEYS.IDENTITY]: restoredIdentity, "__test/restored-from": marker, ...restoredKv } = restored.kv;
			const { [FOKOS_KV_KEYS.IDENTITY]: _sourceIdentity, ...sourceKv } = atSnapshot.kv;
			expect(restoredKv).toEqual(sourceKv);
			expect(restoredIdentity).toEqual(targetIdentity);
			expect(marker).toBe(ctx.doName);
			expect(restored.databaseSize).toBeGreaterThanOrEqual(c.bytes);

			// With the new identity, the copy serves the items of the source under its own name, and it
			// refuses the context of the source.
			const targetCtx = { ...ctx, doName: targetName };
			const targetRpc = openedRpc(target);
			expect(await targetRpc.apiGetItem(targetCtx, { hashKey: kb("hk"), sortKey: kb("b") })).toMatchObject({
				found: true,
				item: { data: "value-b" },
			});
			expect(await targetRpc.apiGetItem(targetCtx, { hashKey: kb("hk"), sortKey: kb("late") })).toMatchObject({ found: false });
			const refused = await targetRpc.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("b") }).then(
				() => null,
				(e: Error) => e.message,
			);
			expect(refused).toMatch(/partition context mismatch/);
		},
	);

	it("without the kvData overwrite, the copy refuses the context of its own name", async ({ expect }) => {
		setFakeSnapshotBackend("memory");
		const { ctx, rpc } = makeStub({ ns: "SNAPSHOT_PARTITION_DO" });
		const targetName = `${ctx.doName}~copy`;
		used.push(ctx.doName, targetName);
		await rpc.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("a"), data: "a", kind: "text" });
		const handle = await snapshotStub(ctx.doName).debugSnapshot();
		await snapshotStub(targetName).debugRestoreFrom(handle);
		const target = openedRpc(await restart(targetName));

		const refused = await target.apiGetItem({ ...ctx, doName: targetName }, { hashKey: kb("hk"), sortKey: kb("a") }).then(
			() => null,
			(e: Error) => e.message,
		);
		expect(refused).toMatch(/partition context mismatch/);
	});
});
