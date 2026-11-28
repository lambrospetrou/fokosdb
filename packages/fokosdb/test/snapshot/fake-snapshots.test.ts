import { runInDurableObject } from "cloudflare:test";
import { beforeAll, describe, it } from "vitest";
import { env } from "cloudflare:workers";
import { FOKOS_KV_KEYS } from "../../src/sharding/sharding-store.js";
import { kb, makeStub } from "../partition-do/helpers.js";
import {
	clearFakeSnapshots,
	decodeCopy,
	encodeCopy,
	setFakeSnapshotBackend,
	type FakeSnapshotBackend,
	type StorageCopy,
} from "./fake-snapshots.js";
import type { SnapshotPartitionDO } from "./snapshot-partition-do.js";

type Dump = { tables: Record<string, unknown[][]>; kv: Record<string, unknown>; alarm: number | null };

/** All the user tables and KV keys of a DO, in a form that `toEqual` compares. */
async function dump(stub: DurableObjectStub<SnapshotPartitionDO>): Promise<Dump> {
	return await runInDurableObject(stub, async (_i: SnapshotPartitionDO, state: DurableObjectState) => {
		const names = state.storage.sql
			.exec<{ name: string }>(
				`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name`,
			)
			.toArray()
			.map((r) => r.name);
		const tables: Record<string, unknown[][]> = {};
		for (const name of names) {
			tables[name] = [...state.storage.sql.exec(`SELECT * FROM "${name}"`).raw()].map((row) =>
				row.map((v) => (v instanceof ArrayBuffer ? [...new Uint8Array(v)] : v)),
			);
		}
		return { tables, kv: Object.fromEntries(state.storage.kv.list()), alarm: await state.storage.getAlarm() };
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

/** The message of the error of `p`, or null. An RPC rejection that `.rejects` reads is reported as unhandled. */
async function errorOf(p: Promise<unknown>): Promise<string | null> {
	return await p.then(
		() => null,
		(e: Error) => e.message,
	);
}

describe("the text form of a fake snapshot", () => {
	it("keeps each value that JSON alone changes", ({ expect }) => {
		const buffer = new Uint8Array([9, 9, 1, 2, 3, 9]).buffer;
		const copy: StorageCopy = {
			schema: [{ type: "table", name: "t", sql: "CREATE TABLE t (a)" }],
			tables: [{ name: "t", columns: ["a"], rows: [[new Uint8Array([0, 255]).buffer], [Infinity], [-0], [null], ["$ab"]] }],
			kv: [
				// A view over part of a buffer, as the hash arena of the topology is.
				["view", new Uint8Array(buffer, 2, 3)],
				["u32", new Uint32Array([1, 2 ** 32 - 1])],
				["obj", { $ab: "not a tag", nested: [undefined, NaN, 10n], m: new Map([["k", new Set([1])]]) }],
			],
		};
		const back = decodeCopy(encodeCopy(copy));
		expect(back).toEqual(copy);
		// Small pages: one row in each page, and the rows of a table stay in order.
		const pages = [...encodeCopy(copy, 1)];
		expect(pages).toHaveLength(1 + copy.tables[0].rows.length);
		expect(decodeCopy(pages)).toEqual(copy);
		expect(Object.is(back.tables[0].rows[2][0], -0)).toBe(true);
		const view = back.kv[0][1] as Uint8Array;
		expect([...view]).toEqual([1, 2, 3]);
		expect(back.kv[1][1]).toBeInstanceOf(Uint32Array);
	});

	it("refuses a value that it cannot keep", ({ expect }) => {
		const copy: StorageCopy = { schema: [], tables: [], kv: [["e", new Error("x")]] };
		expect(() => [...encodeCopy(copy)]).toThrow(/cannot encode a value of type Error/);
	});
});

describe.each<FakeSnapshotBackend>(["memory", "do", "host"])("fake Durable Object snapshots in the %s backend", (backend) => {
	beforeAll(() => {
		setFakeSnapshotBackend(backend);
	});

	it("copies the storage of a partition into a new DO at the snapshot position", async ({ expect }) => {
		const { ctx, rpc } = makeStub({ ns: "SNAPSHOT_PARTITION_DO" });
		// About 1.6 MB of text, so the `do` backend stores the copy in more than one chunk.
		for (let i = 0; i < 20; i++) {
			const data = new Uint8Array(60_000).fill(i);
			await rpc.apiPutItem(ctx, { hashKey: kb(`hk-${i % 4}`), sortKey: kb(`sk-${i}`), data, kind: "bytes" });
		}
		const source = snapshotStub(ctx.doName);
		const atSnapshot = await dump(source);
		const handle = await source.debugSnapshot();
		expect(handle.backend).toBe(backend);
		// A write after the snapshot must not reach the target.
		await rpc.apiPutItem(ctx, { hashKey: kb("hk-late"), sortKey: kb("sk"), data: "late", kind: "text" });

		const targetName = `snapshot-target.${crypto.randomUUID()}`;
		// The target starts empty. It schedules the restore, and the restore applies at its next start.
		const target = snapshotStub(targetName);
		await target.debugRestoreFrom(handle, { "__test/restored": { from: ctx.doName } });
		expect((await dump(target)).kv["__test/restored"]).toBeUndefined();

		const restored = await dump(await restart(targetName));
		const { ["__test/restored"]: marker, ...restoredKv } = restored.kv;
		expect(marker).toEqual({ from: ctx.doName });
		expect(restored.tables).toEqual(atSnapshot.tables);
		expect(restoredKv).toEqual(atSnapshot.kv);
		expect(await dump(source)).not.toEqual(atSnapshot);
		// The copy holds the identity of the source. A split or a promotion by snapshot must replace it
		// with the identity of the target, for example through kvData.
		expect(restored.kv[FOKOS_KV_KEYS.IDENTITY]).toEqual(atSnapshot.kv[FOKOS_KV_KEYS.IDENTITY]);
	});

	it("replaces the storage of a DO that has data, and clears its alarm", async ({ expect }) => {
		const a = makeStub({ ns: "SNAPSHOT_PARTITION_DO" });
		const b = makeStub({ ns: "SNAPSHOT_PARTITION_DO" });
		await a.rpc.apiPutItem(a.ctx, { hashKey: kb("a"), sortKey: kb("a"), data: "a", kind: "text" });
		await b.rpc.apiPutItem(b.ctx, { hashKey: kb("b"), sortKey: kb("b"), data: "b", kind: "text" });
		await runInDurableObject(snapshotStub(b.ctx.doName), (_i: SnapshotPartitionDO, state: DurableObjectState) =>
			state.storage.setAlarm(Date.now() + 3_600_000),
		);

		const atSnapshot = await dump(snapshotStub(a.ctx.doName));
		const handle = await snapshotStub(a.ctx.doName).debugSnapshot();
		await snapshotStub(b.ctx.doName).debugRestoreFrom(handle);
		const restored = await dump(await restart(b.ctx.doName));

		expect(restored.tables).toEqual(atSnapshot.tables);
		expect(restored.kv).toEqual(atSnapshot.kv);
		expect(restored.alarm).toBeNull();
	});

	it("deletes the copies of the file, and keeps a restore that is already scheduled", async ({ expect }) => {
		const { ctx, rpc } = makeStub({ ns: "SNAPSHOT_PARTITION_DO" });
		await rpc.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });
		const atSnapshot = await dump(snapshotStub(ctx.doName));
		const handle = await snapshotStub(ctx.doName).debugSnapshot();
		const scheduledName = `snapshot-target.${crypto.randomUUID()}`;
		await snapshotStub(scheduledName).debugRestoreFrom(handle);

		// A DO made the copy, and the test context deletes it. Both use one module instance.
		await clearFakeSnapshots();

		const late = snapshotStub(`snapshot-target.${crypto.randomUUID()}`);
		expect(await errorOf(late.debugRestoreFrom(handle))).toMatch(/unknown snapshot handle/);
		const restored = await dump(await restart(scheduledName));
		expect(restored.tables).toEqual(atSnapshot.tables);
	});
});

describe("fake Durable Object snapshots", () => {
	it("refuses kvData over the workerd limits", async ({ expect }) => {
		const { ctx } = makeStub({ ns: "SNAPSHOT_PARTITION_DO" });
		const handle = await snapshotStub(ctx.doName).debugSnapshot();
		const target = snapshotStub(`snapshot-target.${crypto.randomUUID()}`);
		const tooMany = Object.fromEntries(Array.from({ length: 21 }, (_, i) => [`k${i}`, i]));
		expect(await errorOf(target.debugRestoreFrom(handle, tooMany))).toMatch(/at most 20 keys/);
		expect(await errorOf(target.debugRestoreFrom(handle, { big: "x".repeat(4000) }))).toMatch(/at most 3072 bytes/);
	});
});
