/**
 * A failed initialization degrades the Durable Object instead of rejecting its construction:
 * `partition_init_failed` answers the storage paths, `fokosStatus` and the destroy pair stay
 * available as the diagnosis and the recovery path, and the alarm re-arms at INIT_RETRY_ALARM_MS so
 * a fixed deployment heals the object on a later run.
 */
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBPartitionDO } from "../../src/server/do-partition.js";
import { FOKOS_KV_KEYS } from "../../src/sharding/sharding-store.js";
import { routingHashSeedOf } from "../../src/sharding/hash-primitives.js";
import type { FokosPartitionIdentity } from "../../src/sharding/route-context.js";
import { INIT_RETRY_ALARM_MS } from "../../src/sharding/scheduler.js";
import { isDestroyAbortError } from "../../src/shared/cf-utils.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { kb } from "./helpers.js";
import { CONTROLLED_NS, makePartition, TestPartition } from "./partition-harness.js";

const ALARM_INFO = { retryCount: 0, isRetry: false, scheduledTime: 0 } satisfies AlarmInvocationInfo;

/** Runs one `alarm(info)` and answers the armed time. The retry must sit at the init interval. */
async function alarmRetryAt(partition: TestPartition): Promise<number | null> {
	return await runInDurableObject(partition.stub, async (instance: FokosDBPartitionDO, state: DurableObjectState) => {
		await instance.alarm({ ...ALARM_INFO, scheduledTime: Date.now() });
		return await state.storage.getAlarm();
	});
}

/** The armed time sits at the fixed init interval, never at the fallback of a job. */
function expectInitRetry(alarmAt: number | null) {
	expect(alarmAt).toBeGreaterThan(Date.now() + INIT_RETRY_ALARM_MS - 60_000);
	expect(alarmAt).toBeLessThanOrEqual(Date.now() + INIT_RETRY_ALARM_MS);
}

/**
 * Breaks the schema so the next construction fails its migrations, then evicts the instance. The
 * writes and the abort run in separate calls: an abort rolls back what its own call wrote, so the
 * poison must land first. A stub of an aborted object stays broken, thus the answer is a new
 * `TestPartition` on the same object.
 */
async function breakAndRestart(partition: TestPartition, breakIt: (state: DurableObjectState) => void): Promise<TestPartition> {
	await runInDurableObject(partition.stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => breakIt(state));
	await runInDurableObject(partition.stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
		state.abort();
	}).catch(() => {});
	return TestPartition.at(partition.ctx);
}

describe("FokosDBPartitionDO — failed initialization", () => {
	it("answers partition_init_failed when the sharding schema is broken, and keeps status and destroy live", async ({ expect }) => {
		const partition = makePartition();
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });

		// The next construction re-runs the sharding migrations, and one needs the column that is gone.
		// The constructor catches the failure instead of rejecting the object.
		const broken = await breakAndRestart(partition, (state) => {
			state.storage.sql.exec(`DROP INDEX idx_fokos_repartitions_seq`);
			state.storage.sql.exec(`ALTER TABLE fokos_repartitions RENAME COLUMN seq TO seq_broken`);
			state.storage.kv.delete(FOKOS_KV_KEYS.SCHEMA_VERSION);
		});

		await runInDurableObject(broken.stub, async (instance: FokosDBPartitionDO, state: DurableObjectState) => {
			await expect(instance.apiGetItem(broken.ctx, { hashKey: kb("hk"), sortKey: kb("sk") })).rejects.toThrow(
				fokosErrorWith("partition_init_failed"),
			);
			// Status stays live: it is the diagnosis path, and it reads KV before it reads a table.
			await expect(instance.fokosStatus({})).resolves.toMatchObject({ initialized: false, destroying: false });

			await instance.alarm({ ...ALARM_INFO, scheduledTime: Date.now() });
			expectInitRetry(await state.storage.getAlarm());

			// The destroy pair is the way out of a poisoned object.
			await instance.fokosPrepareDestroy({});
			await expect(instance.fokosStatus({})).resolves.toMatchObject({ destroying: true });
		});
		// `fokosDestroy` deletes all storage and aborts the instance, so its caller sees the abort
		// sentinel the destroy raises for itself.
		const err = await broken.stub.fokosDestroy().then(
			() => null,
			(e) => e,
		);
		expect(isDestroyAbortError(err)).toBe(true);

		// The wipe cleared the poisoned schema: the next instance migrates clean and serves again.
		const healed = TestPartition.at(partition.ctx);
		await healed.put({ hashKey: kb("hk2"), sortKey: kb("sk"), data: "v2", kind: "text" });
		expect((await healed.get({ hashKey: kb("hk2"), sortKey: kb("sk") })).found).toBeTruthy();
	});

	it("answers partition_init_failed when the host schema is broken, while the runtime keeps answering", async ({ expect }) => {
		const partition = makePartition();
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });

		const broken = await breakAndRestart(partition, (state) => {
			state.storage.sql.exec(`DROP INDEX idx_items_ttl`);
			state.storage.sql.exec(`ALTER TABLE items RENAME COLUMN ttl_epoch_utc_seconds TO ttl_broken`);
			state.storage.kv.delete("__fokosdb/partition/sql_schema_version");
		});

		await runInDurableObject(broken.stub, async (instance: FokosDBPartitionDO, state: DurableObjectState) => {
			await expect(instance.apiGetItem(broken.ctx, { hashKey: kb("hk"), sortKey: kb("sk") })).rejects.toThrow(
				fokosErrorWith("partition_init_failed"),
			);
			// The sharding schema is not the failure: the runtime reports the identity it loaded.
			await expect(instance.fokosStatus({})).resolves.toMatchObject({ initialized: true, destroying: false });
			// The runtime RPCs take the gate of the host error too: a child with a broken host schema must
			// not accept a repartition that it cannot serve.
			await expect(instance.fokosPromotions({ cursor: null })).rejects.toThrow(fokosErrorWith("partition_init_failed"));
			await instance.alarm({ ...ALARM_INFO, scheduledTime: Date.now() });
			expectInitRetry(await state.storage.getAlarm());
		});
		const err = await broken.stub.fokosDestroy().then(
			() => null,
			(e) => e,
		);
		expect(isDestroyAbortError(err)).toBe(true);
	});

	it("refuses all work when the stored routingHash seed differs from the seed of the code, and keeps status and destroy live", async ({
		expect,
	}) => {
		const partition = makePartition();
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });

		const broken = await breakAndRestart(partition, (state) => {
			const identity = state.storage.kv.get<FokosPartitionIdentity>(FOKOS_KV_KEYS.IDENTITY)!;
			expect(identity.routingHashSeed).toBe(routingHashSeedOf(partition.ctx.topology.shardGroup));
			state.storage.kv.put(FOKOS_KV_KEYS.IDENTITY, { ...identity, routingHashSeed: "0000000000000000" });
		});

		await runInDurableObject(broken.stub, async (instance: FokosDBPartitionDO) => {
			await expect(instance.apiGetItem(broken.ctx, { hashKey: kb("hk"), sortKey: kb("sk") })).rejects.toThrow(
				fokosErrorWith("partition_init_failed"),
			);
			await expect(instance.fokosStatus({})).resolves.toMatchObject({ initialized: true, destroying: false });
			await instance.fokosPrepareDestroy({});
		});
		const err = await broken.stub.fokosDestroy().then(
			() => null,
			(e) => e,
		);
		expect(isDestroyAbortError(err)).toBe(true);
	});

	it("degrades on a config override that cannot resolve, and a fresh instance heals", async ({ expect }) => {
		const partition = makePartition({ ns: CONTROLLED_NS });
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });
		await partition.controlled.testConfig({ promotionFraction: 2 });

		await runInDurableObject(partition.stub, async (instance: FokosDBPartitionDO, state: DurableObjectState) => {
			// The next call that resolves the config stores the failure; every call after it answers typed.
			await expect(
				instance.apiPutItem(partition.ctx, { hashKey: kb("hk"), sortKey: kb("sk2"), data: "v", kind: "text" }),
			).rejects.toThrow();
			await expect(instance.apiGetItem(partition.ctx, { hashKey: kb("hk"), sortKey: kb("sk") })).rejects.toThrow(
				fokosErrorWith("partition_init_failed"),
			);
			await expect(instance.fokosStatus({})).resolves.toMatchObject({ initialized: true });
			await instance.alarm({ ...ALARM_INFO, scheduledTime: Date.now() });
			expectInitRetry(await state.storage.getAlarm());
		});

		// A fixed deployment heals the object: the fresh instance resolves the config and serves.
		await runInDurableObject(partition.stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => state.abort()).catch(() => {});
		const fresh = TestPartition.at(partition.ctx);
		await fresh.put({ hashKey: kb("hk2"), sortKey: kb("sk"), data: "v2", kind: "text" });
		expect((await fresh.get({ hashKey: kb("hk2"), sortKey: kb("sk") })).found).toBeTruthy();
	});

	it("degrades on a runtime config override that cannot resolve", async ({ expect }) => {
		const partition = makePartition({ ns: CONTROLLED_NS });
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });
		await partition.controlled.testRuntimeConfig({ nonRetryableRetryMs: 0 });

		// The alarm resolves the config first, so the failure lands in the gate before the API call.
		expectInitRetry(await alarmRetryAt(partition));
		await runInDurableObject(partition.stub, async (instance: FokosDBPartitionDO) => {
			await expect(instance.apiGetItem(partition.ctx, { hashKey: kb("hk"), sortKey: kb("sk") })).rejects.toThrow(
				fokosErrorWith("partition_init_failed"),
			);
			await expect(instance.fokosStatus({})).resolves.toMatchObject({ initialized: true });
		});
	});
});
