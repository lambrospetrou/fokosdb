import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { MaterializationDO } from "../../src/server/materialization/do-materialization.js";
import { validateTargets, type FokosTargetDefinition } from "../../src/server/materialization/targets.js";
import { kb, makeStub, materializationOf, TARGET_WAIT } from "../partition-do/helpers.js";
import type { ReceivedPush } from "./push-target-host.js";

/** The latest record of each key that the push target received from one producer. */
async function latestPushes(producerId: string): Promise<Record<string, ReceivedPush>> {
	const rows: ReceivedPush[] = await env.PUSH_SINK_DO.getByName("sink").received(producerId);
	return Object.fromEntries(rows.map((row) => [row.hashKey, row]));
}

async function materializationState(stub: DurableObjectStub<MaterializationDO>) {
	return await runInDurableObject(stub, (_instance: MaterializationDO, state: DurableObjectState) => {
		const count = (table: string) => state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table}`).one().n;
		return {
			items: count("latest_items"),
			positions: count("change_positions"),
			targets: state.storage.sql.exec(`SELECT target_id, kind, epoch, is_batch_open FROM materialization_targets`).toArray(),
			storedTargets: state.storage.kv.get("__fokosdb/mat/targets"),
		};
	});
}

describe("MaterializationDO — push targets", () => {
	it("sends each base change to the push target, and cleans up the positions and the tombstones", async () => {
		const { ctx, rpc } = makeStub({ captureChanges: true, nsMat: "PUSH_MATERIALIZATION_DO" });
		const mat = materializationOf(ctx, "a");
		const producerId = mat.id.toString();

		await rpc.apiPutItem(ctx, { hashKey: kb("a"), sortKey: kb("s"), data: "one", kind: "text" });
		await rpc.apiPutItem(ctx, { hashKey: kb("a"), sortKey: kb("s"), data: JSON.stringify({ n: 2 }), kind: "json" });
		await vi.waitFor(
			async () =>
				expect((await latestPushes(producerId)).a).toEqual({ hashKey: "a", revision: 2, deleted: false, data: `{"n":2}`, epoch: 1 }),
			TARGET_WAIT,
		);

		await rpc.apiDeleteItem(ctx, { hashKey: kb("a"), sortKey: kb("s") });
		await vi.waitFor(
			async () => expect((await latestPushes(producerId)).a).toEqual({ hashKey: "a", revision: 3, deleted: true, data: null, epoch: 1 }),
			TARGET_WAIT,
		);

		// The target read the tombstone, so the cleanup deletes it and each position.
		await vi.waitFor(async () => expect(await materializationState(mat)).toMatchObject({ items: 0, positions: 0 }), TARGET_WAIT);
		expect(await materializationState(mat)).toMatchObject({
			targets: [{ target_id: "sink", kind: "push_sink", epoch: 1, is_batch_open: 0 }],
			storedTargets: [{ kind: "push_sink", targetId: "sink", binding: "PUSH_SINK_DO", method: "push", objectName: "sink" }],
		});
	});
});

describe("validateTargets", () => {
	const push = (targetId: string): FokosTargetDefinition => ({ kind: "push_sink", targetId, binding: "B", method: "m" });
	/** The detail of the invariant that the validation fails with. */
	const failure = (targets: FokosTargetDefinition[]): string => {
		try {
			validateTargets(targets);
		} catch (error) {
			return String((error as { attributes?: { detail?: unknown } }).attributes?.detail);
		}
		return "no error";
	};

	it("accepts unique push targets and an empty list", () => {
		expect(() => validateTargets([])).not.toThrow();
		expect(() => validateTargets([push("a"), push("b")])).not.toThrow();
	});

	it("rejects an empty, a repeated, or a ~ target id", () => {
		expect(failure([push("")])).toMatch(/targetId/);
		expect(failure([push("a~b")])).toMatch(/targetId/);
		expect(failure([push("a"), push("a")])).toMatch(/not unique/);
	});

	it("accepts a GSI target with valid expressions, and rejects one with an expression that is not valid", () => {
		expect(() => validateTargets([{ kind: "gsi", targetId: "g", hashKey: { ref: "data", path: "$.a" } }])).not.toThrow();
		expect(() => validateTargets([{ kind: "gsi", targetId: "g", hashKey: { ref: "nope" } as never }])).toThrow();
	});

	it("rejects a push projection, which is not supported yet", () => {
		const projected = { ...push("p"), projection: [] } as FokosTargetDefinition;
		expect(failure([projected])).toMatch(/projection/);
	});
});
