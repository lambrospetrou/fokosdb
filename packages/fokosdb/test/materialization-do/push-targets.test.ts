import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { FokosDBMaterializationDO } from "../../src/server/materialization/do-materialization.js";
import { MaterializationStore } from "../../src/server/materialization/materialization-store.js";
import { isMissingRpcMethod } from "../../src/server/materialization/target-runner.js";
import { validateTargets, type FokosTargetDefinition, type PushSinkTargetDefinition } from "../../src/server/materialization/targets.js";
import type { FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { captureConsoleError, kb, makeStub, materializationOf, TARGET_WAIT } from "../partition-do/helpers.js";
import type { ExpressionValue } from "../../src/shared/expression/types.js";
import type { ReceivedPush } from "./push-target-host.js";

/** The latest record of each key that the push target received from one producer. */
async function latestPushes(producerId: string): Promise<Record<string, ReceivedPush>> {
	const rows: ReceivedPush[] = await env.PUSH_SINK_DO.getByName("sink").received(producerId);
	return Object.fromEntries(rows.map((row) => [row.hashKey, row]));
}

async function materializationState(stub: DurableObjectStub<FokosDBMaterializationDO>) {
	return await runInDurableObject(stub, (_instance: FokosDBMaterializationDO, state: DurableObjectState) => {
		const count = (table: string) => state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table}`).one().n;
		return {
			items: count("latest_items"),
			positions: count("change_positions"),
			targets: state.storage.sql.exec(`SELECT target_id, kind, epoch, is_batch_open FROM materialization_targets`).toArray(),
			storedTargets: state.storage.kv.get("__fokosdb/mat/targets"),
		};
	});
}

describe("FokosDBMaterializationDO — push targets", () => {
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
		// Each call names the base table without the shard group prefix, and the target that sent it.
		const tableName = ctx.topology.shardGroup.slice("fokos.p.".length);
		const senders = await env.PUSH_SINK_DO.getByName("sink").senders(producerId);
		expect(senders.length).toBeGreaterThan(0);
		expect(senders).toEqual(senders.map(() => ({ tableName, targetId: "sink" })));
		expect(await materializationState(mat)).toMatchObject({
			targets: [{ target_id: "sink", kind: "push_sink", epoch: 1, is_batch_open: 0 }],
			storedTargets: [{ kind: "push_sink", targetId: "sink", binding: "PUSH_SINK_DO", method: "push", objectName: "sink" }],
		});
	});

	it("a push target whose binding is not in env is stopped with one log, and the partition keeps each change", async () => {
		const { ctx, rpc } = makeStub({ captureChanges: true });
		const mat = await withStoredTarget(ctx, { binding: "NO_SUCH_BINDING", method: "push" });
		const logs = captureConsoleError();
		const stopped = () => logs.withMessage("fokos/mat: this instance cannot call a push target, and stops it");
		try {
			await rpc.apiPutItem(ctx, { hashKey: kb("a"), sortKey: kb("s"), data: "one", kind: "text" });
			await vi.waitFor(() => expect(stopped()).toHaveLength(1), TARGET_WAIT);
			await rpc.apiPutItem(ctx, { hashKey: kb("b"), sortKey: kb("s"), data: "two", kind: "text" });
			await vi.waitFor(async () => expect(await materializationState(mat)).toMatchObject({ items: 2 }), TARGET_WAIT);
			expect(stopped()).toEqual([expect.objectContaining({ targetId: "sink", binding: "NO_SUCH_BINDING" })]);
			expect(logs.withMessage("fokos/mat: a push call has no answer")).toEqual([]);
			// The target made no claim, so the epoch and the batch marker stay.
			expect(await materializationState(mat)).toMatchObject({ targets: [{ target_id: "sink", epoch: 1, is_batch_open: 0 }] });
		} finally {
			logs.spy.mockRestore();
		}
	});

	it("a push target that does not implement its method fails each call as a whole, and keeps the epoch", async () => {
		const { ctx, rpc } = makeStub({ captureChanges: true });
		const mat = await withStoredTarget(ctx, { binding: "PUSH_SINK_DO", method: "noSuchMethod", objectName: "sink" });
		const logs = captureConsoleError();
		const missing = () => logs.withMessage("fokos/mat: a push target does not implement its method");
		try {
			await rpc.apiPutItem(ctx, { hashKey: kb("a"), sortKey: kb("s"), data: "one", kind: "text" });
			// The breaker opens after the third failure.
			await vi.waitFor(() => expect(missing().length).toBeGreaterThanOrEqual(3), TARGET_WAIT);
			expect(missing()[0]).toMatchObject({ targetId: "sink", binding: "PUSH_SINK_DO", method: "noSuchMethod" });
			expect(logs.withMessage("fokos/mat: a push call has no answer")).toEqual([]);
			expect(await materializationState(mat)).toMatchObject({ items: 1, targets: [{ target_id: "sink", epoch: 1 }] });
		} finally {
			logs.spy.mockRestore();
		}
	});
});

/** Stores one push target as the target list of the materialization root, before its first ingest. */
async function withStoredTarget(
	ctx: FokosDBRouteContext,
	target: Omit<PushSinkTargetDefinition, "kind" | "targetId">,
): Promise<DurableObjectStub<FokosDBMaterializationDO>> {
	const mat = materializationOf(ctx, "a");
	await runInDurableObject(mat, (_instance: FokosDBMaterializationDO, state: DurableObjectState) => {
		new MaterializationStore(state.storage).storeTargets([{ kind: "push_sink", targetId: "sink", ...target }]);
	});
	return mat;
}

describe("isMissingRpcMethod in workerd", () => {
	it("matches the error of a call to a method that the receiver does not implement", async () => {
		const call = async (stub: unknown, method: string) =>
			await (stub as Record<string, () => Promise<unknown>>)[method]().then(
				() => undefined,
				(error: unknown) => error,
			);
		const sink = env.PUSH_SINK_DO.getByName("sink");
		expect(isMissingRpcMethod(await call(sink, "noSuchMethod"))).toBe(true);
		// An instance property is not a method over RPC, and workerd gives another wording.
		expect(isMissingRpcMethod(await call(sink, "ctx"))).toBe(true);
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

	it("rejects a GSI projection value that is only bytes, and accepts one that is bytes for some items only", () => {
		const gsi = (expr: ExpressionValue): FokosTargetDefinition => ({
			kind: "gsi",
			targetId: "g",
			hashKey: { ref: "data", path: "$.a" },
			projection: [{ expr, as: "x" }],
		});
		expect(() => validateTargets([gsi({ b64: "AQID" })])).toThrow(/must not be bytes/);
		expect(() => validateTargets([gsi({ fn: "sqlite.unhex", args: [{ val: "01" }] })])).toThrow(/must not be bytes/);
		expect(() => validateTargets([gsi({ ref: "hashKey" })])).not.toThrow();
		// A byte literal is a valid argument of a value that is not bytes.
		expect(() => validateTargets([gsi({ fn: "size", args: [{ b64: "AQID" }] })])).not.toThrow();
	});

	it("rejects a push projection, which is not supported yet", () => {
		const projected = { ...push("p"), projection: [] } as FokosTargetDefinition;
		expect(failure([projected])).toMatch(/projection/);
	});
});
