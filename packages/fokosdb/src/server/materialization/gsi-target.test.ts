import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBMaterializationDO } from "./do-materialization.js";
import { KeyCodec } from "../../sharding/key-codec.js";
import type { ExpressionValue } from "../../shared/expression/types.js";
import { resolveLimits } from "../../shared/transaction-limits.js";
import { checkGsiPlans, desiredGsiEntry, GSI_SQLITE_UNRECOVERABLE_ERROR } from "./gsi-target.js";
import { MaterializationStore } from "./materialization-store.js";
import { validateTargets, type GsiTargetDefinition } from "./targets.js";

const kb = (value: string) => KeyCodec.encode(value);
const NO_SK = KeyCodec.encodeOptional(undefined);

async function withStore(fn: (store: MaterializationStore, sql: SqlStorage) => void): Promise<void> {
	const stub = env.MATERIALIZATION_DO.getByName(`gsi-target.${crypto.randomUUID()}`);
	await runInDurableObject(stub, (_instance: FokosDBMaterializationDO, state: DurableObjectState) => {
		fn(new MaterializationStore(state.storage), state.storage.sql);
	});
}

describe("checkGsiPlans", () => {
	it("refuses a target whose compiled SQL is above the SQL limit, which validateTargets accepts", async () => {
		// A tree of `+` with 8 levels has 255 operators, below the operator limit. Its SQL repeats each
		// argument, so it is above the SQL limit.
		let hashKey: ExpressionValue = { ref: "data", path: "$.n" };
		for (let i = 0; i < 8; i++) {
			hashKey = { fn: "+", args: [hashKey, hashKey] };
		}
		const target: GsiTargetDefinition = { kind: "gsi", targetId: "g", hashKey };
		expect(() => validateTargets([target])).not.toThrow();
		await withStore((store) => {
			expect(() => checkGsiPlans(store, target)).toThrow(expect.objectContaining({ code: "sql_limit" }));
		});
	});

	it("prepares the statements on no row, so the data of an item cannot make the check fail", async () => {
		const overflow: ExpressionValue = {
			fn: "sqlite.abs",
			args: [{ fn: "*", args: [{ ref: "data", path: "$.n" }, { val: 4_294_967_296 }] }],
		};
		const target: GsiTargetDefinition = {
			kind: "gsi",
			targetId: "g",
			hashKey: { fn: "sqlite.concat", args: [{ val: "key" }, overflow] },
			projection: [{ expr: overflow, as: "value" }],
		};
		await withStore((store, sql) => {
			const data = new Uint8Array(sql.exec<{ b: ArrayBuffer }>(`SELECT jsonb(?) AS b`, `{"n":-2147483648}`).one().b);
			store.putLatestImage(
				{ opIndex: 0, hashKey: kb("bad"), sortKey: NO_SK, revision: 1, deleted: false, data, kind: "json", ttlAt: null },
				1_000,
				undefined,
				[],
			);
			const plans = checkGsiPlans(store, target);
			const image = store.targetImage(store.storedImage(kb("bad"), NO_SK)!.itemId)!;
			expect(desiredGsiEntry(store, plans, image, resolveLimits(undefined))).toEqual({
				kind: "violation",
				code: GSI_SQLITE_UNRECOVERABLE_ERROR,
			});
		});
	});
});
