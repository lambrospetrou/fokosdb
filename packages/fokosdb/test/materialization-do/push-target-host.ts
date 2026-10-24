import { DurableObject } from "cloudflare:workers";
import { MaterializationDO } from "../../src/server/materialization/do-materialization.js";
import type { FokosTargetDefinition, PushTargetBatch, PushTargetResult } from "../../src/server/materialization/targets.js";

/** One item that the push target received. */
export type ReceivedPush = { hashKey: string; revision: number; deleted: boolean; data: string | null; epoch: number };

/**
 * A push target for the tests. It stores each item that it receives, by the producer that sent it,
 * and applies each item, or refuses each item of a producer that `refuse` names. All tests share one
 * instance, so a test reads only its own producer.
 */
export class PushSinkDO extends DurableObject<Env> {
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		ctx.storage.sql.exec(
			`CREATE TABLE IF NOT EXISTS received (producer TEXT NOT NULL, hk TEXT NOT NULL, revision INTEGER NOT NULL, deleted INTEGER NOT NULL, data TEXT, epoch INTEGER NOT NULL)`,
		);
		ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS refused (producer TEXT PRIMARY KEY)`);
		ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS calls (producer TEXT NOT NULL, fences TEXT NOT NULL)`);
	}

	/** Makes each later call of `producerId` fail as a whole, so the cursor of its target stays. */
	refuse(producerId: string): void {
		this.ctx.storage.sql.exec(`INSERT OR IGNORE INTO refused (producer) VALUES (?)`, producerId);
	}

	push(batch: PushTargetBatch): PushTargetResult[] {
		this.ctx.storage.sql.exec(`INSERT INTO calls (producer, fences) VALUES (?, ?)`, batch.producerId, JSON.stringify(batch.ancestorFences));
		if (this.ctx.storage.sql.exec(`SELECT 1 FROM refused WHERE producer = ?`, batch.producerId).toArray().length > 0) {
			return batch.items.map(() => ({ outcome: "retry", code: "refused" }));
		}
		return batch.items.map((item) => {
			const data = item.value === undefined ? null : JSON.stringify(item.value.data);
			this.ctx.storage.sql.exec(
				`INSERT INTO received (producer, hk, revision, deleted, data, epoch) VALUES (?, ?, ?, ?, ?, ?)`,
				batch.producerId,
				String(item.baseHashKey),
				item.revision,
				item.deleted ? 1 : 0,
				data,
				batch.epoch,
			);
			return { outcome: "applied" };
		});
	}

	/** The ancestor fences of each call of `producerId`, in call order. */
	fences(producerId: string): PushTargetBatch["ancestorFences"][] {
		return this.ctx.storage.sql
			.exec<{ fences: string }>(`SELECT fences FROM calls WHERE producer = ? ORDER BY rowid`, producerId)
			.toArray()
			.map((row) => JSON.parse(row.fences) as PushTargetBatch["ancestorFences"]);
	}

	received(producerId: string): ReceivedPush[] {
		return this.ctx.storage.sql
			.exec<{ hk: string; revision: number; deleted: number; data: string | null; epoch: number }>(
				`SELECT hk, revision, deleted, data, epoch FROM received WHERE producer = ? ORDER BY hk, revision`,
				producerId,
			)
			.toArray()
			.map((row) => ({ hashKey: row.hk, revision: row.revision, deleted: row.deleted === 1, data: row.data, epoch: row.epoch }));
	}
}

/** A materialization class with one push target: the `PushSinkDO` named `sink`. */
export class PushMaterializationDO extends MaterializationDO {
	protected override fokosTargets(): readonly FokosTargetDefinition[] {
		return [{ kind: "push_sink", targetId: "sink", binding: "PUSH_SINK_DO", method: "push", objectName: "sink" }];
	}
}
