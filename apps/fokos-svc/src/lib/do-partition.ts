import { DurableObject } from "cloudflare:workers";
import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import { GetItemOptions, GetItemResult, PutItemOptions, PutItemResult } from "./types.js";
import { PartitionContext } from "./partition-topology.js";

export interface PartitionAPI {
	putItem(ctx: PartitionContext, opts: PutItemOptions): Promise<PutItemResult>;
	getItem(ctx: PartitionContext, opts: GetItemOptions): Promise<GetItemResult>;
}

export class PartitionDO extends DurableObject implements PartitionAPI {
	#_migrations: SQLSchemaMigrations;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#_migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: ctx.storage,
		});
		void ctx.blockConcurrencyWhile(async () => {
			await this.#_migrations.runAll();
		});
	}

	async putItem(ctx: PartitionContext, opts: PutItemOptions): Promise<PutItemResult> {
		this.ctx.storage.sql.exec(
			`INSERT OR REPLACE INTO items (hk, sk, data, ttl_epoch_utc_seconds) VALUES (?, ?, ?, ?)`,
			opts.hashKey,
			opts.sortKey ?? null,
			opts.data,
			opts.ttlEpochUTCSeconds ?? null,
		);
		return {};
	}

	async getItem(ctx: PartitionContext, opts: GetItemOptions): Promise<GetItemResult> {
		const result = this.ctx.storage.sql
			.exec<{
				data: string | ArrayBuffer;
				ttl_epoch_utc_seconds: number | null;
			}>(`SELECT data, ttl_epoch_utc_seconds FROM items WHERE hk = ? AND sk = ?`, opts.hashKey, opts.sortKey ?? null)
			.one();
		if (!result) {
			return { found: false };
		}
		return {
			found: true,
			hashKey: opts.hashKey,
			sortKey: opts.sortKey,
			data: typeof result.data === "string" ? result.data : new Uint8Array(result.data),
			ttlEpochUTCSeconds: result.ttl_epoch_utc_seconds ? Number(result.ttl_epoch_utc_seconds) : undefined,
		};
	}
}

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create items table",
		// The type of `data` is ANY to allow SQLite to retain the input type (e.g. BLOB vs TEXT) and avoid unnecessary conversions.
		// The Durable Object API only accepts strings and Uint8Arrays, so we can safely store them as-is and retrieve them with the correct type.
		sql: `
            CREATE TABLE IF NOT EXISTS items (
                hk TEXT NOT NULL,
                sk TEXT,
                data ANY NOT NULL,
                ttl_epoch_utc_seconds INTEGER,
                PRIMARY KEY (hk, sk)
            ) WITHOUT ROWID, STRICT;`,
	},
];
