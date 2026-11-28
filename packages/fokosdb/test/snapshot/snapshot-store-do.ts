/**
 * The `do` backend of `fake-snapshots.ts`: it keeps the pages of each copy in its SQLite storage.
 * One instance serves all the copies of a test file.
 */
import { DurableObject } from "cloudflare:workers";

/**
 * A row of DO SQLite holds at most 2 MB. A page can be larger when one row of the copy is large,
 * thus the store keeps a page in parts of this many characters. The text is base64 and JSON, so
 * one character is one byte.
 */
const PART_CHARS = 512 * 1024;

export class SnapshotStoreDO extends DurableObject<Env> {
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS pages (
			id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			part INTEGER NOT NULL,
			text TEXT NOT NULL,
			PRIMARY KEY (id, seq, part)
		) WITHOUT ROWID, STRICT`);
		// A copy is complete when it has a row here.
		ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS copies (id TEXT PRIMARY KEY, pages INTEGER NOT NULL) STRICT`);
	}

	async putPage(id: string, seq: number, text: string): Promise<void> {
		this.ctx.storage.sql.exec(`DELETE FROM pages WHERE id = ? AND seq = ?`, id, seq);
		for (let part = 0; part * PART_CHARS < text.length || part === 0; part++) {
			this.ctx.storage.sql.exec(
				`INSERT INTO pages (id, seq, part, text) VALUES (?, ?, ?, ?)`,
				id,
				seq,
				part,
				text.slice(part * PART_CHARS, (part + 1) * PART_CHARS),
			);
		}
	}

	async finish(id: string, pages: number): Promise<void> {
		this.ctx.storage.sql.exec(`INSERT OR REPLACE INTO copies (id, pages) VALUES (?, ?)`, id, pages);
	}

	/** The number of pages of a complete copy, or null when the store has no complete copy. */
	async pageCount(id: string): Promise<number | null> {
		return this.ctx.storage.sql.exec<{ pages: number }>(`SELECT pages FROM copies WHERE id = ?`, id).toArray()[0]?.pages ?? null;
	}

	async getPage(id: string, seq: number): Promise<string> {
		return this.ctx.storage.sql
			.exec<{ text: string }>(`SELECT text FROM pages WHERE id = ? AND seq = ? ORDER BY part`, id, seq)
			.toArray()
			.map((r) => r.text)
			.join("");
	}

	async deleteCopies(ids: string[]): Promise<void> {
		for (const id of ids) {
			this.ctx.storage.sql.exec(`DELETE FROM copies WHERE id = ?`, id);
			this.ctx.storage.sql.exec(`DELETE FROM pages WHERE id = ?`, id);
		}
	}
}
