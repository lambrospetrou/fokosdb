/**
 * The cleanup of `MaterializationDO`: it deletes the change positions that each target finished, and
 * the tombstones that each target read and that no target still needs.
 *
 * A pass has two phases. The first deletes the positions at or below the lowest cursor. The second
 * reads the tombstones in the order of `idx_latest_items_deleted_gc`, and deletes each tombstone
 * when each target read its newest position and no target has a `retry` or `violation` row for it.
 * Each call of `step()` does one bounded chunk in one storage transaction. Any loop can drive it: it
 * holds its own state and reads no state of the caller.
 *
 * The work changes only when the lowest cursor moves or when a row of a target that holds a tombstone
 * goes away. With no target, the work changes when the ingest writes a tombstone. A pass starts only
 * after one of these events: the cleanup compares the lowest cursor with the cursor of its last pass,
 * and a caller reports each other event with `tombstoneUnblocked()`.
 * Thus a target that is down, and that holds the lowest cursor, does not make each pass read all the
 * tombstones again.
 */
import invariant from "../../shared/invariant.js";
import type { MaterializationStore, TombstoneCursor } from "./materialization-store.js";

export type MaterializationCleanupConfig = {
	/** The positions, or the tombstones, that one step deletes. */
	deleteRows: number;
	/** The tombstones that one step reads. */
	scanRows: number;
};

export type MaterializationCleanupDeps = {
	store: MaterializationStore;
	config: () => MaterializationCleanupConfig;
};

type Pass =
	| { phase: "positions"; minCursor: number | null }
	| { phase: "tombstones"; minCursor: number | null; after: TombstoneCursor | null };

export class MaterializationCleanup {
	readonly #deps: MaterializationCleanupDeps;
	/** The pass that runs now, or null. */
	#pass: Pass | null = null;
	/** The lowest cursor of the last pass. A new instance does not know it, so its first pass runs. */
	#lastMinCursor: number | null | undefined = undefined;
	/** A tombstone can be free now with no cursor move. */
	#unblocked = true;

	constructor(deps: MaterializationCleanupDeps) {
		this.#deps = deps;
	}

	/**
	 * Reports that a tombstone can be free with no move of the lowest cursor: a target deleted its row
	 * of the tombstone, or the ingest wrote the tombstone while no target exists. The next pass reads
	 * the tombstones again.
	 */
	tombstoneUnblocked(): void {
		this.#unblocked = true;
	}

	/** The time at which the cleanup has work, or null when it has none. */
	deadline(now: number): number | null {
		return this.#pass !== null || this.#hasNewWork() ? now : null;
	}

	/**
	 * Does one bounded chunk. Returns true when more work can remain, so the caller calls it again.
	 * Returns false when it did nothing.
	 */
	step(): boolean {
		const { store } = this.#deps;
		if (this.#pass === null) {
			if (!this.#hasNewWork()) {
				return false;
			}
			this.#unblocked = false;
			this.#pass = { phase: "positions", minCursor: store.minCursor() };
		}
		const config = this.#deps.config();
		const pass = this.#pass;
		if (pass.phase === "positions") {
			if (pass.minCursor !== null && store.deletePositionsUpTo(pass.minCursor, config.deleteRows) === config.deleteRows) {
				return true;
			}
			this.#pass = { phase: "tombstones", minCursor: pass.minCursor, after: null };
			return true;
		}
		const { done, after } = store.transactionSync(() => this.#tombstoneChunk(pass.minCursor, pass.after, config));
		if (!done) {
			pass.after = after;
			return true;
		}
		this.#lastMinCursor = pass.minCursor;
		this.#pass = null;
		return this.#hasNewWork();
	}

	/**
	 * Deletes the free tombstones of one chunk. With no target, each tombstone is free, because no
	 * target reads it.
	 */
	#tombstoneChunk(
		minCursor: number | null,
		after: TombstoneCursor | null,
		config: MaterializationCleanupConfig,
	): { done: boolean; after: TombstoneCursor | null } {
		const { store } = this.#deps;
		const rows = store.tombstonesAfter(after, config.scanRows);
		let deleted = 0;
		let last = after;
		for (const row of rows) {
			if (deleted >= config.deleteRows) {
				return { done: false, after: last };
			}
			last = row;
			const read = minCursor === null || row.newestSeq === null || row.newestSeq <= minCursor;
			if (read && store.deleteTombstone(row.itemId)) {
				deleted += 1;
			}
		}
		return { done: rows.length < config.scanRows, after: last };
	}

	/** True when a tombstone can be free now, or when the lowest cursor moved since the last pass. */
	#hasNewWork(): boolean {
		return this.#unblocked || this.#deps.store.minCursor() !== this.#lastMinCursor;
	}
}

export function validateMaterializationCleanupConfig(config: MaterializationCleanupConfig): void {
	for (const name of ["deleteRows", "scanRows"] as const) {
		invariant(Number.isInteger(config[name]) && config[name] > 0, `materialization-cleanup: ${name} must be an integer greater than zero`);
	}
}
