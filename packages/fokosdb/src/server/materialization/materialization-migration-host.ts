/**
 * The half of the migration of `MaterializationDO` that the host owns: the streams that carry its rows.
 *
 * The streams run in order, and each drains before the next starts:
 *
 *   1. `items`           — the `latest_items` rows of the slice, each with its change positions and
 *                          its `target_retry` rows.
 *   2. `producer_fences` — the whole `producer_fences` table. The child merges it with MAX, so that it
 *                          rejects each late call that the source rejects.
 *   3. `state`           — one page: the target list, the cursor and the epoch of each target, and the
 *                          ancestor fences of the source.
 *
 * The pages serve only after the cutover, so they hold the final state of the slice. A split source
 * is a router after the cutover, so its cursors do not change. A promotion source continues to send
 * its other keys, and its cursors move past the moved items. Thus the pages of a promotion use the
 * cursor snapshot that the cutover stored.
 */
import { KeyCodec } from "../../sharding/key-codec.js";
import invariant from "../../shared/invariant.js";
import { collectStream, comparePositions, sliceKeyRange, type StreamReader } from "../../shared/partition/fokos-migration-host.js";
import type { HashKeyWalkCursor, ScanCursor } from "../../shared/partition/partition-store.js";
import type { FokosSlice } from "../../sharding/repartition-slice.js";
import type { MigrationHost, RouteKey } from "../../sharding/repartition-types.js";
import type { FokosMigrationPageBudget } from "../../sharding/runtime-config.js";
import type { MaterializationStore, MigratedLatestItem, MigratedState } from "./materialization-store.js";

/** The page size estimate of a position or of a `target_retry` row, beside the item. */
const LINKED_ROW_BYTES = 64;

/** Where the host has got to. The flow stores it as it is and never reads inside it. */
export type MaterializationHostCursor =
	| { stream: "items"; cursor: HashKeyWalkCursor<ScanCursor> | null }
	| { stream: "producer_fences"; after: string | null }
	| { stream: "state" };

export type MaterializationHostPage =
	| { stream: "items"; items: MigratedLatestItem[] }
	| { stream: "producer_fences"; fences: Array<{ producerId: string; epoch: number }> }
	| { stream: "state"; state: MigratedState };

export type MaterializationMigrationHostDeps = {
	store: MaterializationStore;
	/** The `DurableObjectId` of this partition, in hex: the producer ID of its target calls. */
	selfId: () => string;
};

const STREAM_ORDER: Record<MaterializationHostCursor["stream"], number> = { items: 0, producer_fences: 1, state: 2 };

export class MaterializationMigrationHost implements MigrationHost {
	constructor(private readonly deps: MaterializationMigrationHostDeps) {}

	buildPage(
		cursor: unknown,
		slice: FokosSlice,
		belongsToTarget: (key: RouteKey) => boolean,
		budget: FokosMigrationPageBudget,
	): { page: MaterializationHostPage; nextCursor: MaterializationHostCursor | null } {
		const { store } = this.deps;
		const from = asHostCursor(cursor);
		switch (from.stream) {
			case "items": {
				// A position at or below the lowest cursor that the child gets is done for each target.
				const cursors = [...this.#cursors(slice).values()];
				const minSeq = cursors.length === 0 ? Number.POSITIVE_INFINITY : Math.min(...cursors);
				const reader: StreamReader<MigratedLatestItem, ScanCursor> = {
					readRange: (c, limit, range) => store.queryMigrationItems(c, limit, range, minSeq),
					walk: (c, limit, owns) => store.walkMigrationItems(c, limit, owns, minSeq),
					cursorOf: (row) => ({ hk: row.hk, sk: row.sk }),
					estimateBytes: (row) =>
						row.est_row_bytes +
						(row.positions.length + row.retries.length) * LINKED_ROW_BYTES +
						(row.indexKeys ?? []).reduce((n, key) => n + LINKED_ROW_BYTES + key.index_hk.length + key.index_sk.length, 0),
				};
				const { rows, nextCursor } = collectStream(reader, from.cursor, sliceKeyRange(slice), belongsToTarget, budget);
				const next: MaterializationHostCursor = nextCursor
					? { stream: "items", cursor: nextCursor }
					: { stream: "producer_fences", after: null };
				return { page: { stream: "items", items: rows }, nextCursor: next };
			}
			case "producer_fences": {
				const fences = store.producerFencesAfter(from.after, budget.pageRows);
				const next: MaterializationHostCursor =
					fences.length < budget.pageRows
						? { stream: "state" }
						: { stream: "producer_fences", after: fences[fences.length - 1].producerId };
				return { page: { stream: "producer_fences", fences }, nextCursor: next };
			}
			case "state": {
				const state: MigratedState = {
					sourceId: this.deps.selfId(),
					targetList: store.storedTargets(),
					cursors: this.#cursors(slice),
					epochs: new Map(store.targetRows().map((row) => [row.targetId, row.epoch])),
					fences: store.allTargetFences(),
				};
				return { page: { stream: "state", state }, nextCursor: null };
			}
		}
	}

	/**
	 * Applies one page. It runs inside the page transaction of the flow, so each write commits or
	 * rolls back with the cursor of the page.
	 */
	applyPage(page: unknown, _slice: FokosSlice): void {
		const { store } = this.deps;
		const p = page as MaterializationHostPage;
		switch (p.stream) {
			case "items": {
				// An item that is present already contributes nothing, so a retried page does not count
				// its rows two times.
				const added: Array<{ hk: MigratedLatestItem["hk"]; bytes: number }> = [];
				for (const item of p.items) {
					if (store.insertMigratedItem(item)) {
						added.push({ hk: item.hk, bytes: item.est_row_bytes });
					}
				}
				store.keySizes.addRows(added);
				return;
			}
			case "producer_fences":
				for (const fence of p.fences) {
					store.raiseProducerEpoch(fence.producerId, fence.epoch);
				}
				return;
			case "state":
				store.importMigrationState(p.state);
		}
	}

	validatePage(cursor: unknown, page: unknown, nextCursor: unknown, inSlice: (key: RouteKey) => boolean): void {
		const requested = asHostCursor(cursor);
		const p = page as MaterializationHostPage;
		invariant(
			p.stream === requested.stream,
			() => `fokos/mat-migration: asked for the ${requested.stream} stream and received ${p.stream}`,
		);
		if (nextCursor === null) {
			invariant(
				requested.stream === "state",
				() => `fokos/mat-migration: a null cursor ends the ${requested.stream} stream, which is not the last`,
			);
		} else {
			const next = asHostCursor(nextCursor);
			const step = STREAM_ORDER[next.stream] - STREAM_ORDER[requested.stream];
			invariant(step === 0 || step === 1, () => `fokos/mat-migration: the page cursor moves from ${requested.stream} to ${next.stream}`);
			// A cursor that does not move forward makes the child pull the same page again and again.
			invariant(
				step === 1 || movesForward(requested, next),
				() => `fokos/mat-migration: the ${requested.stream} cursor of the page does not move forward`,
			);
		}
		if (p.stream === "items") {
			for (const item of p.items) {
				// The closure is made only on a failure, because a page can hold many rows.
				if (!inSlice({ hashKey: item.hk, sortKey: item.sk })) {
					invariant(false, () => `fokos/mat-migration: the items page holds ${KeyCodec.pairForLog(item.hk, item.sk)}, outside the slice`);
				}
			}
		}
	}

	/**
	 * The cursor of each target that the child gets. A promotion uses the snapshot of its cutover,
	 * because the cursors of the source move past the moved items after the cutover.
	 */
	#cursors(slice: FokosSlice): ReadonlyMap<string, number> {
		const { store } = this.deps;
		if (slice.kind === "promoted_key") {
			const snapshot = store.cursorSnapshot(slice.hashKey);
			invariant(snapshot !== undefined, "fokos/mat-migration: a promotion source has no cursor snapshot");
			return snapshot;
		}
		return new Map(store.targetRows().map((row) => [row.targetId, row.cursorSeq]));
	}
}

/** Reads the opaque cursor of the flow back as the cursor of this host. A null cursor starts the first stream. */
function asHostCursor(cursor: unknown): MaterializationHostCursor {
	if (cursor === null || cursor === undefined) {
		return { stream: "items", cursor: null };
	}
	const c = cursor as MaterializationHostCursor;
	invariant(c.stream in STREAM_ORDER, () => `fokos/mat-migration: unknown stream ${String(c.stream)}`);
	return c;
}

/** True when `next` is after `requested` in the same stream. */
function movesForward(requested: MaterializationHostCursor, next: MaterializationHostCursor): boolean {
	if (requested.stream === "items" && next.stream === "items") {
		return next.cursor !== null && (requested.cursor === null || comparePositions(next.cursor, requested.cursor) > 0);
	}
	if (requested.stream === "producer_fences" && next.stream === "producer_fences") {
		return next.after !== null && (requested.after === null || next.after > requested.after);
	}
	return false;
}
