/**
 * FokosDB's half of the migration: the streams that carry the data a partition stores.
 *
 * The flow moves ownership and knows nothing about items or locks. It hands this host an opaque
 * cursor and passes back an opaque page, so the streams below, their order, and what each page
 * carries are entirely this file's business. A later runtime package keeps that boundary: a
 * different application defines different streams and the flow does not change.
 *
 * The streams run in order and each drains before the next starts:
 *
 *   1. `items`      — the committed rows of the slice.
 *   2. `pending_tx` — the locks of in-flight transactions over the slice, plus the deletion metadata.
 *
 * The two never merge into one page: `items` holds committed state only, and a pending lock is a
 * separate row that commit or cancel resolves later.
 */
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import invariant from "../invariant.js";
import { collectBatch } from "./batch-scan.js";
import {
	estimateItemBytes,
	estimatePendingTxBytes,
	PartitionStore,
	type MigratedItem,
	type PendingTransactionCursor,
	type PendingTransactionRow,
	type ScanCursor,
} from "./partition-store.js";
import { sliceIncludesItem, type FokosSlice } from "./repartition/repartition-slice.js";
import type { MigrationHost } from "./repartition/repartition-types.js";

/** The page budgets of one pull. The source owns them; the request carries no budget. */
const PAGE_BYTES = 20 * 1024 * 1024;
const PAGE_ROWS = 1_000;
const SCAN_ROWS = 10_000;

/** Where the host has got to. The flow stores it verbatim and never reads inside it. */
export type FokosDbHostCursor =
	| { stream: "items"; cursor: ScanCursor | null }
	| { stream: "pending_tx"; cursor: PendingTransactionCursor | null };

export type FokosDbHostPage =
	| { stream: "items"; items: MigratedItem[] }
	| {
			stream: "pending_tx";
			pendingTransactions: PendingTransactionRow[];
			deletionMetadata: { maxDeleteTxOrderTs: number; deleteRevision: number };
	  };

export type FokosMigrationHostDeps = {
	store: PartitionStore;
	/** The source's own `hashSplitN`, which decides which hash child owns a key. */
	hashSplitN: () => number;
};

export class FokosMigrationHost implements MigrationHost {
	constructor(private readonly deps: FokosMigrationHostDeps) {}

	buildPage(cursor: unknown, slice: FokosSlice): { page: FokosDbHostPage; nextCursor: FokosDbHostCursor | null } {
		const from = asHostCursor(cursor);
		return from.stream === "items" ? this.#buildItemsPage(from.cursor, slice) : this.#buildPendingTxPage(from.cursor, slice);
	}

	/**
	 * Applies one page. It runs inside the flow's page transaction, so it is synchronous and every
	 * write it makes commits or rolls back with the cursor that page advanced.
	 */
	applyPage(page: unknown, _slice: FokosSlice): void {
		const p = page as FokosDbHostPage;
		if (p.stream === "items") {
			this.#applyItems(p.items);
			return;
		}
		this.#applyPendingTx(p);
	}

	validatePage(cursor: unknown, page: unknown, nextCursor: unknown | null): void {
		assertHostPageFollowsCursor(cursor, page, nextCursor);
	}

	// ─── items ────────────────────────────────────────────────────────────────

	#buildItemsPage(cursor: ScanCursor | null, slice: FokosSlice): { page: FokosDbHostPage; nextCursor: FokosDbHostCursor | null } {
		const { store, hashSplitN } = this.deps;
		const n = hashSplitN();
		const { rows, nextCursor } = collectBatch<MigratedItem, ScanCursor>({
			fetchPage: (c, pageSize) => store.queryItemsPage(c, pageSize),
			advanceCursor: (row) => ({ hk: row.hk, sk: row.sk }),
			// A terminal override's rows belong to a range tree, not to the hash child that inherits the
			// override. The child receives the forward pointer in the overrides phase and no item copy.
			include: (row) => sliceIncludesItem(slice, row.hk, row.sk, n) && !store.hasTerminalRouteOverride(row.hk),
			estimateBytes: estimateItemBytes,
			budgetBytes: PAGE_BYTES,
			maxItems: PAGE_ROWS,
			maxScannedRows: SCAN_ROWS,
			pageSize: PAGE_ROWS,
			startCursor: cursor,
		});
		// A drained stream hands over to the next one with a fresh cursor. That costs one extra RPC and
		// keeps each page to a single stream.
		const next: FokosDbHostCursor = nextCursor ? { stream: "items", cursor: nextCursor } : { stream: "pending_tx", cursor: null };
		return { page: { stream: "items", items: rows }, nextCursor: next };
	}

	#applyItems(items: readonly MigratedItem[]): void {
		const { store } = this.deps;
		// The exact stored sizes, added per page, are what removes the whole-table estimate rebuild that
		// used to close an import. A row already present contributes nothing, so a retried page cannot
		// count its rows twice.
		// Keyed by the KeyCodec.mapKey hash so no text is built per row. The hash is not an identity:
		// two distinct hash keys can share one value, so a bucket holds every key of one hash and the
		// raw bytes decide which entry a row joins. A bucket has one entry except on a collision.
		const bytesByKey = new Map<bigint, { hk: KeyBytes; bytes: number }[]>();
		for (const item of items) {
			const { inserted, estRowBytes } = store.insertItemIfAbsent(item);
			if (!inserted) continue;
			const id = KeyCodec.mapKey(item.hk);
			const bucket = bytesByKey.get(id);
			if (bucket === undefined) {
				bytesByKey.set(id, [{ hk: item.hk, bytes: estRowBytes }]);
				continue;
			}
			const entry = bucket.find((e) => KeyCodec.compare(e.hk, item.hk) === 0);
			if (entry) {
				entry.bytes += estRowBytes;
			} else {
				bucket.push({ hk: item.hk, bytes: estRowBytes });
			}
		}
		for (const bucket of bytesByKey.values()) {
			for (const { hk, bytes } of bucket) {
				store.addKeySizeEstimate(hk, bytes);
			}
		}
	}

	// ─── pending transactions ─────────────────────────────────────────────────

	#buildPendingTxPage(
		cursor: PendingTransactionCursor | null,
		slice: FokosSlice,
	): { page: FokosDbHostPage; nextCursor: FokosDbHostCursor | null } {
		const { store, hashSplitN } = this.deps;
		const n = hashSplitN();
		const { rows, nextCursor } = collectBatch<PendingTransactionRow, PendingTransactionCursor>({
			fetchPage: (c, pageSize) => store.queryPendingTxPage(c, pageSize),
			advanceCursor: (row) => ({ hk: row.hk, sk: row.sk, transaction_id: row.transaction_id }),
			include: (row) => sliceIncludesItem(slice, row.hk, row.sk, n),
			estimateBytes: estimatePendingTxBytes,
			budgetBytes: PAGE_BYTES,
			maxItems: PAGE_ROWS,
			maxScannedRows: SCAN_ROWS,
			pageSize: PAGE_ROWS,
			startCursor: cursor,
		});
		// Every page of this stream carries the deletion metadata, so a slice with no lock at all still
		// receives it in one empty page. A promoted key never has a lock — promotion cutover requires a
		// zero lock count — and still needs the watermark.
		const page: FokosDbHostPage = { stream: "pending_tx", pendingTransactions: rows, deletionMetadata: store.getDeletionMetadata() };
		return { page, nextCursor: nextCursor ? { stream: "pending_tx", cursor: nextCursor } : null };
	}

	#applyPendingTx(page: Extract<FokosDbHostPage, { stream: "pending_tx" }>): void {
		const { store } = this.deps;
		for (const row of page.pendingTransactions) store.insertPendingLock(row);
		store.mergeDeletionMetadata(page.deletionMetadata);
	}
}

/**
 * Reads the flow's opaque cursor back as this host's own. A null cursor starts the first stream.
 * The flow validates the phase; this validates the stream inside it, so a page can never apply
 * against a cursor from another stream.
 */
function asHostCursor(cursor: unknown): FokosDbHostCursor {
	if (cursor === null || cursor === undefined) return { stream: "items", cursor: null };
	const c = cursor as FokosDbHostCursor;
	invariant(c.stream === "items" || c.stream === "pending_tx", () => `fokos/migration-host: unknown stream ${String(c.stream)}`);
	return c;
}

/** The stream order, so a page can be checked against the cursor that asked for it. */
const STREAM_ORDER: Record<FokosDbHostCursor["stream"], number> = { items: 0, pending_tx: 1 };

/** Throws when a page would move the host's own streams backwards. */
export function assertHostPageFollowsCursor(cursor: unknown, page: unknown, nextCursor: unknown | null): void {
	const requested = asHostCursor(cursor);
	const answered = (page as FokosDbHostPage).stream;
	invariant(
		STREAM_ORDER[answered] === STREAM_ORDER[requested.stream],
		() => `fokos/migration-host: asked for the ${requested.stream} stream and received ${answered}`,
	);
	if (nextCursor === null) {
		invariant(
			requested.stream === "pending_tx",
			() => `fokos/migration-host: a null cursor ends the ${requested.stream} stream, which is not the last`,
		);
		return;
	}
	const next = asHostCursor(nextCursor);
	invariant(
		STREAM_ORDER[next.stream] >= STREAM_ORDER[requested.stream],
		() => `fokos/migration-host: the page cursor moves back from ${requested.stream} to ${next.stream}`,
	);
	invariant(
		STREAM_ORDER[next.stream] <= STREAM_ORDER[requested.stream] + 1,
		() => `fokos/migration-host: the page cursor skips a stream from ${requested.stream} to ${next.stream}`,
	);
}
