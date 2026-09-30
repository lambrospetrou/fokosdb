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
 *   2. `pending_tx_info` — the locks of in-flight transactions over the slice, plus the deletion metadata.
 *
 * The two never merge into one page: `items` holds committed state only, and a pending lock is a
 * separate row that commit or cancel resolves later.
 */
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import invariant from "../invariant.js";
import { collectBatch } from "../../sharding/batch-scan.js";
import {
	estimateItemBytes,
	estimatePendingTxBytes,
	PartitionStore,
	type MigratedItem,
	type PendingTransactionCursor,
	type PendingTxItem,
	type ScanCursor,
} from "./partition-store.js";
import type { FokosSlice } from "../../sharding/repartition-slice.js";
import type { MigrationHost, RouteKey } from "../../sharding/repartition-types.js";
import type { FokosMigrationPageBudget } from "../../sharding/runtime-config.js";

type BelongsToTarget = (key: RouteKey) => boolean;

/** Where the host has got to. The flow stores it verbatim and never reads inside it. */
export type FokosDBHostCursor =
	| { stream: "items"; cursor: ScanCursor | null }
	| { stream: "pending_tx"; cursor: PendingTransactionCursor | null };

export type FokosDBHostPage =
	| { stream: "items"; items: MigratedItem[] }
	| {
			stream: "pending_tx";
			pendingTransactions: PendingTxItem[];
			deletionMetadata: { maxDeleteTxOrderTs: number; deleteRevision: number };
	  };

export type FokosMigrationHostDeps = {
	store: PartitionStore;
	/** The log fields of this partition, with its `doName` and `partitionId`, as `TtlExpiry` takes them. */
	logParams: () => Record<string, unknown>;
};

export class FokosMigrationHost implements MigrationHost {
	constructor(private readonly deps: FokosMigrationHostDeps) {}

	/**
	 * `belongsToTarget` is the ownership function of the slice; the flow owns it and every row passes
	 * through it. `budget` holds the page budgets of the source. The request carries no budget.
	 */
	buildPage(
		cursor: unknown,
		_slice: FokosSlice,
		belongsToTarget: BelongsToTarget,
		budget: FokosMigrationPageBudget,
	): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const from = asHostCursor(cursor);
		return from.stream === "items"
			? this.#buildItemsPage(from.cursor, belongsToTarget, budget)
			: this.#buildPendingTxPage(from.cursor, belongsToTarget, budget);
	}

	/**
	 * Applies one page. It runs inside the flow's page transaction, so it is synchronous and every
	 * write it makes commits or rolls back with the cursor that page advanced.
	 */
	applyPage(page: unknown, _slice: FokosSlice): void {
		const p = page as FokosDBHostPage;
		if (p.stream === "items") {
			this.#applyItems(p.items);
			return;
		}
		this.#applyPendingTx(p);
	}

	validatePage(cursor: unknown, page: unknown, nextCursor: unknown): void {
		assertHostPageFollowsCursor(cursor, page, nextCursor);
	}

	// ─── items ────────────────────────────────────────────────────────────────

	#buildItemsPage(
		cursor: ScanCursor | null,
		belongsToTarget: BelongsToTarget,
		budget: FokosMigrationPageBudget,
	): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const { store } = this.deps;
		const { rows, nextCursor } = collectBatch<MigratedItem, ScanCursor>({
			fetchPage: (c, pageSize) => store.queryItemsPage(c, pageSize),
			advanceCursor: (row) => ({ hk: row.hk, sk: row.sk }),
			include: (row) => belongsToTarget({ hashKey: row.hk, sortKey: row.sk }),
			estimateBytes: estimateItemBytes,
			budgetBytes: budget.pageBytes,
			maxItems: budget.pageRows,
			maxScannedRows: budget.scanRows,
			pageSize: budget.pageRows,
			startCursor: cursor,
		});
		// A drained stream hands over to the next one with a fresh cursor. That costs one extra RPC and
		// keeps each page to a single stream.
		const next: FokosDBHostCursor = nextCursor ? { stream: "items", cursor: nextCursor } : { stream: "pending_tx", cursor: null };
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
			if (!inserted) {
				continue;
			}
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
		belongsToTarget: BelongsToTarget,
		budget: FokosMigrationPageBudget,
	): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const { store } = this.deps;
		const { rows, nextCursor } = collectBatch<PendingTxItem, PendingTransactionCursor>({
			fetchPage: (c, pageSize) => store.queryPendingTxPage(c, pageSize),
			advanceCursor: (row) => ({ hk: row.hk, sk: row.sk, transaction_id: row.transaction_id }),
			include: (row) => belongsToTarget({ hashKey: row.hk, sortKey: row.sk }),
			estimateBytes: estimatePendingTxBytes,
			budgetBytes: budget.pageBytes,
			maxItems: budget.pageRows,
			maxScannedRows: budget.scanRows,
			pageSize: budget.pageRows,
			startCursor: cursor,
		});
		// Each page of this stream carries the deletion metadata, so a slice with no lock receives it in
		// one empty page. A promoted key carries the locks that it holds at cutover.
		const page: FokosDBHostPage = { stream: "pending_tx", pendingTransactions: rows, deletionMetadata: store.getDeletionMetadata() };
		return { page, nextCursor: nextCursor ? { stream: "pending_tx", cursor: nextCursor } : null };
	}

	#applyPendingTx(page: Extract<FokosDBHostPage, { stream: "pending_tx" }>): void {
		const { store } = this.deps;
		for (const row of page.pendingTransactions) {
			store.insertPendingLock(row);
		}
		store.mergeDeletionMetadata(page.deletionMetadata);
		this.#logQuarantinedLocks(page.pendingTransactions);
	}

	/**
	 * Logs each quarantined lock that this page brings. The stale scan skips a guarded row, and the
	 * guard writes its line only on the partition where the quarantine starts. This line gives the
	 * operator the partition that owns the lock now. The stream pages in `(hk, sk, transaction_id)`
	 * order, so a transaction that spans pages gets one line for each page.
	 *
	 * The log must not fail the page: a throw rolls the page back on each retry and stops the import.
	 * Thus the method reads the coordinator reference with no validation, and logs text that it cannot
	 * read as it is.
	 */
	#logQuarantinedLocks(rows: readonly PendingTxItem[]): void {
		try {
			const byTransaction = new Map<string, PendingTxItem[]>();
			for (const row of rows) {
				if (row.guarded_at === null) {
					continue;
				}
				const group = byTransaction.get(row.transaction_id);
				if (group) {
					group.push(row);
				} else {
					byTransaction.set(row.transaction_id, [row]);
				}
			}
			if (byTransaction.size === 0) {
				return;
			}
			const logParams = this.deps.logParams();
			for (const [transactionId, group] of byTransaction) {
				console.error({
					...logParams,
					message: "fokos/partition: imported a quarantined lock",
					transactionId,
					...coordinatorFieldsForLog(group[0].coordinator_json),
					keys: group.map((row) => KeyCodec.pairForLog(row.hk, row.sk)),
					lockCreatedAt: Math.min(...group.map((row) => row.created_at)),
					guardedAt: Math.min(...group.map((row) => row.guarded_at ?? Number.POSITIVE_INFINITY)),
				});
			}
		} catch (error) {
			try {
				console.error({ message: "fokos/partition: failed to log an imported quarantined lock", error: String(error) });
			} catch {
				// The log sink failed. The page must apply all the same.
			}
		}
	}
}

/** The coordinator of a lock, for a log line only. It validates nothing, and keeps JSON that it cannot read as raw text. */
function coordinatorFieldsForLog(json: string): { coordinatorDoName: unknown; idempotencyToken: unknown } | { coordinatorRef: string } {
	try {
		const ref = JSON.parse(json) as { doName?: unknown; idempotencyToken?: unknown };
		return { coordinatorDoName: ref.doName, idempotencyToken: ref.idempotencyToken };
	} catch {
		return { coordinatorRef: json };
	}
}

/**
 * Reads the flow's opaque cursor back as this host's own. A null cursor starts the first stream.
 * The flow validates the phase; this validates the stream inside it, so a page can never apply
 * against a cursor from another stream.
 */
function asHostCursor(cursor: unknown): FokosDBHostCursor {
	if (cursor === null || cursor === undefined) {
		return { stream: "items", cursor: null };
	}
	const c = cursor as FokosDBHostCursor;
	invariant(c.stream === "items" || c.stream === "pending_tx", () => `fokos/migration-host: unknown stream ${String(c.stream)}`);
	return c;
}

/** The stream order, so a page can be checked against the cursor that asked for it. */
const STREAM_ORDER: Record<FokosDBHostCursor["stream"], number> = { items: 0, pending_tx: 1 };

/** Throws when a page would move the host's own streams backwards. */
export function assertHostPageFollowsCursor(cursor: unknown, page: unknown, nextCursor: unknown): void {
	const requested = asHostCursor(cursor);
	const answered = (page as FokosDBHostPage).stream;
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
