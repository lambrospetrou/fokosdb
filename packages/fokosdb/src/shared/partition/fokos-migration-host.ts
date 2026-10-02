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
	type HashKeyWalkCursor,
	type HashKeyWalkEntry,
	type KeyRange,
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
	| { stream: "items"; cursor: HashKeyWalkCursor<ScanCursor> | null }
	| { stream: "pending_tx"; cursor: HashKeyWalkCursor<PendingTransactionCursor> | null };

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
	 * The slice limits the read: a promoted key or a range slice reads only the rows of its key range.
	 * A hash child has no key range, so each stream walks its table one hash key at a time.
	 * `belongsToTarget` is the ownership function of the slice, and the flow owns it. A range read
	 * passes each row through it. The hash-key walk passes the first row of each hash key through it.
	 * `budget` holds the page budgets of the source. The request carries no budget.
	 */
	buildPage(
		cursor: unknown,
		slice: FokosSlice,
		belongsToTarget: BelongsToTarget,
		budget: FokosMigrationPageBudget,
	): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const from = asHostCursor(cursor);
		const range = sliceKeyRange(slice);
		return from.stream === "items"
			? this.#buildItemsPage(from.cursor, range, belongsToTarget, budget)
			: this.#buildPendingTxPage(from.cursor, range, belongsToTarget, budget);
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
		cursor: HashKeyWalkCursor<ScanCursor> | null,
		range: KeyRange | null,
		belongsToTarget: BelongsToTarget,
		budget: FokosMigrationPageBudget,
	): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const { store } = this.deps;
		const reader: StreamReader<MigratedItem, ScanCursor> = {
			readRange: (c, limit, r) => store.queryItemsPage(c, limit, r),
			walk: (c, limit, owns) => store.walkItemsByHashKey(c, limit, owns),
			cursorOf: (row) => ({ hk: row.hk, sk: row.sk }),
			estimateBytes: estimateItemBytes,
		};
		const { rows, nextCursor } = collectStream(reader, cursor, range, belongsToTarget, budget);
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
		cursor: HashKeyWalkCursor<PendingTransactionCursor> | null,
		range: KeyRange | null,
		belongsToTarget: BelongsToTarget,
		budget: FokosMigrationPageBudget,
	): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const { store } = this.deps;
		const reader: StreamReader<PendingTxItem, PendingTransactionCursor> = {
			readRange: (c, limit, r) => store.queryPendingTxPage(c, limit, r),
			walk: (c, limit, owns) => store.walkPendingTxByHashKey(c, limit, owns),
			cursorOf: (row) => ({ hk: row.hk, sk: row.sk, transaction_id: row.transaction_id }),
			estimateBytes: estimatePendingTxBytes,
		};
		const { rows, nextCursor } = collectStream(reader, cursor, range, belongsToTarget, budget);
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

/** How the host reads the rows of one stream: as one key range, or as a walk over the hash keys. */
type StreamReader<T, C extends { hk: KeyBytes; sk: KeyBytes }> = {
	readRange: (cursor: C | null, limit: number, range: KeyRange) => Iterable<T>;
	walk: (
		cursor: HashKeyWalkCursor<C> | null,
		limit: number,
		owns: (hk: KeyBytes, firstSk: KeyBytes) => boolean,
	) => Iterable<HashKeyWalkEntry<T>>;
	cursorOf: (row: T) => C;
	estimateBytes: (row: T) => number;
};

/**
 * Collects one page of a stream. With a key range, it reads the range and passes each row through
 * `belongsToTarget`. Without one (a hash child), it walks the hash keys. A hash-child slice owns whole
 * hash keys: the answer of `belongsToTarget` does not depend on the sort key. So the first row of a key
 * decides for each row of the key, and a key that the child does not own costs one seek and one
 * scanned entry.
 */
function collectStream<T extends { hk: KeyBytes; sk: KeyBytes }, C extends { hk: KeyBytes; sk: KeyBytes }>(
	reader: StreamReader<T, C>,
	cursor: HashKeyWalkCursor<C> | null,
	range: KeyRange | null,
	belongsToTarget: BelongsToTarget,
	budget: FokosMigrationPageBudget,
): { rows: T[]; nextCursor: HashKeyWalkCursor<C> | null } {
	const limits = { budgetBytes: budget.pageBytes, maxItems: budget.pageRows, maxScannedRows: budget.scanRows, pageSize: budget.pageRows };
	if (range !== null) {
		let start: C | null = null;
		if (cursor !== null) {
			invariant(cursor.kind === "row", "fokos/migration-host: a range read cannot continue after a skipped hash key");
			start = cursor.row;
		}
		const { rows, nextCursor } = collectBatch<T, C>({
			fetchPage: (c, limit) => reader.readRange(c, limit, range),
			advanceCursor: reader.cursorOf,
			include: (row) => belongsToTarget({ hashKey: row.hk, sortKey: row.sk }),
			estimateBytes: reader.estimateBytes,
			...limits,
			startCursor: start,
		});
		return { rows, nextCursor: nextCursor === null ? null : { kind: "row", row: nextCursor } };
	}
	const owns = (hk: KeyBytes, firstSk: KeyBytes) => belongsToTarget({ hashKey: hk, sortKey: firstSk });
	const { rows, nextCursor } = collectBatch<HashKeyWalkEntry<T>, HashKeyWalkCursor<C>>({
		fetchPage: (c, limit) => reader.walk(c, limit, owns),
		advanceCursor: (entry) =>
			entry.kind === "row" ? { kind: "row", row: reader.cursorOf(entry.row) } : { kind: "after_key", hk: entry.hk },
		include: (entry) => entry.kind === "row",
		estimateBytes: (entry) => (entry.kind === "row" ? reader.estimateBytes(entry.row) : 0),
		...limits,
		startCursor: cursor,
	});
	const page: T[] = [];
	for (const entry of rows) {
		if (entry.kind === "row") {
			page.push(entry.row);
		}
	}
	return { rows: page, nextCursor };
}

/** The key range that holds every row of the slice, or null when the rows of the slice are not one key range. */
function sliceKeyRange(slice: FokosSlice): KeyRange | null {
	switch (slice.kind) {
		case "promoted_key":
			return { hk: slice.hashKey, start: KeyCodec.encodeOptional(undefined), end: null };
		case "range":
			return { hk: slice.hashKey, start: slice.start ?? KeyCodec.encodeOptional(undefined), end: slice.end };
		case "hash_child":
			return null;
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
	assertStreamCursor(c);
	return c;
}

/**
 * Throws when the position inside a stream is not a cursor that this host makes. A key that is not
 * bytes makes the seek `hk > ?` match no row, and the read would then end the stream with no error,
 * and the target would miss the rest of its rows.
 */
function assertStreamCursor({ stream, cursor }: FokosDBHostCursor): void {
	if (cursor === null) {
		return;
	}
	const isKey = (value: unknown) => value instanceof Uint8Array;
	if (cursor.kind === "after_key") {
		invariant(isKey(cursor.hk), () => `fokos/migration-host: the ${stream} cursor has no hash key`);
		return;
	}
	invariant(
		cursor.kind === "row",
		() => `fokos/migration-host: unknown ${stream} cursor kind ${String((cursor as { kind?: unknown }).kind)}`,
	);
	const row: { hk?: unknown; sk?: unknown; transaction_id?: unknown } = cursor.row;
	invariant(isKey(row.hk) && isKey(row.sk), () => `fokos/migration-host: the ${stream} cursor has no hash key or no sort key`);
	invariant(
		stream === "items" || typeof row.transaction_id === "string",
		"fokos/migration-host: the pending_tx cursor has no transaction id",
	);
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
