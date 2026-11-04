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
 *   3. `producer_fences` — the whole `producer_fences` table. The target merges it with MAX, so that it
 *                     rejects each late call that the source rejects.
 *   4. `outbox`     — the `materialization_outbox` rows of the slice. Only a table that captures its
 *                     changes runs this stream.
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
	type KeyRange,
	type MigratedItem,
	type MigratedOutboxRow,
	type ExportFence,
	type PendingTransactionCursor,
	type PendingTxItem,
	type ProducerFence,
	type StorageCursor,
} from "./partition-store.js";
import type { FokosSlice } from "../../sharding/repartition-slice.js";
import { compareRoutingHashes, type RoutingHash } from "../../sharding/hash-primitives.js";
import type { MigrationHost, RouteKey } from "../../sharding/repartition-types.js";
import type { FokosMigrationPageBudget } from "../../sharding/runtime-config.js";

type BelongsToTarget = (key: RouteKey) => boolean;

/** The page size estimate of an outbox row beside its keys: the revision, the flags, and the framing. */
const OUTBOX_ROW_FIXED_BYTES = 32;

/** Where the host has got to. The flow stores it verbatim and never reads inside it. */
export type FokosDBHostCursor =
	| { stream: "items"; cursor: StorageCursor | null }
	| { stream: "pending_tx"; cursor: PendingTransactionCursor | null }
	| { stream: "producer_fences"; after: string | null }
	| { stream: "outbox"; cursor: StorageCursor | null };

type KeyStreamCursor = Exclude<FokosDBHostCursor, { stream: "producer_fences" }>;

export type FokosDBHostPage =
	| { stream: "items"; items: MigratedItem[] }
	| {
			stream: "pending_tx";
			pendingTransactions: PendingTxItem[];
			deletionMetadata: { maxDeleteTxOrderTs: number; maxDeletedV: number };
	  }
	| { stream: "producer_fences"; fences: ProducerFence[] }
	| {
			stream: "outbox";
			rows: MigratedOutboxRow[];
			/**
			 * The export fences of the target. Only the last page of the stream carries them: the source
			 * at its export epoch for this repartition, and the export fences of the source.
			 */
			fences?: { sourceId: string; epoch: number; inherited: ExportFence[] };
	  };

export type FokosMigrationHostDeps = {
	store: PartitionStore;
	/** The log fields of this partition, with its `doName` and `partitionId`, as `TtlExpiry` takes them. */
	logParams: () => Record<string, unknown>;
	/** True when the table captures its changes. The source then sends the outbox stream after the locks. */
	captureChanges: () => boolean;
	/** The `DurableObjectId` of this partition, in hex: the producer ID of its export calls. */
	selfId: () => string;
	/** The routingHash of a hash key in the shard group of this partition. */
	routingHashForKey: (hashKey: KeyBytes) => RoutingHash;
};

export class FokosMigrationHost implements MigrationHost {
	constructor(private readonly deps: FokosMigrationHostDeps) {}

	/**
	 * Each key stream reads `interval`, the storage interval of the target, in storage order, and
	 * passes each row through `belongsToTarget`, the ownership function of the slice, which the flow
	 * owns. `budget` holds the page budgets of the source. The request carries no budget.
	 */
	buildPage(
		cursor: unknown,
		slice: FokosSlice,
		belongsToTarget: BelongsToTarget,
		budget: FokosMigrationPageBudget,
		interval: KeyRange,
	): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const from = asHostCursor(cursor);
		const read: StreamRead = { interval, include: rowFilter(slice, belongsToTarget), budget };
		switch (from.stream) {
			case "items":
				return this.#buildItemsPage(from.cursor, read);
			case "pending_tx":
				return this.#buildPendingTxPage(from.cursor, read);
			case "producer_fences":
				return this.#buildProducerFencesPage(from.after, budget);
			case "outbox":
				return this.#buildOutboxPage(from.cursor, read);
		}
	}

	/**
	 * Applies one page. It runs inside the flow's page transaction, so it is synchronous and every
	 * write it makes commits or rolls back with the cursor that page advanced.
	 */
	applyPage(page: unknown, _slice: FokosSlice): void {
		const p = page as FokosDBHostPage;
		switch (p.stream) {
			case "items":
				this.#applyItems(p.items);
				return;
			case "pending_tx":
				this.#applyPendingTx(p);
				return;
			case "producer_fences":
				for (const fence of p.fences) {
					this.deps.store.raiseProducerEpoch(fence.producerId, fence.epoch);
				}
				return;
			case "outbox":
				for (const row of p.rows) {
					this.deps.store.insertMigratedOutboxRow(row);
				}
				if (p.fences) {
					this.deps.store.importExportFences(p.fences.sourceId, p.fences.epoch, p.fences.inherited);
				}
		}
	}

	validatePage(cursor: unknown, page: unknown, nextCursor: unknown, inSlice: BelongsToTarget): void {
		assertHostPageFollowsCursor(cursor, page, nextCursor);
		const p = page as FokosDBHostPage;
		if (p.stream === "producer_fences") {
			return;
		}
		for (const row of p.stream === "items" ? p.items : p.stream === "pending_tx" ? p.pendingTransactions : p.rows) {
			// The closure is made only on a failure, because a page can hold many rows.
			if (!inSlice({ hashKey: row.hk, sortKey: row.sk })) {
				invariant(
					false,
					() => `fokos/migration-host: the ${p.stream} page holds ${KeyCodec.pairForLog(row.hk, row.sk)}, outside the slice`,
				);
			}
			// The row is stored under the routingHash it carries, so a wrong routingHash puts it outside the range
			// of its own hash key, where a later read by routingHash cannot find it.
			invariant(
				compareRoutingHashes(row.routingHash, this.deps.routingHashForKey(row.hk)) === 0,
				() =>
					`fokos/migration-host: the ${p.stream} page holds ${KeyCodec.pairForLog(row.hk, row.sk)} with a routingHash that is not the routingHash of the hash key`,
			);
		}
	}

	// ─── items ────────────────────────────────────────────────────────────────

	#buildItemsPage(cursor: StorageCursor | null, read: StreamRead): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const { store } = this.deps;
		const reader: StreamReader<MigratedItem, StorageCursor> = {
			read: (c, limit, interval) => store.queryItemsPage(c, limit, interval),
			cursorOf: (row) => ({ routingHash: row.routingHash, hk: row.hk, sk: row.sk }),
			// A migrated item carries its routingHash on the wire beside the item fields.
			estimateBytes: (row) => row.routingHash.byteLength + estimateItemBytes(row),
		};
		const { rows, nextCursor } = collectStream(reader, cursor, read);
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
		const added: Array<{ routingHash: RoutingHash; hk: KeyBytes; bytes: number }> = [];
		let maxItemId = 0;
		for (const item of items) {
			const { inserted, estRowBytes } = store.insertItemIfAbsent(item);
			if (inserted) {
				added.push({ routingHash: item.routingHash, hk: item.hk, bytes: estRowBytes });
			}
			maxItemId = Math.max(maxItemId, item.item_id);
		}
		store.keySizes.addRows(added);
		if (items.length > 0) {
			store.raiseItemIdMark(maxItemId);
		}
	}

	// ─── pending transactions ─────────────────────────────────────────────────

	#buildPendingTxPage(
		cursor: PendingTransactionCursor | null,
		read: StreamRead,
	): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const { store } = this.deps;
		const reader: StreamReader<PendingTxItem, PendingTransactionCursor> = {
			read: (c, limit, interval) => store.queryPendingTxPage(c, limit, interval),
			cursorOf: (row) => ({ routingHash: row.routingHash, hk: row.hk, sk: row.sk, transaction_id: row.transaction_id }),
			estimateBytes: estimatePendingTxBytes,
		};
		const { rows, nextCursor } = collectStream(reader, cursor, read);
		// Each page of this stream carries the deletion metadata, so a slice with no lock receives it in
		// one empty page. A promoted key carries the locks that it holds at cutover.
		const page: FokosDBHostPage = { stream: "pending_tx", pendingTransactions: rows, deletionMetadata: store.getDeletionMetadata() };
		if (nextCursor) {
			return { page, nextCursor: { stream: "pending_tx", cursor: nextCursor } };
		}
		return { page, nextCursor: { stream: "producer_fences", after: null } };
	}

	// ─── producer fences ──────────────────────────────────────────────────────

	/** Each target gets the whole table: a fence is not a key of the slice. */
	#buildProducerFencesPage(
		after: string | null,
		budget: FokosMigrationPageBudget,
	): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const fences = this.deps.store.producerFencesAfter(after, budget.pageRows);
		const page: FokosDBHostPage = { stream: "producer_fences", fences };
		if (fences.length === budget.pageRows) {
			return { page, nextCursor: { stream: "producer_fences", after: fences[fences.length - 1].producerId } };
		}
		return { page, nextCursor: this.deps.captureChanges() ? { stream: "outbox", cursor: null } : null };
	}

	// ─── materialization outbox ───────────────────────────────────────────────

	/**
	 * The pages of the outbox serve only after the cutover, so they hold the final export state of the
	 * slice. The source raised its export epoch in the cutover transaction (`onRepartitionCutover`), so
	 * each of its export calls that can carry a row of the slice has a lower epoch. The last page gives
	 * the current epoch E to the target. The target sends E with its inherited rows, so each receiver
	 * rejects a late call of the source for those rows.
	 */
	#buildOutboxPage(cursor: StorageCursor | null, read: StreamRead): { page: FokosDBHostPage; nextCursor: FokosDBHostCursor | null } {
		const { store } = this.deps;
		const reader: StreamReader<MigratedOutboxRow, StorageCursor> = {
			read: (c, limit, interval) => store.queryOutboxPage(c, limit, interval),
			cursorOf: (row) => ({ routingHash: row.routingHash, hk: row.hk, sk: row.sk }),
			estimateBytes: (row) => row.routingHash.byteLength + row.hk.byteLength + row.sk.byteLength + OUTBOX_ROW_FIXED_BYTES,
		};
		const { rows, nextCursor } = collectStream(reader, cursor, read);
		if (nextCursor) {
			return { page: { stream: "outbox", rows }, nextCursor: { stream: "outbox", cursor: nextCursor } };
		}
		const fences = { sourceId: this.deps.selfId(), epoch: store.exportState().epoch, inherited: store.exportFences() };
		return { page: { stream: "outbox", rows, fences }, nextCursor: null };
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
	 * operator the partition that owns the lock now. The stream pages in storage order, so a
	 * transaction that spans pages gets one line for each page.
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

/** How the host reads the rows of one stream: one page of the interval, strictly after a cursor. */
export type StreamReader<T, C> = {
	read: (cursor: C | null, limit: number, interval: KeyRange) => Iterable<T>;
	cursorOf: (row: T) => C;
	estimateBytes: (row: T) => number;
};

/** The parts of one page read that each key stream shares: the interval, the row filter, and the budgets. */
type StreamRead = { interval: KeyRange; include: (row: { hk: KeyBytes; sk: KeyBytes }) => boolean; budget: FokosMigrationPageBudget };

/**
 * The row filter of a slice. A hash-child slice owns whole hash keys: the answer of `belongsToTarget`
 * does not depend on the sort key. A read in storage order gives the rows of one hash key one after
 * the other, so the first row of a key decides for each row of the key.
 */
export function rowFilter(slice: FokosSlice, belongsToTarget: BelongsToTarget): (row: { hk: KeyBytes; sk: KeyBytes }) => boolean {
	if (slice.kind !== "hash_child") {
		return (row) => belongsToTarget({ hashKey: row.hk, sortKey: row.sk });
	}
	let lastHk: KeyBytes | null = null;
	let lastAnswer = false;
	return (row) => {
		if (lastHk === null || KeyCodec.compare(lastHk, row.hk) !== 0) {
			lastHk = row.hk;
			lastAnswer = belongsToTarget({ hashKey: row.hk, sortKey: row.sk });
		}
		return lastAnswer;
	};
}

/** Collects one page of a stream: the rows of the interval after `cursor` that `include` accepts. */
export function collectStream<T, C>(reader: StreamReader<T, C>, cursor: C | null, read: StreamRead): { rows: T[]; nextCursor: C | null } {
	const { interval, include, budget } = read;
	return collectBatch<T, C>({
		fetchPage: (c, limit) => reader.read(c, limit, interval),
		advanceCursor: reader.cursorOf,
		include: include as (row: T) => boolean,
		estimateBytes: reader.estimateBytes,
		budgetBytes: budget.pageBytes,
		maxItems: budget.pageRows,
		maxScannedRows: budget.scanRows,
		pageSize: budget.pageRows,
		startCursor: cursor,
	});
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
	invariant(
		c.stream === "items" || c.stream === "pending_tx" || c.stream === "producer_fences" || c.stream === "outbox",
		() => `fokos/migration-host: unknown stream ${String(c.stream)}`,
	);
	assertStreamCursor(c);
	return c;
}

/**
 * Throws when the position inside a stream is not a cursor that this host makes. A key that is not
 * bytes makes the row-value seek match no row, and the read would then end the stream with no error,
 * and the target would miss the rest of its rows.
 */
function assertStreamCursor(c: FokosDBHostCursor): void {
	if (c.stream === "producer_fences") {
		invariant(c.after === null || typeof c.after === "string", "fokos/migration-host: the producer_fences cursor is not a string");
		return;
	}
	const { stream, cursor } = c;
	if (cursor === null) {
		return;
	}
	const row: { routingHash?: unknown; hk?: unknown; sk?: unknown; transaction_id?: unknown } = cursor;
	const isKey = (value: unknown) => value instanceof Uint8Array;
	invariant(
		isKey(row.routingHash) && isKey(row.hk) && isKey(row.sk),
		() => `fokos/migration-host: the ${stream} cursor has no routingHash, hash key, or sort key`,
	);
	invariant(
		stream !== "pending_tx" || typeof row.transaction_id === "string",
		"fokos/migration-host: the pending_tx cursor has no transaction id",
	);
}

/** The stream order, so a page can be checked against the cursor that asked for it. */
const STREAM_ORDER: Record<FokosDBHostCursor["stream"], number> = { items: 0, pending_tx: 1, producer_fences: 2, outbox: 3 };

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
			requested.stream !== "items",
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
	// A cursor that does not move forward makes the target pull the same page again and again.
	invariant(
		next.stream !== requested.stream || movesForward(requested, next),
		() => `fokos/migration-host: the ${requested.stream} cursor of the page does not move forward`,
	);
}

function movesForward(requested: FokosDBHostCursor, next: FokosDBHostCursor): boolean {
	if (requested.stream === "producer_fences" || next.stream === "producer_fences") {
		return (
			requested.stream === "producer_fences" &&
			next.stream === "producer_fences" &&
			next.after !== null &&
			(requested.after === null || next.after > requested.after)
		);
	}
	return next.cursor !== null && (requested.cursor === null || comparePositions(next.cursor, requested.cursor) > 0);
}

export type StreamPosition = NonNullable<KeyStreamCursor["cursor"]>;

/**
 * Compares two positions of one stream in its read order: `(routingHash, hk, sk)` for items, and
 * `(routingHash, hk, sk, transaction_id)` for locks. Each key compares by unsigned bytes, as SQLite
 * compares a BLOB. A transaction id is ASCII, so the string order of JavaScript is the byte order of
 * SQLite.
 */
export function comparePositions(a: StreamPosition, b: StreamPosition): number {
	const byKey =
		KeyCodec.compare(KeyCodec.asKeyBytes(a.routingHash), KeyCodec.asKeyBytes(b.routingHash)) ||
		KeyCodec.compare(a.hk, b.hk) ||
		KeyCodec.compare(a.sk, b.sk);
	if (byKey !== 0) {
		return byKey;
	}
	const tx = (p: StreamPosition) => ("transaction_id" in p ? p.transaction_id : "");
	return tx(a) < tx(b) ? -1 : tx(a) > tx(b) ? 1 : 0;
}
