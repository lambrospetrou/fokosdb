import { DurableObject } from "cloudflare:workers";
import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import { tryWhile } from "durable-utils/retries";
import type { PartitionContextResolved } from "./partition-topology/partition-context.js";
import { KeyCodec, type KeyBytes } from "./partition-topology/key-codec.js";
import { DATA_KINDS, type DataKind } from "./types.js";
import type {
	InitiateReadRequest,
	InitiateReadResponseEncoded,
	InitiateWriteRequest,
	InitiateWriteResponse,
	ReadForTransactionItemResultEncoded,
	RecoverTransactionResult,
	RejectionReason,
	TCState,
	TransactionItem,
} from "./transaction-types.js";
import { PartitionDO } from "./do-partition.js";

type TcStateRow = {
	idempotency_token: string;
	transaction_id: string;
	state: TCState;
	transaction_ts: number;
	created_at: number;
	rejection_reason_json: string | null;
};

type TcParticipantRow = {
	transaction_id: string;
	partition_do_name: string;
	partition_context_json: string;
	prepare_outcome: string | null;
	commit_outcome: string | null;
	cancel_outcome: string | null;
};

type TcItemRow = {
	transaction_id: string;
	hk: ArrayBuffer;
	sk: ArrayBuffer;
	operation: string;
	data: string | ArrayBuffer | null;
	// Persisted so the reconstructed TransactionItem carries the kind through prepare/commit; for json,
	// `data` is the JSON text (the DO re-encodes to JSONB on commit). NULL for delete/check (no data).
	data_kind: number | null;
	conditions_json: string | null;
	partition_do_name: string;
};

// tc_state.rejection_reason_json must round-trip binary (Uint8Array) keys through JSON, which plain
// JSON.stringify mangles. These tag/restore Uint8Array values so a rejected transaction over binary
// keys still reports the exact key after reload.
function stringifyReason(reason: RejectionReason): string {
	return JSON.stringify(reason, (_k, v) => (v instanceof Uint8Array ? { $u8: Array.from(v) } : v));
}
function parseReason(json: string): RejectionReason {
	return JSON.parse(json, (_k, v) =>
		v && typeof v === "object" && Array.isArray((v as { $u8?: unknown }).$u8) ? new Uint8Array((v as { $u8: number[] }).$u8) : v,
	) as RejectionReason;
}

// tc_items hk/sk are BLOB; materialize a read column as KeyBytes (trusted re-brand, no copy of bytes).
function keyFromBlob(value: ArrayBuffer): KeyBytes {
	return KeyCodec.asKeyBytes(new Uint8Array(value));
}

const STALE_THRESHOLD_MS = 5_000;

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create TC state machine tables",
		sql: `
            CREATE TABLE IF NOT EXISTS tc_state (
                idempotency_token       TEXT    NOT NULL PRIMARY KEY,
                transaction_id          TEXT    NOT NULL,
                state                   TEXT    NOT NULL,
                transaction_ts          INTEGER NOT NULL,
                created_at              INTEGER NOT NULL,
                rejection_reason_json   TEXT
			) WITHOUT ROWID, STRICT;

			CREATE INDEX IF NOT EXISTS tc_state_transaction_id ON tc_state (transaction_id);

            CREATE TABLE IF NOT EXISTS tc_participants (
                transaction_id          TEXT    NOT NULL,
                partition_do_name       TEXT    NOT NULL,
                partition_context_json  TEXT    NOT NULL DEFAULT '',
                prepare_outcome         TEXT,
                commit_outcome          TEXT,
                cancel_outcome          TEXT,
                PRIMARY KEY (transaction_id, partition_do_name)
            ) WITHOUT ROWID, STRICT;

            CREATE TABLE IF NOT EXISTS tc_items (
                transaction_id      TEXT    NOT NULL,
                hk                  BLOB    NOT NULL,
                sk                  BLOB    NOT NULL DEFAULT x'',
                operation           TEXT    NOT NULL,
                data                ANY,
                data_kind           INTEGER,
                conditions_json     TEXT,
                partition_do_name   TEXT    NOT NULL,
                PRIMARY KEY (transaction_id, hk, sk)
            ) WITHOUT ROWID, STRICT;
        `,
	},
];

export class TransactionCoordinatorDO extends DurableObject<Env> {
	#migrations: SQLSchemaMigrations;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: ctx.storage,
		});
		void ctx.blockConcurrencyWhile(async () => {
			await this.#migrations.runAll();
		});
	}

	async initiateWrite(request: InitiateWriteRequest): Promise<InitiateWriteResponse> {
		const transactionId = crypto.randomUUID().replaceAll("-", "");
		const idempotencyToken = request.clientRequestToken ?? transactionId;
		const coordinatorDoId = this.ctx.id.toString();

		const existingRow = this.loadStateRow(idempotencyToken);
		if (existingRow) {
			return await this.resumeTransaction(existingRow, idempotencyToken);
		}

		// Key/operation validation is the client's single boundary (FokosDB.transactWriteItems); the TC
		// receives already-validated, already-encoded operations.

		// TODO: append DO shard suffix for tie-breaking when TC pooling is introduced
		const transactionTs = Date.now();

		// Collect one partitionContext per distinct partition (doName → context).
		const partitionContextByDoName = new Map<string, PartitionContextResolved>();
		for (const op of request.operations) {
			partitionContextByDoName.set(op.partitionContext.doName, op.partitionContext);
		}

		this.ctx.storage.transactionSync(() => {
			this.ctx.storage.sql.exec(
				`INSERT INTO tc_state (idempotency_token, transaction_id, state, transaction_ts, created_at)
                 VALUES (?, ?, 'CREATED', ?, ?)`,
				idempotencyToken,
				transactionId,
				transactionTs,
				Date.now(),
			);
			for (const op of request.operations) {
				this.ctx.storage.sql.exec(
					`INSERT INTO tc_items (transaction_id, hk, sk, operation, data, data_kind, conditions_json, partition_do_name)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
					transactionId,
					op.hashKey,
					op.sortKey,
					op.operation,
					op.data ?? null,
					// data and kind travel together: put carries both; delete/check carry neither (NULL kind).
					op.kind === undefined ? null : DATA_KINDS.indexOf(op.kind),
					op.conditions ? JSON.stringify(op.conditions) : null,
					op.partitionContext.doName,
				);
			}
			for (const [partitionDoName, pCtx] of partitionContextByDoName) {
				this.ctx.storage.sql.exec(
					`INSERT INTO tc_participants (transaction_id, partition_do_name, partition_context_json) VALUES (?, ?, ?)`,
					transactionId,
					partitionDoName,
					JSON.stringify(pCtx),
				);
			}
		});

		if (!(await this.ctx.storage.getAlarm())) {
			await this.ctx.storage.setAlarm(Date.now() + STALE_THRESHOLD_MS);
		}

		return await this.drivePrepare(transactionId, idempotencyToken, coordinatorDoId);
	}

	private async resumeTransaction(existingRow: TcStateRow, idempotencyToken: string): Promise<InitiateWriteResponse> {
		const { transaction_id: transactionId } = existingRow;
		switch (existingRow.state) {
			case "COMMITTED":
				return this.loadFinalResponse(transactionId, idempotencyToken, existingRow);
			case "CANCELLED":
				return this.loadFinalResponse(transactionId, idempotencyToken, existingRow);
			case "PREPARING": {
				await this.runPrepareRecovery(transactionId, idempotencyToken);
				return this.loadFinalResponse(transactionId, idempotencyToken);
			}
			case "PREPARED":
			case "COMMITTING": {
				await this.runCommit(transactionId, idempotencyToken);
				return this.loadFinalResponse(transactionId, idempotencyToken);
			}
			case "CANCELLING": {
				await this.runCancel(transactionId, idempotencyToken);
				return this.loadFinalResponse(transactionId, idempotencyToken);
			}
			case "CREATED": {
				const coordinatorDoId = this.ctx.id.toString();
				return await this.drivePrepare(transactionId, idempotencyToken, coordinatorDoId);
			}
		}
	}

	private loadFinalResponse(transactionId: string, idempotencyToken: string, existingRow?: TcStateRow): InitiateWriteResponse {
		const row = existingRow ?? this.loadStateRow(idempotencyToken)!;
		if (row.state === "COMMITTED") {
			// Decode keys to the public form; the empty sentinel ([]) maps back to an absent sortKey.
			const items = this.loadItems(transactionId).map((i) => {
				const hk = keyFromBlob(i.hk);
				const sk = keyFromBlob(i.sk);
				return sk.length === 0 ? { hashKey: KeyCodec.decode(hk) } : { hashKey: KeyCodec.decode(hk), sortKey: KeyCodec.decode(sk) };
			});
			return { outcome: "committed", transactionId, idempotencyToken, items };
		}
		if (row.state === "CANCELLED" && row.rejection_reason_json) {
			return {
				outcome: "cancelled",
				transactionId,
				idempotencyToken,
				reason: parseReason(row.rejection_reason_json),
			};
		}
		// Some participants didn't respond during recovery; alarm will retry.
		throw new Error(`fokos/tc: transaction ${transactionId} still in progress (state=${row.state}), retry later`);
	}

	private async drivePrepare(transactionId: string, idempotencyToken: string, coordinatorDoId: string): Promise<InitiateWriteResponse> {
		this.ctx.storage.sql.exec(
			`UPDATE tc_state SET state = 'PREPARING' WHERE idempotency_token = ? AND state = 'CREATED'`,
			idempotencyToken,
		);

		const stateRow = this.loadStateRow(idempotencyToken)!;
		const items = this.loadItems(transactionId);
		const participants = this.loadParticipants(transactionId);
		const itemsByPartition = groupByPartition(items);

		const prepareResults = await Promise.allSettled(
			participants.map(async (p) => {
				const pCtx = deserializePartitionContext(p.partition_context_json);
				const partitionItems = itemsByPartition.get(p.partition_do_name) ?? [];
				const result = await tryWhile(
					async () => {
						const r = await PartitionDO.getByName(this.env[pCtx.ns], p.partition_do_name).txPrepare(pCtx, {
							transactionId,
							coordinatorDoId,
							transactionTimestamp: stateRow.transaction_ts,
							items: toTransactionItems(partitionItems),
						});
						this.ctx.storage.sql.exec(
							`UPDATE tc_participants SET prepare_outcome = ? WHERE transaction_id = ? AND partition_do_name = ?`,
							r.outcome,
							transactionId,
							p.partition_do_name,
						);
						return r;
					},
					(_err, nextAttempt) => nextAttempt <= 3,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				);
				return { partitionDoName: p.partition_do_name, result };
			}),
		);

		let firstRejectionReason: RejectionReason | null = null;
		for (const r of prepareResults) {
			if (r.status === "rejected") {
				firstRejectionReason ??= { type: "transient_error" };
			} else if (r.value.result.outcome === "rejected") {
				firstRejectionReason ??= r.value.result.reason;
			}
		}

		if (!firstRejectionReason) {
			// All accepted — PREPARED is the point of no return
			this.ctx.storage.sql.exec(
				`UPDATE tc_state SET state = 'PREPARED' WHERE idempotency_token = ? AND state = 'PREPARING'`,
				idempotencyToken,
			);
			await this.runCommit(transactionId, idempotencyToken).catch((e) =>
				console.error({
					message: "fokos/tc: background commit failed",
					transactionId,
					idempotencyToken,
					error: String(e),
				}),
			);
			return this.loadFinalResponse(transactionId, idempotencyToken);
		}

		this.ctx.storage.sql.exec(
			`UPDATE tc_state SET state = 'CANCELLING', rejection_reason_json = ? WHERE idempotency_token = ? AND state = 'PREPARING'`,
			stringifyReason(firstRejectionReason),
			idempotencyToken,
		);
		await this.runCancel(transactionId, idempotencyToken);
		return this.loadFinalResponse(transactionId, idempotencyToken);
	}

	private async runCommit(transactionId: string, idempotencyToken: string): Promise<void> {
		this.ctx.storage.sql.exec(
			`UPDATE tc_state SET state = 'COMMITTING' WHERE idempotency_token = ? AND state IN ('PREPARED', 'COMMITTING')`,
			idempotencyToken,
		);

		const stateRow = this.loadStateRow(idempotencyToken)!;
		const items = this.loadItems(transactionId);
		const itemsByPartition = groupByPartition(items);

		const pendingParticipants = this.ctx.storage.sql
			.exec<TcParticipantRow>(
				`SELECT transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome
                 FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL`,
				transactionId,
			)
			.toArray();

		await Promise.allSettled(
			pendingParticipants.map(async (p) => {
				const pCtx = deserializePartitionContext(p.partition_context_json);
				const partitionItems = itemsByPartition.get(p.partition_do_name) ?? [];
				await tryWhile(
					async () => {
						await PartitionDO.getByName(this.env[pCtx.ns], p.partition_do_name).txCommit(pCtx, {
							transactionId,
							transactionTimestamp: stateRow.transaction_ts,
							items: toTransactionItems(partitionItems),
						});
						this.ctx.storage.sql.exec(
							`UPDATE tc_participants SET commit_outcome = 'committed' WHERE transaction_id = ? AND partition_do_name = ?`,
							transactionId,
							p.partition_do_name,
						);
					},
					(_err, nextAttempt) => nextAttempt <= 10,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				);
			}),
		);

		// Defensive: only advance to COMMITTED when all participants confirmed.
		const uncommitted =
			this.ctx.storage.sql
				.exec<{ n: number }>(`SELECT COUNT(*) as n FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL`, transactionId)
				.toArray()[0]?.n ?? 0;
		if (uncommitted === 0) {
			this.ctx.storage.sql.exec(
				`UPDATE tc_state SET state = 'COMMITTED' WHERE idempotency_token = ? AND state = 'COMMITTING'`,
				idempotencyToken,
			);
		}
	}

	private async runCancel(transactionId: string, idempotencyToken: string): Promise<void> {
		// Cancel any participant not yet committed and not yet cancelled — this includes both
		// confirmed 'accepted' and NULL-outcome participants that may have silently locked items
		// (e.g., response lost in transit). PartitionDO.cancel is a no-op DELETE, so sending it
		// to a participant that never prepared is safe.
		const pendingParticipants = this.ctx.storage.sql
			.exec<TcParticipantRow>(
				`SELECT transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome
                 FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL AND cancel_outcome IS NULL`,
				transactionId,
			)
			.toArray();

		await Promise.allSettled(
			pendingParticipants.map(async (p) => {
				const pCtx = deserializePartitionContext(p.partition_context_json);
				await tryWhile(
					async () => {
						await PartitionDO.getByName(this.env[pCtx.ns], p.partition_do_name).txCancel(pCtx, { transactionId });
						this.ctx.storage.sql.exec(
							`UPDATE tc_participants SET cancel_outcome = 'cancelled' WHERE transaction_id = ? AND partition_do_name = ?`,
							transactionId,
							p.partition_do_name,
						);
					},
					(_err, nextAttempt) => nextAttempt <= 10,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				);
			}),
		);

		// Only advance to CANCELLED once every eligible participant is confirmed — otherwise leave
		// in CANCELLING so the alarm retries the remaining ones.
		const stillPending =
			this.ctx.storage.sql
				.exec<{
					n: number;
				}>(
					`SELECT COUNT(*) as n FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL AND cancel_outcome IS NULL`,
					transactionId,
				)
				.toArray()[0]?.n ?? 0;
		if (stillPending === 0) {
			this.ctx.storage.sql.exec(
				`UPDATE tc_state SET state = 'CANCELLED' WHERE idempotency_token = ? AND state = 'CANCELLING'`,
				idempotencyToken,
			);
		}
	}

	private async runPrepareRecovery(transactionId: string, idempotencyToken: string): Promise<void> {
		const stateRow = this.loadStateRow(idempotencyToken);
		if (!stateRow) return;

		const items = this.loadItems(transactionId);
		const itemsByPartition = groupByPartition(items);
		const coordinatorDoId = this.ctx.id.toString();

		const nullParticipants = this.ctx.storage.sql
			.exec<TcParticipantRow>(
				`SELECT transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome
                 FROM tc_participants WHERE transaction_id = ? AND prepare_outcome IS NULL`,
				transactionId,
			)
			.toArray();

		let firstNewRejectionReason: RejectionReason | null = null;

		const recoveryResults = await Promise.allSettled(
			nullParticipants.map(async (p) => {
				const pCtx = deserializePartitionContext(p.partition_context_json);
				const partitionItems = itemsByPartition.get(p.partition_do_name) ?? [];
				const result = await tryWhile(
					async () => {
						const r = await PartitionDO.getByName(this.env[pCtx.ns], p.partition_do_name).txPrepare(pCtx, {
							transactionId,
							coordinatorDoId,
							transactionTimestamp: stateRow.transaction_ts,
							items: toTransactionItems(partitionItems),
						});
						this.ctx.storage.sql.exec(
							`UPDATE tc_participants SET prepare_outcome = ? WHERE transaction_id = ? AND partition_do_name = ?`,
							r.outcome,
							transactionId,
							p.partition_do_name,
						);
						return r;
					},
					(_err, nextAttempt) => nextAttempt <= 5,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				);
				if (result.outcome === "rejected") {
					firstNewRejectionReason ??= result.reason;
				}
			}),
		);
		for (const r of recoveryResults) {
			if (r.status === "rejected") firstNewRejectionReason ??= { type: "transient_error" };
		}

		const allParticipants = this.loadParticipants(transactionId);
		const anyRejected = allParticipants.some((p) => p.prepare_outcome === "rejected");
		const allAccepted = allParticipants.every((p) => p.prepare_outcome === "accepted");

		if (allAccepted) {
			this.ctx.storage.sql.exec(
				`UPDATE tc_state SET state = 'PREPARED' WHERE idempotency_token = ? AND state = 'PREPARING'`,
				idempotencyToken,
			);
			await this.runCommit(transactionId, idempotencyToken);
		} else if (anyRejected) {
			const reasonJson = firstNewRejectionReason ? stringifyReason(firstNewRejectionReason) : stringifyReason({ type: "transient_error" });
			this.ctx.storage.sql.exec(
				`UPDATE tc_state SET state = 'CANCELLING', rejection_reason_json = COALESCE(rejection_reason_json, ?)
                 WHERE idempotency_token = ? AND state = 'PREPARING'`,
				reasonJson,
				idempotencyToken,
			);
			await this.runCancel(transactionId, idempotencyToken);
		}
		// If some participants still NULL, leave in PREPARING; alarm will retry
	}

	async initiateRead(request: InitiateReadRequest): Promise<InitiateReadResponseEncoded> {
		const transactionId = crypto.randomUUID().replaceAll("-", "");

		// Group items by partition, keeping the context alongside.
		const partitionMap = new Map<string, { pCtx: PartitionContextResolved; items: InitiateReadRequest["items"] }>();
		for (const item of request.items) {
			const doName = item.partitionContext.doName;
			let entry = partitionMap.get(doName);
			if (!entry) {
				entry = { pCtx: item.partitionContext, items: [] };
				partitionMap.set(doName, entry);
			}
			entry.items.push(item);
		}
		const partitionEntries = [...partitionMap.values()];

		// Phase 1
		const phase1Settled = await Promise.allSettled(
			partitionEntries.map(({ pCtx, items }) =>
				tryWhile(
					async () =>
						await PartitionDO.getByName(this.env[pCtx.ns], pCtx.doName).txReadForTransaction(pCtx, {
							transactionId,
							items: items.map((i) => ({ hashKey: i.hashKey, sortKey: i.sortKey })),
						}),
					(_err, nextAttempt) => nextAttempt <= 5,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				),
			),
		);

		const phase1Flat: ReadForTransactionItemResultEncoded[] = [];
		for (const r of phase1Settled) {
			if (r.status === "rejected") return { outcome: "aborted", reason: "transient_error" };
			phase1Flat.push(...r.value.items);
		}

		if (phase1Flat.some((item) => item.hasPendingWrite)) {
			return { outcome: "aborted", reason: "pending_write" };
		}

		// Phase 2 — verify no concurrent mutations
		const phase2Settled = await Promise.allSettled(
			partitionEntries.map(({ pCtx, items }) =>
				tryWhile(
					async () =>
						await PartitionDO.getByName(this.env[pCtx.ns], pCtx.doName).txReadForTransaction(pCtx, {
							transactionId,
							items: items.map((i) => ({ hashKey: i.hashKey, sortKey: i.sortKey })),
						}),
					(_err, nextAttempt) => nextAttempt <= 5,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				),
			),
		);

		const phase2Flat: ReadForTransactionItemResultEncoded[] = [];
		for (const r of phase2Settled) {
			if (r.status === "rejected") return { outcome: "aborted", reason: "transient_error" };
			phase2Flat.push(...r.value.items);
		}

		if (phase2Flat.some((item) => item.hasPendingWrite)) {
			return { outcome: "aborted", reason: "pending_write" };
		}

		// Key-based comparison guards against any future reordering in PartitionDO. Re-encode the
		// decoded result keys to a collision-proof composite identity for arbitrary key bytes.
		const resultKey = (r: ReadForTransactionItemResultEncoded): string => `${r.hashKey.length}:${r.hashKey}:${r.sortKey ?? ""}`;
		const phase2ByKey = new Map(phase2Flat.map((r) => [resultKey(r), r]));
		for (const p1 of phase1Flat) {
			const p2 = phase2ByKey.get(resultKey(p1));
			if (!p2 || p1.lastCommittedTs !== p2.lastCommittedTs) {
				return { outcome: "aborted", reason: "read_conflict" };
			}
		}

		return { outcome: "committed", items: phase1Flat };
	}

	async alarm(): Promise<void> {
		const rows = this.ctx.storage.sql
			.exec<{
				idempotency_token: string;
				transaction_id: string;
				state: TCState;
				created_at: number;
			}>(
				`SELECT idempotency_token, transaction_id, state, created_at
                 FROM tc_state WHERE state NOT IN ('COMMITTED', 'CANCELLED') LIMIT 100`,
			)
			.toArray();

		for (const row of rows) {
			// FIXME Move this into the SQL above.
			if (Date.now() - row.created_at < STALE_THRESHOLD_MS) continue;
			try {
				const coordinatorDoId = this.ctx.id.toString();
				switch (row.state) {
					case "CREATED":
						await this.drivePrepare(row.transaction_id, row.idempotency_token, coordinatorDoId);
						break;
					case "PREPARING":
						await this.runPrepareRecovery(row.transaction_id, row.idempotency_token);
						break;
					case "PREPARED":
					case "COMMITTING":
						await this.runCommit(row.transaction_id, row.idempotency_token);
						break;
					case "CANCELLING":
						await this.runCancel(row.transaction_id, row.idempotency_token);
						break;
				}
			} catch (e) {
				console.error({
					message: "fokos/tc: alarm recovery failed",
					transactionId: row.transaction_id,
					state: row.state,
					error: String(e),
				});
			}
		}

		const remaining =
			this.ctx.storage.sql
				.exec<{ n: number }>(`SELECT COUNT(*) as n FROM tc_state WHERE state NOT IN ('COMMITTED', 'CANCELLED')`)
				.toArray()[0]?.n ?? 0;
		if (remaining > 0) {
			await this.ctx.storage.setAlarm(Date.now() + STALE_THRESHOLD_MS);
		}
	}

	async recoverTransaction(transactionId: string): Promise<RecoverTransactionResult> {
		const row = this.ctx.storage.sql
			.exec<{
				idempotency_token: string;
				state: TCState;
			}>(`SELECT idempotency_token, state FROM tc_state WHERE transaction_id = ?`, transactionId)
			.toArray()[0];

		if (!row) return { state: "not_found" };
		if (row.state === "COMMITTED" || row.state === "CANCELLED") return { state: row.state };

		try {
			const coordinatorDoId = this.ctx.id.toString();
			switch (row.state) {
				case "CREATED":
					await this.drivePrepare(transactionId, row.idempotency_token, coordinatorDoId);
					break;
				case "PREPARING":
					await this.runPrepareRecovery(transactionId, row.idempotency_token);
					break;
				case "PREPARED":
				case "COMMITTING":
					await this.runCommit(transactionId, row.idempotency_token);
					break;
				case "CANCELLING":
					await this.runCancel(transactionId, row.idempotency_token);
					break;
			}
		} catch (e) {
			console.error({
				message: "fokos/tc: recoverTransaction failed, scheduling alarm",
				transactionId,
				error: String(e),
			});
			if (!(await this.ctx.storage.getAlarm())) {
				await this.ctx.storage.setAlarm(Date.now());
			}
		}
		return { state: "driving" };
	}

	private loadStateRow(idempotencyToken: string): TcStateRow | undefined {
		return this.ctx.storage.sql
			.exec<TcStateRow>(
				`SELECT idempotency_token, transaction_id, state, transaction_ts, created_at, rejection_reason_json
                 FROM tc_state WHERE idempotency_token = ?`,
				idempotencyToken,
			)
			.toArray()[0];
	}

	private loadItems(transactionId: string): TcItemRow[] {
		return this.ctx.storage.sql
			.exec<TcItemRow>(
				`SELECT transaction_id, hk, sk, operation, data, data_kind, conditions_json, partition_do_name
                 FROM tc_items WHERE transaction_id = ?`,
				transactionId,
			)
			.toArray();
	}

	private loadParticipants(transactionId: string): TcParticipantRow[] {
		return this.ctx.storage.sql
			.exec<TcParticipantRow>(
				`SELECT transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome
                 FROM tc_participants WHERE transaction_id = ?`,
				transactionId,
			)
			.toArray();
	}
}

function deserializePartitionContext(json: string): PartitionContextResolved {
	return JSON.parse(json) as PartitionContextResolved;
}

function groupByPartition(items: TcItemRow[]): Map<string, TcItemRow[]> {
	const map = new Map<string, TcItemRow[]>();
	for (const item of items) {
		let arr = map.get(item.partition_do_name);
		if (!arr) {
			arr = [];
			map.set(item.partition_do_name, arr);
		}
		arr.push(item);
	}
	return map;
}

function toTransactionItems(rows: TcItemRow[]): TransactionItem[] {
	return rows.map((row) => ({
		hashKey: keyFromBlob(row.hk),
		sortKey: keyFromBlob(row.sk), // empty KeyBytes ([]) is the absent sentinel
		operation: row.operation as TransactionItem["operation"],
		data: row.data instanceof ArrayBuffer ? new Uint8Array(row.data) : (row.data ?? undefined),
		kind: row.data_kind === null ? undefined : (DATA_KINDS[row.data_kind] as DataKind),
		conditions: row.conditions_json ? JSON.parse(row.conditions_json) : undefined,
	}));
}
