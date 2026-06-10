import type { InitFromSplitOptions, PartitionContextResolved } from "../partition-topology/partition-context.js";
import type {
	MigratedItem,
	MigrationCursor,
	PendingTransactionCursor,
	PendingTransactionRow,
	PromotedKeyCursor,
	PromotedKeyStatus,
} from "./partition-store.js";

export type GetItemsBatchResult = {
	items: MigratedItem[];
	nextCursor: MigrationCursor | null;
};

export type GetPartitionTransactionMetadataResult = {
	maxDeletedTs: number;
	pendingTransactions: PendingTransactionRow[];
	nextCursor: PendingTransactionCursor | null;
};

export type GetPromotedKeysBatchResult = {
	rows: { hash_key: string; status: PromotedKeyStatus }[];
	nextCursor: PromotedKeyCursor | null;
};

/**
 * The single gateway interface for everything components need from a REMOTE PartitionDO — the
 * narrow RPC surface handed down by the DO (boundary rule: only DO classes and FokosDB acquire
 * stubs; components receive a PartitionPeer and never resolve stubs themselves).
 *
 * A `DurableObjectStub<PartitionDO>` satisfies this structurally — no wrapper class. Components
 * needing only a subset take a `Pick<PartitionPeer, ...>`. Tests pass an in-memory fake.
 *
 * Deliberately NOT unified with the TC's own `PartitionDOStub` type
 * (prepare/commit/cancel/readForTransaction): that is the 2PC surface, a different concern — the
 * TC must not be coupled to partition/ internals.
 */
export interface PartitionPeer {
	getItemsBatch(opts: { childPartitionContext: PartitionContextResolved; cursor: MigrationCursor | null }): Promise<GetItemsBatchResult>;
	getPartitionTransactionMetadata(opts: {
		childPartitionContext: PartitionContextResolved;
		cursor: PendingTransactionCursor | null;
	}): Promise<GetPartitionTransactionMetadataResult>;
	getPromotedKeysBatch(opts: {
		childPartitionContext: PartitionContextResolved;
		cursor: PromotedKeyCursor | null;
	}): Promise<GetPromotedKeysBatchResult>;
	acknowledgeChildMigrationComplete(childDoName: string): Promise<void>;
	acknowledgePromotionComplete(hashKey: string): Promise<void>;
	initFromSplit(opts: InitFromSplitOptions): Promise<void>;
	triggerMigration(): Promise<void>;
}
