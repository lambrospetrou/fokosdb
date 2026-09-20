/**
 * The wire types of the repartition protocol, and the boundary between the flow and its application.
 *
 * The flow moves ownership. It knows about repartitions, targets, slices and route overrides, and it
 * knows nothing about items, locks or any other data the application stores. That data crosses in the
 * `host` migration phase, behind an opaque cursor and an opaque page that the flow passes through
 * without reading, which is what lets a later runtime package keep the flow unchanged while an
 * application defines its own streams.
 */
import type { KeyBytes } from "./key-codec.js";
import type { FokosPartitionRef, FokosRouteContext } from "./route-context.js";
import type { RangeAncestorInfo } from "./types.js";
import type { PromotedKeyCursor, RepartitionKind, RepartitionState, RepartitionTargetRow, TargetInitialization } from "./sharding-store.js";
import type { FokosSlice } from "./repartition-slice.js";

export type { FokosPartitionRef, FokosSlice };

/** A point route key. The sharding layer routes on these two values only. */
export type RouteKey = { hashKey: KeyBytes; sortKey: KeyBytes };

// ─── the source plan ─────────────────────────────────────────────────────────

/**
 * The immutable part of a repartition, written once with the target rows and deleted at cutover.
 *
 * It holds only what the target rows cannot: the source identity and the range ancestors selected for
 * this split. The computed boundaries are the target slices themselves, so the plan does not repeat
 * them, and the mutable split thresholds are never stored — a router rebuilds every forwarded context
 * from its own current context instead.
 */
export type RepartitionPlan = {
	schema: 1;
	source: FokosPartitionRef;
	/** The depth the targets receive. A range split's children, or 0 for a promotion's range root. */
	rangeDepth?: number;
	rangeAncestors?: RangeAncestorInfo[];
};

// ─── the target import record ────────────────────────────────────────────────

/**
 * The target's whole durable import state, under the KV key `__fokos/import`.
 *
 * `imported` is persisted BEFORE the target acknowledges its source, so a crash or a lost reply
 * causes another acknowledgement attempt rather than a silent loss. `active` means the source
 * accepted that acknowledgement.
 *
 * `source` is a reference: the target reaches the source through its own stored route context, so
 * it needs no stored remote context.
 */
export type FokosImportRecord = {
	schema: 2;
	state: FokosImportState;
	repartitionId: string;
	source: FokosPartitionRef;
	slice: FokosSlice;
	cursor: FokosMigrationCursor | null;
	attempts: number;
	nextAttemptAt: number;
	updatedAt: number;
};

export type FokosImportState = "awaiting_data" | "importing" | "imported" | "active";

/**
 * Where an import has got to. A null record cursor means the start of the `overrides` phase, and a
 * null page cursor means both phases are complete.
 *
 * The `overrides` phase moves the route overrides the flow owns. The `host` phase moves the
 * application's data behind `inner`, which the flow never reads: it starts the phase with `inner:
 * null` and repeats until the host answers with a null cursor.
 */
export type FokosMigrationCursor = { phase: "overrides"; inner: PromotedKeyCursor | null } | { phase: "host"; inner: unknown };

// ─── the control RPCs ────────────────────────────────────────────────────────

export type FokosInitRequest = {
	repartitionId: string;
	source: FokosPartitionRef;
	/** The full route context of the target: its identity plus the source's topology, range config and policy. */
	target: FokosRouteContext<unknown>;
	slice: FokosSlice;
	rangeDepth?: number;
	rangeAncestors?: RangeAncestorInfo[];
};

export type FokosStartImportRequest = {
	repartitionId: string;
	source: FokosPartitionRef;
};

export type FokosMigrationPullRequest = {
	repartitionId: string;
	target: FokosPartitionRef;
	cursor: FokosMigrationCursor | null;
};

export type FokosMigrationAckRequest = {
	repartitionId: string;
	target: FokosPartitionRef;
};

/** One bounded page of one phase. A `host` page also holds exactly one of the host's own streams. */
export type FokosMigrationPage =
	| { phase: "overrides"; overrides: { hashKey: KeyBytes }[]; nextCursor: FokosMigrationCursor | null }
	| { phase: "host"; page: unknown; nextCursor: FokosMigrationCursor | null };

/**
 * One read a still-importing target asks its source to serve, for the slice that target owns. The
 * operation name and the request are opaque here: the source knows which operations it serves and
 * narrows both.
 */
export type FokosExecuteLocalRequest = { op: string; repartitionId: string; caller: FokosPartitionRef; request: unknown };

/** Everything one partition calls on another to run a repartition. */
export interface FokosPartitionControlRpc {
	fokosInit(req: FokosInitRequest): Promise<void>;
	fokosStartImport(req: FokosStartImportRequest): Promise<void>;
	fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage>;
	fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void>;
	fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<unknown>;
}

/** The four methods the flow itself calls. It never reads through a peer; the DO owns that path. */
export type FokosRepartitionPeer = Omit<FokosPartitionControlRpc, "fokosExecuteLocal">;

/**
 * The part of the source half that routing reads, and the only part the topology policies see.
 *
 * It is a narrow interface and not the class, so that `split-policy.ts` needs a type-only import. The
 * flow imports `selectRangeAncestors` from there as a value, and a value import back would make a
 * runtime cycle.
 */
export interface RepartitionRouting {
	/** Whether this partition has become a pure router, owning no key of its own. */
	routerRole(): boolean;
	/** The targets of the split in `target_index` order, which a range split tiles in ascending order. */
	splitTargets(): RepartitionTargetRow[];
	/** How far the promotion of one hash key has got, or undefined when this partition still owns it. */
	overrideFor(hashKey: KeyBytes): RepartitionState | undefined;
	/** Whether the range tree, not this partition, owns the key. True from cutover onwards. */
	ownedByRangeTree(hashKey: KeyBytes): boolean;
}

// ─── the application boundary ────────────────────────────────────────────────

/**
 * The application's half of the migration. `buildPage` reads the source; `applyPage` writes the
 * target inside the page transaction, so it must be synchronous and must not await.
 *
 * Both the cursor and the page are opaque to the flow. The host decides how many streams it has and
 * what each page carries; it ends the phase by answering with a null cursor.
 *
 * `belongsToTarget` is the ownership function of the slice, and the host must filter its rows with
 * it. For a `hash_child` slice it returns false for a hash key with a terminal route override, because
 * a range tree owns that key; the child receives the override pointer and no data copy. For a `range`
 * slice it tests the hash key and `[start, end)`. For a `promoted_key` slice it tests the hash key only.
 */
export interface MigrationHost {
	buildPage(cursor: unknown, slice: FokosSlice, belongsToTarget: (key: RouteKey) => boolean): { page: unknown; nextCursor: unknown | null };
	applyPage(page: unknown, slice: FokosSlice): void;
	validatePage(cursor: unknown, page: unknown, nextCursor: unknown | null): void;
}

// ─── the paginated administration view ───────────────────────────────────────

export type FokosStatusCursor = { seq: number; targetIndex: number };

export type FokosStatusEntry = {
	repartition: { id: string; seq: number; kind: RepartitionKind; state: RepartitionState };
	target: null | {
		index: number;
		ref: FokosPartitionRef;
		initialization: TargetInitialization;
		acknowledged: boolean;
	};
};

export type FokosStatusPage = {
	initialized: boolean;
	destroying: boolean;
	/** The identity of this partition, or null before it has one. */
	ref: FokosPartitionRef | null;
	importState: FokosImportState | null;
	entries: FokosStatusEntry[];
	nextCursor: FokosStatusCursor | null;
};

export type FokosStatusRequest = {
	cursor: FokosStatusCursor | null;
	rootContext?: FokosRouteContext<unknown>;
};

export type FokosPrepareDestroyRequest = {
	rootContext?: FokosRouteContext<unknown>;
};

/** What a destroy traversal calls on every partition it reaches, in this order. */
export interface FokosPartitionStatusRpc {
	fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void>;
	fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage>;
	destroyPartition(): Promise<void>;
}
