import type { PartitionContextResolved } from "./partition-context.js";
import type { SplitStatus, SplitType } from "./types.js";
import invariant from "../invariant.js";

/**
 * The KV-persisted split lifecycle record. Defined here (with the state machine that owns it)
 * and re-exported by split-policy.ts.
 */
export type SplitStatusKVItem =
	| {
			status: Extract<SplitStatus, "split_queued">;
			splitType: SplitType;
			createdAt: number;
			partitionContext: PartitionContextResolved;
	  }
	| {
			status: Extract<SplitStatus, "split_started" | "split_completed">;
			splitType: SplitType;
			createdAt: number;
			partitionContext: PartitionContextResolved;
			childPartitionContexts: PartitionContextResolved[];
			migratedChildDoNames: string[];
			history: Pick<SplitStatusKVItem, "status" | "splitType" | "createdAt" | "partitionContext">[];
	  };

/**
 * The KV-backed split lifecycle state machine (split_queued → split_started → split_completed
 * plus child-migration ack bookkeeping). One shared implementation — previously duplicated
 * ~verbatim between the hash and range topology impls. Both split policies hold an instance;
 * the machine performs no RPC and reads no partition tables, only its own KV key.
 */
export class SplitStateMachine {
	constructor(
		private readonly storage: DurableObjectStorage,
		private readonly kvKey: string,
	) {}

	splitStatus(): SplitStatusKVItem | undefined {
		return this.storage.kv.get<SplitStatusKVItem>(this.kvKey);
	}

	childPartitionContexts(): PartitionContextResolved[] | undefined {
		const splitStatus = this.splitStatus();
		if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
			return splitStatus.childPartitionContexts;
		}
		return undefined;
	}

	/** Idempotent: only writes split_queued when no split record exists yet. */
	queueSplit(splitType: SplitType, partitionContext: PartitionContextResolved): SplitStatusKVItem {
		const nowStatus = this.splitStatus();
		if (!nowStatus) {
			this.storage.kv.put<SplitStatusKVItem>(this.kvKey, {
				status: "split_queued",
				splitType,
				createdAt: Date.now(),
				partitionContext,
			});
		}
		// Alarm scheduling is the caller's responsibility (PartitionDO.checkSplits).
		const written = this.splitStatus();
		invariant(written != null, "fokos/split-state.queueSplit: KV write succeeded but splitStatus() returned null");
		return written;
	}

	/**
	 * Transitions split_queued → split_started with exactly the given children (set once, never
	 * accumulates) and the queued entry pushed onto history. Idempotent: a no-op when already
	 * split_started/split_completed. Callers MUST have completed the child initFromSplit fan-out
	 * before this transition — aborting on init failure before the KV write is what keeps the
	 * retry path safe.
	 */
	commitSplitStarted(children: PartitionContextResolved[]): void {
		const splitStatus = this.splitStatus();
		invariant(splitStatus, "fokos/split-state.commitSplitStarted: splitStatus must exist");
		if (splitStatus.status !== "split_queued") return; // idempotent — a concurrent run won the transition
		this.storage.kv.put<SplitStatusKVItem>(this.kvKey, {
			status: "split_started",
			splitType: splitStatus.splitType,
			createdAt: Date.now(),
			partitionContext: splitStatus.partitionContext,
			childPartitionContexts: children,
			migratedChildDoNames: [],
			history: [
				{
					status: splitStatus.status,
					splitType: splitStatus.splitType,
					createdAt: splitStatus.createdAt,
					partitionContext: splitStatus.partitionContext,
				},
			],
		});
	}

	/**
	 * Records a child's migration-complete ack. Idempotent per child. Transitions to
	 * split_completed once every child has acknowledged.
	 */
	acknowledgeChildMigration(childDoName: string): void {
		const splitStatus = this.splitStatus();
		invariant(splitStatus, "fokos/split-state.acknowledgeChildMigration: splitStatus must exist to acknowledge child migration");
		// Already fully completed — idempotent no-op.
		if (splitStatus.status === "split_completed") return;
		invariant(
			splitStatus.status === "split_started",
			`fokos/split-state.acknowledgeChildMigration: cannot acknowledge child migration in status ${splitStatus.status}`,
		);
		if (splitStatus.migratedChildDoNames.includes(childDoName)) return;

		const migratedChildDoNames = [...splitStatus.migratedChildDoNames, childDoName];
		invariant(
			migratedChildDoNames.length <= splitStatus.childPartitionContexts.length,
			`fokos/split-state.acknowledgeChildMigration: more acks (${migratedChildDoNames.length}) than expected children (${splitStatus.childPartitionContexts.length})`,
		);
		const allMigrated = splitStatus.childPartitionContexts.every((c) => migratedChildDoNames.includes(c.doName));

		const newStatus: SplitStatusKVItem = allMigrated
			? {
					status: "split_completed",
					splitType: splitStatus.splitType,
					createdAt: Date.now(),
					partitionContext: splitStatus.partitionContext,
					childPartitionContexts: splitStatus.childPartitionContexts,
					migratedChildDoNames,
					history: [
						...splitStatus.history,
						{
							status: splitStatus.status,
							splitType: splitStatus.splitType,
							createdAt: splitStatus.createdAt,
							partitionContext: splitStatus.partitionContext,
						},
					],
				}
			: { ...splitStatus, migratedChildDoNames };

		this.storage.kv.put<SplitStatusKVItem>(this.kvKey, newStatus);
	}
}
