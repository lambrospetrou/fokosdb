import { DeleteItemOptions, GetItemOptions, InitiateReadResponse, InitiateWriteResponse, PutItemOptions } from "./types.js";
import { PartitionDO } from "./do-partition.js";
import { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import {
	PartitionTopologyRouter,
	resolveRangePartitionContext,
	type PartitionContextResolved,
} from "./partition-topology/partition-topology.js";
import type { TCWriteOperation, TCReadItem } from "./transaction-types.js";
import { validateItemKeys, validateTransactWriteOperations } from "./transaction-limits.js";
import type { ItemCondition } from "./types.js";
import { StaticShardedDO } from "durable-utils/do-sharding";
import { tryWhile } from "durable-utils/retries";
import { isErrorRetryable } from "durable-utils/do-utils";

export const DEFAULT_NUM_TRANSACTION_COORDINATORS = 100;

export type FokosDBOptions = {
	ns: DurableObjectNamespace<PartitionDO>;
	topology: PartitionTopologyRouter;
	transactionCoordinatorNs: DurableObjectNamespace<TransactionCoordinatorDO>;

	// TODO Temporary since ideally the transaction coordinators should also auto-scale.
	// This is safe to increase if needed except for retrying the same transaction with the same idempotency token.
	// Data partitions record the actual DO name they should reach out for recovering the transaction.
	numTransactionCoordinators?: number;
};

export class FokosDB {
	#options: Required<FokosDBOptions>;
	#staticShardedTCs: StaticShardedDO<TransactionCoordinatorDO>;

	constructor(options: FokosDBOptions) {
		this.#options = {
			...options,
			numTransactionCoordinators: options.numTransactionCoordinators ?? DEFAULT_NUM_TRANSACTION_COORDINATORS,
		};
		if (!Number.isInteger(this.#options.numTransactionCoordinators) || this.#options.numTransactionCoordinators <= 0) {
			throw new Error("fokosdb: numTransactionCoordinators must be an integer greater or equal to 1");
		}
		this.#staticShardedTCs = new StaticShardedDO(this.#options.transactionCoordinatorNs, {
			numShards: this.#options.numTransactionCoordinators,
			shardGroupName: `${this.#options.topology.partitionContext().databaseName}.tc`,
		});
	}

	options() {
		return { ...this.#options };
	}

	async putItem(opts: PutItemOptions) {
		validateItemKeys(opts.hashKey, opts.sortKey);
		const { doId, partitionContext } = this.#options.topology.pickPartition(opts.hashKey, opts.sortKey);
		const ns = this.#options.ns;
		const stub = ns.get(doId);
		return await stub.putItem(partitionContext, opts);
	}

	async getItem(opts: GetItemOptions) {
		validateItemKeys(opts.hashKey, opts.sortKey);
		const { doId, partitionContext } = this.#options.topology.pickPartition(opts.hashKey, opts.sortKey);
		const ns = this.#options.ns;
		const stub = ns.get(doId);
		return await stub.getItem(partitionContext, opts);
	}

	async deleteItem(opts: DeleteItemOptions) {
		validateItemKeys(opts.hashKey, opts.sortKey);
		const { doId, partitionContext } = this.#options.topology.pickPartition(opts.hashKey, opts.sortKey);
		const ns = this.#options.ns;
		const stub = ns.get(doId);
		return await stub.deleteItem(partitionContext, opts);
	}

	async transactWriteItems(opts: {
		operations: Array<{
			hashKey: string;
			sortKey?: string;
			operation: "put" | "delete" | "check";
			data?: Uint8Array | string;
			conditions?: ItemCondition[];
		}>;
		clientRequestToken?: string;
	}): Promise<InitiateWriteResponse> {
		const operations: TCWriteOperation[] = opts.operations.map((op) => {
			const { partitionContext } = this.#options.topology.pickPartition(op.hashKey, op.sortKey);
			return { ...op, partitionContext };
		});

		validateTransactWriteOperations(operations);

		// TODO: We need to catch DO errors and retry with a different idempotency token to route
		// to a different TC if the chosen one is overloaded or has failed. Tricky to do for writes though...
		const idempotencyToken = opts.clientRequestToken ?? crypto.randomUUID().replaceAll("-", "");
		return await this.#staticShardedTCs.one(idempotencyToken, async (tcStub: DurableObjectStub<TransactionCoordinatorDO>) => {
			return await tcStub.initiateWrite({ clientRequestToken: idempotencyToken, operations });
		});
	}

	async transactGetItems(opts: { items: Array<{ hashKey: string; sortKey?: string }> }): Promise<InitiateReadResponse> {
		const items: TCReadItem[] = opts.items.map((item) => {
			validateItemKeys(item.hashKey, item.sortKey);
			const { partitionContext } = this.#options.topology.pickPartition(item.hashKey, item.sortKey);
			return { ...item, partitionContext };
		});

		return await tryWhile(
			async () => {
				// Read-only TCs are ephemeral — random UUID, no client idempotency token needed.
				// Even better using a different shard key each time to maximize chances of hitting different TCs if there is an overloaded one.
				return await this.#staticShardedTCs.one(crypto.randomUUID(), async (tcStub: DurableObjectStub<TransactionCoordinatorDO>) => {
					return await tcStub.initiateRead({ items });
				});
			},
			(err: unknown, nextAttempt: number) => isErrorRetryable(err) && nextAttempt <= 3,
		);
	}

	async destroy(): Promise<{ ok: true }> {
		const ns = this.#options.ns;

		// Dedupe range structures: a 'promoted' entry is inherited by every hash child that took ownership,
		// so the same global rangeRoot(hashKey) may be enumerated from multiple hash partitions.
		const destroyedRangeRoots = new Set<string>();

		// TODO Move the traversal logic in the topology router.
		const destroyPartition = async (ctx: PartitionContextResolved): Promise<void> => {
			const stub = ns.get(ns.idFromName(ctx.doName));
			console.warn(`Destroying partition DO ${ctx.doName} (partitionId=${ctx.partitionId})`);
			// Discover children dynamically: the in-memory topology only knows root nodes,
			// but split children are recorded in the DO's own split status.
			const { splitStatus, promotedKeys } = await stub.status(ctx);
			if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
				for (const childCtx of splitStatus.childPartitionContexts) {
					await destroyPartition(childCtx);
				}
			}
			// Destroy each linked range structure BEFORE the hash partition that links it. Each range root
			// recurses its own split children via the same path. Deduped by hashKey across the whole tree.
			// Skip 'queued' keys — their range root is not created yet (nothing to destroy).
			for (const { hashKey, status } of promotedKeys ?? []) {
				if (status === "queued") continue;
				const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, hashKey, null, null);
				if (destroyedRangeRoots.has(rangeRootCtx.doName)) continue;
				destroyedRangeRoots.add(rangeRootCtx.doName);
				await destroyPartition(rangeRootCtx);
			}
			try {
				await stub.destroyPartition();
			} catch (e) {
				// console.error(`Error destroying partition DO ${ctx.doName} (partitionId=${ctx.partitionId}):`, e);
				if (!String(e).includes("__special_destroy_sentinel")) throw e;
			}
			console.warn(`Destroyed partition DO ${ctx.doName} (partitionId=${ctx.partitionId})`);
		};

		for (const rootCtx of this.#options.topology.rootPartitionContexts()) {
			await destroyPartition(rootCtx);
		}

		return { ok: true };
	}
}
