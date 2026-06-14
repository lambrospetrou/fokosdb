import { DeleteItemOptions, GetItemOptions, InitiateReadResponse, InitiateWriteResponse, PutItemOptions } from "./types.js";
import { PartitionDO } from "./do-partition.js";
import { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import type { PartitionTopologyRouter } from "./partition-topology/router.js";
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
			shardGroupName: `${this.#options.topology.partitionContext().tableName}.tc`,
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

		// The router owns the traversal (child-discovery order, range-root resolution, dedup);
		// FokosDB supplies the two callbacks that perform the RPCs.
		await this.#options.topology.traverseForDestroy(
			async (ctx) => {
				const stub = ns.get(ns.idFromName(ctx.doName));
				console.warn(`Destroying partition DO ${ctx.doName} (partitionId=${ctx.partitionId})`);
				const { splitStatus, promotedKeys } = await stub.status(ctx);
				return { splitStatus, promotedKeys };
			},
			async (ctx) => {
				const stub = ns.get(ns.idFromName(ctx.doName));
				try {
					await stub.destroyPartition();
				} catch (e) {
					// console.error(`Error destroying partition DO ${ctx.doName} (partitionId=${ctx.partitionId}):`, e);
					if (!String(e).includes("__special_destroy_sentinel")) throw e;
				}
				console.warn(`Destroyed partition DO ${ctx.doName} (partitionId=${ctx.partitionId})`);
			},
		);

		return { ok: true };
	}
}
