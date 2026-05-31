import { DeleteItemOptions, GetItemOptions, InitiateReadResponse, InitiateWriteResponse, PutItemOptions } from "./types.js";
import { PartitionDO } from "./do-partition.js";
import { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import { PartitionTopologyRouter, resolveRangePartitionContext, type PartitionContextResolved } from "./partition-topology/partition-topology.js";
import type { TCWriteOperation, TCReadItem } from "./transaction-types.js";
import type { ItemCondition } from "./types.js";

export type FokosDBOptions = {
	ns: DurableObjectNamespace<PartitionDO>;
	topology: PartitionTopologyRouter;
	transactionCoordinatorNs: DurableObjectNamespace<TransactionCoordinatorDO>;
};

export class FokosDB {
	constructor(public readonly options: FokosDBOptions) {}

	async putItem(opts: PutItemOptions) {
		const { doId, partitionContext } = this.options.topology.pickPartition(opts.hashKey, opts.sortKey);
		const ns = this.options.ns;
		const stub = ns.get(doId);
		return await stub.putItem(partitionContext, opts);
	}

	async getItem(opts: GetItemOptions) {
		const { doId, partitionContext } = this.options.topology.pickPartition(opts.hashKey, opts.sortKey);
		const ns = this.options.ns;
		const stub = ns.get(doId);
		return await stub.getItem(partitionContext, opts);
	}

	async deleteItem(opts: DeleteItemOptions) {
		const { doId, partitionContext } = this.options.topology.pickPartition(opts.hashKey, opts.sortKey);
		const ns = this.options.ns;
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
			const { partitionContext } = this.options.topology.pickPartition(op.hashKey, op.sortKey);
			return { ...op, partitionContext };
		});

		validateTransactWriteItems(operations);

		const idempotencyToken = opts.clientRequestToken ?? crypto.randomUUID().replaceAll("-", "");
		const tcStub = this.options.transactionCoordinatorNs.get(this.options.transactionCoordinatorNs.idFromName(idempotencyToken));
		return await tcStub.initiateWrite({ clientRequestToken: opts.clientRequestToken, operations });
	}

	async destroy(): Promise<{ ok: true }> {
		const ns = this.options.ns;

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

		for (const rootCtx of this.options.topology.rootPartitionContexts()) {
			await destroyPartition(rootCtx);
		}

		return { ok: true };
	}

	async transactGetItems(opts: { items: Array<{ hashKey: string; sortKey?: string }> }): Promise<InitiateReadResponse> {
		const items: TCReadItem[] = opts.items.map((item) => {
			const { partitionContext } = this.options.topology.pickPartition(item.hashKey, item.sortKey);
			return { ...item, partitionContext };
		});

		// Read-only TCs are ephemeral — random UUID DO name, no idempotency.
		const tcStub = this.options.transactionCoordinatorNs.get(
			this.options.transactionCoordinatorNs.idFromName(crypto.randomUUID().replaceAll("-", "")),
		);
		return await tcStub.initiateRead({ items });
	}
}

function validateTransactWriteItems(ops: TCWriteOperation[]): void {
	if (ops.length > 100) throw new Error("TransactWriteItems: max 100 items");
	const seen = new Set<string>();
	let totalBytes = 0;
	for (const op of ops) {
		if (op.operation === "put" && op.data == null) {
			throw new Error(`TransactWriteItems: "put" operation requires data (${op.hashKey}${op.sortKey ? `, ${op.sortKey}` : ""})`);
		}
		const key = `${op.hashKey}\0${op.sortKey ?? ""}`;
		if (seen.has(key)) throw new Error(`TransactWriteItems: duplicate key (${op.hashKey}, ${op.sortKey ?? ""})`);
		seen.add(key);
		if (op.data) totalBytes += typeof op.data === "string" ? op.data.length * 2 : op.data.byteLength;
	}
	if (totalBytes > 4 * 1024 * 1024) throw new Error("TransactWriteItems: total payload exceeds 4 MB");
}
