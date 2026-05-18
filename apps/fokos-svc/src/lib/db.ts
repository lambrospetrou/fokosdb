import { DeleteItemOptions, GetItemOptions, InitiateReadRequest, InitiateReadResponse, InitiateWriteRequest, InitiateWriteResponse, PutItemOptions } from "./types.js";
import { PartitionDO } from "./do-partition.js";
import { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import { PartitionTopologyRouter } from "./partition-topology/partition-topology.js";
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

	async transactGetItems(opts: { items: Array<{ hashKey: string; sortKey?: string }> }): Promise<InitiateReadResponse> {
		const items: TCReadItem[] = opts.items.map((item) => {
			const { partitionContext } = this.options.topology.pickPartition(item.hashKey, item.sortKey);
			return { ...item, partitionContext };
		});

		// Read-only TCs are ephemeral — random UUID DO name, no idempotency.
		const tcStub = this.options.transactionCoordinatorNs.get(this.options.transactionCoordinatorNs.idFromName(crypto.randomUUID().replaceAll("-", "")));
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
