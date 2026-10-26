/**
 * The writer of the GSI tables of a `FokosDBMaterializationDO`. It holds one `FokosShardingClient` for each
 * GSI shard group, sends `gsiWriteEntries` to the owner partition of each entry, and maps each entry
 * to one outcome. It does not use `FokosDB`.
 */
import { classifyBatchFailure } from "../../shared/batch-failure.js";
import { gsiTableConfig, type FokosDBPolicy, type FokosDBRouteContext, type FokosDBTableConfig } from "../../shared/partition-context.js";
import { FokosShardingClient } from "../../sharding/client.js";
import type { GsiWriteEntriesRequest, PartitionOps } from "../do-partition.js";
import type { GsiEntryOutcome } from "./target-runner.js";
import type { GsiTargetDefinition } from "./targets.js";

export type GsiWriterDeps = {
	/** The table configuration of this materialization partition. */
	table: () => FokosDBTableConfig;
	stub: (ctx: FokosDBRouteContext, doName: string) => DurableObjectStub;
};

export class GsiWriter {
	readonly #deps: GsiWriterDeps;
	readonly #clients = new Map<string, { policyVersion: number; client: FokosShardingClient<FokosDBPolicy, PartitionOps> }>();

	constructor(deps: GsiWriterDeps) {
		this.#deps = deps;
	}

	/** One outcome for each entry, in order. No retry: the target runner retries with its own rules. */
	async write(target: GsiTargetDefinition, req: GsiWriteEntriesRequest): Promise<GsiEntryOutcome[]> {
		const client = this.#client(target.targetId);
		const out: Array<GsiEntryOutcome | undefined> = Array.from({ length: req.items.length });
		await Promise.all(
			client.resolveAll(req.items).map(async ({ ctx, indexes }) => {
				const part = indexes.map((i) => req.items[i]);
				try {
					const { value } = await client.send("gsiWriteEntries", ctx, { ...req, items: part }, part);
					for (const { opIndex } of value.applied) {
						out[opIndex] = { outcome: "applied" };
					}
					for (const failure of value.failed) {
						for (const opIndex of failure.opIndexes) {
							out[opIndex] = { outcome: failure.outcome, code: failure.code };
						}
					}
				} catch (error) {
					const failure = classifyBatchFailure(error, { write: true, local: false });
					for (const i of indexes) {
						out[i] = { outcome: failure.outcome, code: failure.code, wholeCall: true };
					}
				}
			}),
		);
		return out.map((outcome) => outcome ?? { outcome: "unknown", code: "invariant_failed" });
	}

	#client(indexName: string): FokosShardingClient<FokosDBPolicy, PartitionOps> {
		const table = gsiTableConfig(this.#deps.table(), indexName);
		const cached = this.#clients.get(indexName);
		if (cached !== undefined && cached.policyVersion === table.policyVersion) {
			return cached.client;
		}
		const client = new FokosShardingClient<FokosDBPolicy, PartitionOps>({ ...table, stub: this.#deps.stub });
		this.#clients.set(indexName, { policyVersion: table.policyVersion, client });
		return client;
	}
}
