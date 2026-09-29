/**
 * The cases of the client creation benchmarks. A Worker makes its clients for each request, so each case
 * makes one set of objects the way a request does. `client-creation.workerd-bench.ts` times the cases in
 * workerd, and `client-creation-alloc.mjs` counts their heap bytes in Node.
 */
import { FokosDB } from "../../src/client/db.js";
import { createTableConfig, type FokosDBPolicy } from "../../src/shared/partition-context.js";
import { FokosShardingClient } from "../../src/sharding/client.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { FokosRouter } from "../../src/sharding/router.js";
import type { FokosOperationSpec } from "../../src/sharding/runtime-types.js";

// The table options of examples/http-api/src/shared.ts.
const TABLE_OPTIONS = {
	table: { name: "bench", ns: "CUSTOM_PARTITION_DO", nsTx: "TRANSACTION_COORDINATOR_DO", rootTreesN: 10, hashSplitN: 4 },
	rangeSplitN: 4,
	hashSplitConditions: { maxSizeMb: 500 },
	rangeSplitConditions: { maxSizeMb: 500 },
} as const;

const table = createTableConfig(TABLE_OPTIONS);
const key = { hashKey: KeyCodec.encode("user#12345"), sortKey: KeyCodec.encode("order#1") };
// The stub function is never called: no case sends a request.
const stub = (): DurableObjectStub => {
	throw new Error("the benchmark sends no request");
};

export const CASES: [string, () => unknown][] = [
	["createTableConfig", () => createTableConfig(TABLE_OPTIONS)],
	["new FokosRouter", () => new FokosRouter(table.topology, table.rangeConfig, table.policy)],
	["new FokosShardingClient", () => new FokosShardingClient<FokosDBPolicy, FokosOperationSpec>({ ...table, stub })],
	[
		// The router of a new client caches no root context, so the first request of each client builds one.
		"new FokosShardingClient + first resolve",
		() => new FokosShardingClient<FokosDBPolicy, FokosOperationSpec>({ ...table, stub }).resolve(key),
	],
	// The path of examples/http-api/src/shared.ts for each request.
	["new FokosDB", () => new FokosDB(TABLE_OPTIONS)],
];
