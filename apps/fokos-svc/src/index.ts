import { DurableObject } from "cloudflare:workers";
import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import * as v from "valibot";
import { FokosDB } from "./lib/db.js";
import { PartitionContextCreator, PartitionTopologyRouterImpl, type SplitConditions } from "./lib/partition-topology/partition-topology.js";
import type { GetItemResult, InitiateReadResponse } from "./lib/types.js";

export { PartitionDO } from "./lib/do-partition.js";
export { TransactionCoordinatorDO } from "./lib/do-transaction-coordinator.js";

// ── Valibot schemas ────────────────────────────────────────────────────────────

const SplitConditionsSchema = v.object({
	maxSizeMb: v.optional(v.number()),
	// maxItems: v.optional(v.number()),
});

const PartitionOptionsSchema = v.optional(
	v.object({
		rootTreesN: v.optional(v.number()),
		hashSplitN: v.optional(v.number()),
		rangeSplitN: v.optional(v.number()),
		hashSplitConditions: v.optional(SplitConditionsSchema),
		rangeSplitConditions: v.optional(SplitConditionsSchema),
	}),
);

const ItemConditionSchema = v.union([
	v.object({ type: v.literal("item_exists") }),
	v.object({ type: v.literal("item_not_exists") }),
	v.object({ type: v.literal("attribute_equals"), attribute: v.literal("v"), value: v.number() }),
]);

const PutItemBodySchema = v.pipe(
	v.object({
		hashKey: v.string(),
		sortKey: v.optional(v.string()),
		ttlSeconds: v.optional(v.number()),
		ttlEpochUTCSeconds: v.optional(v.number()),
		data: v.string(),
		conditions: v.optional(v.array(ItemConditionSchema)),
		partitionOptions: PartitionOptionsSchema,
	}),
	v.check(
		(input) => !(input.ttlSeconds !== undefined && input.ttlEpochUTCSeconds !== undefined),
		"Only one of ttlSeconds or ttlEpochUTCSeconds may be provided, not both",
	),
);

const GetItemBodySchema = v.object({
	hashKey: v.string(),
	sortKey: v.optional(v.string()),
	partitionOptions: PartitionOptionsSchema,
});

const DeleteItemBodySchema = v.object({
	hashKey: v.string(),
	sortKey: v.optional(v.string()),
	conditions: v.optional(v.array(ItemConditionSchema)),
	partitionOptions: PartitionOptionsSchema,
});

const TransactWriteItemsBodySchema = v.object({
	operations: v.array(
		v.object({
			hashKey: v.string(),
			sortKey: v.optional(v.string()),
			operation: v.union([v.literal("put"), v.literal("delete"), v.literal("check")]),
			data: v.optional(v.string()),
			conditions: v.optional(v.array(ItemConditionSchema)),
		}),
	),
	clientRequestToken: v.optional(v.string()),
	partitionOptions: PartitionOptionsSchema,
});

const TransactGetItemsBodySchema = v.object({
	items: v.array(v.object({ hashKey: v.string(), sortKey: v.optional(v.string()) })),
	partitionOptions: PartitionOptionsSchema,
});

// ── Helpers ────────────────────────────────────────────────────────────────────

const DEFAULT_PARTITION_OPTIONS = {
	rootTreesN: 10,
	hashSplitN: 4,
	rangeSplitN: 4,
	hashSplitConditions: { maxSizeMb: 500 } as SplitConditions,
	rangeSplitConditions: { maxSizeMb: 500 } as SplitConditions,
};

type PartitionOptionsInput = v.InferOutput<typeof PartitionOptionsSchema>;

function makeFokosDB(env: Env, databaseName: string, partitionOptions?: PartitionOptionsInput): FokosDB {
	const partitionContext = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		databaseName,
		rootTreesN: partitionOptions?.rootTreesN ?? DEFAULT_PARTITION_OPTIONS.rootTreesN,
		hashSplitN: partitionOptions?.hashSplitN ?? DEFAULT_PARTITION_OPTIONS.hashSplitN,
		rangeSplitN: partitionOptions?.rangeSplitN ?? DEFAULT_PARTITION_OPTIONS.rangeSplitN,
		hashSplitConditions: partitionOptions?.hashSplitConditions ?? DEFAULT_PARTITION_OPTIONS.hashSplitConditions,
		rangeSplitConditions: partitionOptions?.rangeSplitConditions ?? DEFAULT_PARTITION_OPTIONS.rangeSplitConditions,
	});
	const topology = new PartitionTopologyRouterImpl(partitionContext);
	return new FokosDB({
		ns: env.PARTITION_DO,
		topology,
		transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
	});
}

function encodeData(data: Uint8Array | string): { data: string; dataEncoding: "utf8" | "base64" } {
	if (data instanceof Uint8Array) {
		return { data: Buffer.from(data).toString("base64"), dataEncoding: "base64" };
	}
	return { data, dataEncoding: "utf8" };
}

function serializeGetItemResult(result: GetItemResult) {
	if (!result.found) return result;
	const { data, ...itemRest } = result.item;
	return { ...result, item: { ...itemRest, ...encodeData(data) } };
}

function serializeTransactGetItemsResult(result: InitiateReadResponse) {
	if (result.outcome !== "committed") return result;
	return {
		...result,
		items: result.items.map((item) => {
			if (!item.found) return item;
			const { data, ...rest } = item;
			return { ...rest, ...encodeData(data) };
		}),
	};
}

// ── Routes ────────────────────────────────────────────────────────────────────

type HonoVariables = { dbItemMeta?: object };

const api = new Hono<{ Bindings: Env; Variables: HonoVariables }>().basePath("/api");

let cachedValidTokens: Set<string> | null = null;

api.onError((err, c) => {
	if (err instanceof HTTPException) {
		return err.getResponse();
	}
	console.error({
		message: "Unexpected error in catch-all",
		error: String(err),
		errorProps: err,
	});
	return c.json({ error: "Internal Server Error" }, 500);
});

api.use(async (c, next) => {
	const token = c.req.header("x-fokos-secret-token");
	if (!token) {
		throw new HTTPException(401, { message: "Missing x-fokos-secret-token header" });
	}
	cachedValidTokens ??= new Set(
		c.env.FOKOS_API_TOKENS.split(",")
			.map((t) => t.trim())
			.filter(Boolean),
	);
	const validTokens = cachedValidTokens;
	if (!validTokens.has(token)) {
		throw new HTTPException(401, { message: "Invalid token" });
	}
	await next();
});

api.use(async (c, next) => {
	const start = Date.now();
	await next();
	console.log({
		message: `${c.req.method} ${c.req.path} - ${c.res.status}`,
		status: c.res.status,
		path: c.req.path,
		durationMs: Date.now() - start,
		dbItemMeta: c.get("dbItemMeta"),
	});
});

api.get("/hello/:name", async (c) => {
	const name = c.req.param("name");
	return c.json({ message: `Hello, ${name}!` });
});

api.delete("/databases/:databaseName", async (c) => {
	const databaseName = c.req.param("databaseName");
	let partitionOptions: PartitionOptionsInput | undefined;
	try {
		const body = await c.req.json();
		const result = v.safeParse(v.object({ partitionOptions: PartitionOptionsSchema }), body);
		if (!result.success) {
			throw new HTTPException(400, {
				message: JSON.stringify({ error: "Validation failed", issues: v.flatten(result.issues) }),
			});
		}
		partitionOptions = result.output.partitionOptions;
	} catch (e) {
		if (e instanceof HTTPException) throw e;
		// No body or non-JSON body is fine — use defaults.
	}
	await makeFokosDB(c.env, databaseName, partitionOptions).destroy();
	return c.json({ destroyed: true });
});

api.post("/rpc/:databaseName/:rpcAction", async (c) => {
	const databaseName = c.req.param("databaseName");
	const rpcAction = c.req.param("rpcAction");

	let rawBody: unknown;
	try {
		rawBody = await c.req.json();
	} catch {
		throw new HTTPException(400, { message: "Invalid JSON body" });
	}

	function parseBody<S extends v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>(schema: S): v.InferOutput<S> {
		const result = v.safeParse(schema, rawBody);
		if (!result.success) {
			throw new HTTPException(400, {
				message: JSON.stringify({ error: "Validation failed", issues: v.flatten(result.issues) }),
			});
		}
		return result.output as v.InferOutput<S>;
	}

	// TODO The following creates a new FokosDB instance (and therefore reconstructs the partition topology) on every request, which is inefficient.
	// We should cache these instances keyed by databaseName + partitionOptions.
	switch (rpcAction) {
		case "putItem": {
			const { partitionOptions, ...opts } = parseBody(PutItemBodySchema);
			const result = await makeFokosDB(c.env, databaseName, partitionOptions).putItem(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(result);
		}
		case "getItem": {
			const { partitionOptions, ...opts } = parseBody(GetItemBodySchema);
			const result = await makeFokosDB(c.env, databaseName, partitionOptions).getItem(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(serializeGetItemResult(result));
		}
		case "deleteItem": {
			const { partitionOptions, ...opts } = parseBody(DeleteItemBodySchema);
			const result = await makeFokosDB(c.env, databaseName, partitionOptions).deleteItem(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(result);
		}
		case "transactWriteItems": {
			const { partitionOptions, ...opts } = parseBody(TransactWriteItemsBodySchema);
			return c.json(await makeFokosDB(c.env, databaseName, partitionOptions).transactWriteItems(opts));
		}
		case "transactGetItems": {
			const { partitionOptions, ...opts } = parseBody(TransactGetItemsBodySchema);
			return c.json(serializeTransactGetItemsResult(await makeFokosDB(c.env, databaseName, partitionOptions).transactGetItems(opts)));
		}
		default:
			throw new HTTPException(404, { message: `Unknown rpcAction: ${rpcAction}` });
	}
});

export default {
	async fetch(request, env, ctx): Promise<Response> {
		return api.fetch(request, env, ctx);
	},
} satisfies ExportedHandler<Env>;
