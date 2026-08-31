import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import * as v from "valibot";
import { FokosDB } from "../../lib/db.js";
import { PartitionContextCreator, type SplitConditions } from "../../lib/partition-topology/partition-context.js";
import { PartitionTopologyRouterImpl } from "../../lib/partition-topology/router.js";
import type {
	ConditionExpression,
	ExpressionReference,
	ExpressionValue,
	GetItemResult,
	InitiateReadResponse,
	JsonValue,
	QueryItemsResult,
} from "../../lib/types.js";
import { PartitionDO } from "../../lib/do-partition.js";
import { ExpressionError } from "../../lib/expression/errors.js";

export { PartitionDO } from "../../lib/do-partition.js";
export { TransactionCoordinatorDO } from "../../lib/do-transaction-coordinator.js";

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

const JsonValueSchema: v.GenericSchema<JsonValue> = v.lazy(() =>
	v.union([v.string(), v.number(), v.boolean(), v.null(), v.array(JsonValueSchema), v.record(v.string(), JsonValueSchema)]),
);

const ExpressionReferenceSchema: v.GenericSchema<ExpressionReference> = v.union([
	v.strictObject({ ref: v.literal("hashKey") }),
	v.strictObject({ ref: v.literal("sortKey") }),
	v.strictObject({ ref: v.literal("v") }),
	v.strictObject({ ref: v.literal("ttlAt") }),
	v.strictObject({ ref: v.literal("data"), path: v.optional(v.string()) }),
]);

const ExpressionValueSchema: v.GenericSchema<ExpressionValue> = v.lazy(() =>
	v.union([
		v.strictObject({ val: JsonValueSchema }),
		v.strictObject({ b64: v.string() }),
		ExpressionReferenceSchema,
		v.strictObject({ fn: v.string(), args: v.array(ExpressionValueSchema) }),
	]),
);

const ConditionExpressionSchema: v.GenericSchema<ConditionExpression> = v.lazy(() =>
	v.variant("op", [
		v.strictObject({
			op: v.union([v.literal("eq"), v.literal("ne"), v.literal("lt"), v.literal("lte"), v.literal("gt"), v.literal("gte")]),
			args: v.tuple([ExpressionValueSchema, ExpressionValueSchema]),
		}),
		v.strictObject({ op: v.literal("between"), args: v.tuple([ExpressionValueSchema, ExpressionValueSchema, ExpressionValueSchema]) }),
		v.strictObject({ op: v.literal("in"), args: v.tupleWithRest([ExpressionValueSchema, ExpressionValueSchema], ExpressionValueSchema) }),
		v.strictObject({
			op: v.union([v.literal("and"), v.literal("or")]),
			args: v.tupleWithRest(
				[v.lazy(() => ConditionExpressionSchema), v.lazy(() => ConditionExpressionSchema)],
				v.lazy(() => ConditionExpressionSchema),
			),
		}),
		v.strictObject({ op: v.literal("not"), args: v.tuple([v.lazy(() => ConditionExpressionSchema)]) }),
		v.strictObject({
			op: v.union([v.literal("exists"), v.literal("not_exists")]),
			args: v.tuple([ExpressionReferenceSchema]),
		}),
		v.strictObject({
			op: v.union([v.literal("begins_with"), v.literal("contains")]),
			args: v.tuple([ExpressionValueSchema, ExpressionValueSchema]),
		}),
	]),
);

const PutItemBodySchema = v.strictObject({
	hashKey: v.string(),
	sortKey: v.optional(v.string()),
	ttlAt: v.optional(v.number()),
	data: v.string(),
	condition: v.optional(ConditionExpressionSchema),
	partitionOptions: PartitionOptionsSchema,
});

const GetItemBodySchema = v.object({
	hashKey: v.string(),
	sortKey: v.optional(v.string()),
	partitionOptions: PartitionOptionsSchema,
});

const DeleteItemBodySchema = v.strictObject({
	hashKey: v.string(),
	sortKey: v.optional(v.string()),
	condition: v.optional(ConditionExpressionSchema),
	partitionOptions: PartitionOptionsSchema,
});

// Mirrors the `TransactWriteItem` union. `strictObject` turns a field belonging to another variant —
// data on a delete — into a 400 that names it, instead of silently stripping it.
const TransactWriteItemBodySchema = v.variant("operation", [
	v.strictObject({
		operation: v.literal("put"),
		hashKey: v.string(),
		sortKey: v.optional(v.string()),
		data: v.string(),
		ttlAt: v.optional(v.number()),
		condition: v.optional(ConditionExpressionSchema),
	}),
	v.strictObject({
		operation: v.literal("delete"),
		hashKey: v.string(),
		sortKey: v.optional(v.string()),
		condition: v.optional(ConditionExpressionSchema),
	}),
	v.strictObject({
		operation: v.literal("check"),
		hashKey: v.string(),
		sortKey: v.optional(v.string()),
		condition: ConditionExpressionSchema,
	}),
]);

const TransactWriteItemsBodySchema = v.object({
	items: v.array(TransactWriteItemBodySchema),
	clientRequestToken: v.optional(v.string()),
	partitionOptions: PartitionOptionsSchema,
});

const TransactGetItemsBodySchema = v.object({
	items: v.array(v.object({ hashKey: v.string(), sortKey: v.optional(v.string()) })),
	partitionOptions: PartitionOptionsSchema,
});

// FIXME: the HTTP API only accepts string keys; support Uint8Array (binary) keys via a
// keyEncoding discriminator or base64-encoded binary form.
const SortKeyConditionSchema = v.union([
	v.object({ op: v.literal("eq"), value: v.string() }),
	v.object({ op: v.union([v.literal("lt"), v.literal("lte"), v.literal("gt"), v.literal("gte")]), value: v.string() }),
	v.object({ op: v.literal("between"), lower: v.string(), upper: v.string() }),
	v.object({ op: v.literal("begins_with"), prefix: v.string() }),
	v.object({
		op: v.literal("range"),
		lower: v.optional(v.object({ value: v.string(), inclusive: v.boolean() })),
		upper: v.optional(v.object({ value: v.string(), inclusive: v.boolean() })),
	}),
]);

const PositiveIntSchema = v.pipe(v.number(), v.integer(), v.minValue(1));

const QueryItemsBodySchema = v.object({
	queries: v.array(
		v.object({ hashKey: v.string(), sortKeyCondition: v.optional(SortKeyConditionSchema), scanIndexForward: v.optional(v.boolean()) }),
	),
	limit: v.optional(PositiveIntSchema),
	maxPageBytes: v.optional(PositiveIntSchema),
	cursor: v.optional(v.string()),
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

function makeFokosDB(env: Env, tableName: string, partitionOptions?: PartitionOptionsInput): FokosDB {
	const partitionContext = PartitionContextCreator.create({
		ns: "CUSTOM_PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName,
		rootTreesN: partitionOptions?.rootTreesN ?? DEFAULT_PARTITION_OPTIONS.rootTreesN,
		hashSplitN: partitionOptions?.hashSplitN ?? DEFAULT_PARTITION_OPTIONS.hashSplitN,
		rangeSplitN: partitionOptions?.rangeSplitN ?? DEFAULT_PARTITION_OPTIONS.rangeSplitN,
		hashSplitConditions: partitionOptions?.hashSplitConditions ?? DEFAULT_PARTITION_OPTIONS.hashSplitConditions,
		rangeSplitConditions: partitionOptions?.rangeSplitConditions ?? DEFAULT_PARTITION_OPTIONS.rangeSplitConditions,
	});
	const topology = new PartitionTopologyRouterImpl(partitionContext);
	return new FokosDB({
		topology,
		transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
	});
}

// The HTTP write surface only accepts string `data` (PutItemBodySchema), so over HTTP items are always
// text; but a json/bytes row created via the programmatic API can still be read back here, so all three
// kinds are serialized. json values are re-stringified with a distinct `dataEncoding` discriminant.
function encodeData(data: string | Uint8Array | JsonValue): { data: string; dataEncoding: "utf8" | "base64" | "json" } {
	if (data instanceof Uint8Array) {
		return { data: Buffer.from(data).toString("base64"), dataEncoding: "base64" };
	}
	if (typeof data === "string") {
		return { data, dataEncoding: "utf8" };
	}
	return { data: JSON.stringify(data), dataEncoding: "json" };
}

function serializeGetItemResult(result: GetItemResult) {
	if (!result.found) return result;
	const { data, ...itemRest } = result.item;
	return { ...result, item: { ...itemRest, ...encodeData(data) } };
}

function serializeQueryItemsResult(result: QueryItemsResult) {
	return {
		...result,
		items: result.items.map((item) => {
			const { data, hashKey, sortKey, ...rest } = item;
			// The HTTP surface is string-only for keys (every endpoint uses v.string()), so writes can
			// only produce UTF-8 keys and a scan can only decode strings back. A Uint8Array key here
			// means a binary key reached the store via the programmatic/RPC API — it would serialize to
			// `{"0":..}` over c.json. Fail loudly rather than emit broken JSON; binary keys over HTTP
			// would need a keyEncoding discriminator, not yet wired.
			if (hashKey instanceof Uint8Array || sortKey instanceof Uint8Array) {
				throw new HTTPException(500, { message: "fokos/queryItems: binary keys are not supported over the HTTP API" });
			}
			return { ...rest, hashKey, sortKey, ...encodeData(data) };
		}),
	};
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
	if (err instanceof ExpressionError) {
		return c.json({ error: err.message, code: err.code }, 400);
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
	const durationMs = Date.now() - start;
	c.header("Server-Timing", `worker;dur=${durationMs}`);
	console.log({
		message: `${c.req.method} ${c.req.path} - ${c.res.status}`,
		status: c.res.status,
		path: c.req.path,
		durationMs,
		dbItemMeta: c.get("dbItemMeta"),
	});
});

api.get("/hello/:name", async (c) => {
	const name = c.req.param("name");
	return c.json({ message: `Hello, ${name}!` });
});

api.delete("/databases/:tableName", async (c) => {
	const tableName = c.req.param("tableName");
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
	await makeFokosDB(c.env, tableName, partitionOptions).destroy();
	return c.json({ destroyed: true });
});

api.post("/rpc/:tableName/:rpcAction", async (c) => {
	const tableName = c.req.param("tableName");
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
	// We should cache these instances keyed by tableName + partitionOptions.
	switch (rpcAction) {
		case "putItem": {
			const { partitionOptions, ...opts } = parseBody(PutItemBodySchema);
			const result = await makeFokosDB(c.env, tableName, partitionOptions).putItem(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(result);
		}
		case "getItem": {
			const { partitionOptions, ...opts } = parseBody(GetItemBodySchema);
			const result = await makeFokosDB(c.env, tableName, partitionOptions).getItem(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(serializeGetItemResult(result));
		}
		case "deleteItem": {
			const { partitionOptions, ...opts } = parseBody(DeleteItemBodySchema);
			const result = await makeFokosDB(c.env, tableName, partitionOptions).deleteItem(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(result);
		}
		case "transactWriteItems": {
			const { partitionOptions, ...opts } = parseBody(TransactWriteItemsBodySchema);
			return c.json(await makeFokosDB(c.env, tableName, partitionOptions).transactWriteItems(opts));
		}
		case "transactGetItems": {
			const { partitionOptions, ...opts } = parseBody(TransactGetItemsBodySchema);
			return c.json(serializeTransactGetItemsResult(await makeFokosDB(c.env, tableName, partitionOptions).transactGetItems(opts)));
		}
		case "queryItems": {
			const { partitionOptions, ...opts } = parseBody(QueryItemsBodySchema);
			const result = await makeFokosDB(c.env, tableName, partitionOptions).queryItems(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(serializeQueryItemsResult(result));
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

// TESTING THE PartitionDO override capabilities.

export class CustomPartitionDO extends PartitionDO {}
