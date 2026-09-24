import * as v from "valibot";
import type {
	ConditionExpression,
	ExpressionReference,
	ExpressionValue,
	JsonValue,
	ProjectionExpression,
	UpdateAction,
	UpdateExpression,
	UpdateTarget,
} from "fokosdb/client";
import { PartitionOptionsSchema } from "../shared.js";

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

const ProjectionEntrySchema: v.GenericSchema<ProjectionExpression> = v.strictObject({
	expr: ExpressionValueSchema,
	as: v.optional(v.string()),
});

export const PutItemBodySchema = v.strictObject({
	hashKey: v.string(),
	sortKey: v.optional(v.string()),
	ttlAt: v.optional(v.number()),
	data: v.string(),
	condition: v.optional(ConditionExpressionSchema),
	partitionOptions: PartitionOptionsSchema,
});

export const GetItemBodySchema = v.object({
	hashKey: v.string(),
	sortKey: v.optional(v.string()),
	projection: v.optional(v.array(ProjectionEntrySchema)),
	partitionOptions: PartitionOptionsSchema,
});

export const DeleteItemBodySchema = v.strictObject({
	hashKey: v.string(),
	sortKey: v.optional(v.string()),
	condition: v.optional(ConditionExpressionSchema),
	partitionOptions: PartitionOptionsSchema,
});

const UpdateTargetSchema: v.GenericSchema<UpdateTarget> = v.strictObject({
	ref: v.literal("data"),
	path: v.string(),
});

const UpdateActionSchema: v.GenericSchema<UpdateAction> = v.variant("action", [
	v.strictObject({
		action: v.literal("set"),
		target: UpdateTargetSchema,
		value: ExpressionValueSchema,
	}),
	v.strictObject({
		action: v.literal("remove"),
		target: UpdateTargetSchema,
	}),
]);

const UpdateExpressionSchema: v.GenericSchema<UpdateExpression> = v.array(UpdateActionSchema);

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
	v.strictObject({
		operation: v.literal("update"),
		hashKey: v.string(),
		sortKey: v.optional(v.string()),
		update: UpdateExpressionSchema,
		ttlAt: v.optional(v.number()),
		condition: v.optional(ConditionExpressionSchema),
	}),
]);

export const TransactWriteItemsBodySchema = v.object({
	items: v.array(TransactWriteItemBodySchema),
	clientRequestToken: v.optional(v.string()),
	partitionOptions: PartitionOptionsSchema,
});

export const TransactGetItemsBodySchema = v.object({
	items: v.array(
		v.object({ hashKey: v.string(), sortKey: v.optional(v.string()), projection: v.optional(v.array(ProjectionEntrySchema)) }),
	),
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

export const QueryItemsBodySchema = v.object({
	queries: v.array(
		v.object({ hashKey: v.string(), sortKeyCondition: v.optional(SortKeyConditionSchema), scanIndexForward: v.optional(v.boolean()) }),
	),
	limit: v.optional(PositiveIntSchema),
	maxResponseBytes: v.optional(PositiveIntSchema),
	cursor: v.optional(v.string()),
	select: v.optional(v.union([v.literal("projection"), v.literal("count")])),
	filter: v.optional(ConditionExpressionSchema),
	projection: v.optional(v.array(ProjectionEntrySchema)),
	partitionOptions: PartitionOptionsSchema,
});
