/**
 * Client entry point: everything a Worker needs to talk to a FokosDB table.
 *
 * This file IS the public surface. Every name is listed, never `export *`, so adding a type to a
 * shared module does not publish it and the list below is the whole inventory a caller can reach.
 *
 * It reaches into `../shared/` per module rather than through a barrel, so an entry pulls in only
 * the modules it names. Nothing here imports a Durable Object class as a value, which keeps the
 * server implementation out of this bundle.
 */
export { FokosDB } from "./db.js";
export type { FokosDBOptions } from "./db.js";
export { FokosStd } from "./fokos-std.js";

// ─── Item data and single-item operations ─────────────────────────────────────

export { DATA_KINDS } from "../shared/types.js";
export type {
	ConditionCheckImage,
	DataKind,
	DecodedItemData,
	DeleteItemOptions,
	DeleteItemResult,
	GetItemOptions,
	GetItemResult,
	ItemDeleter,
	ItemGetter,
	ItemKey,
	ItemPutter,
	ItemQuerier,
	OperationMetrics,
	PartitionInfo,
	PutItemOptions,
	PutItemResult,
	QueryItemsMeta,
	QueryItemsOptions,
	QueryItemsPage,
	QueryItemsProjectedOptions,
	QueryItemsProjectedResult,
	QueryItemsResult,
	QuerySelect,
	ReadItem,
	ReadItemValue,
	ReturnValuesOnConditionCheckFailure,
	SortKeyCondition,
} from "../shared/types.js";

// ─── Transactions ─────────────────────────────────────────────────────────────

// Only what a caller of FokosDB needs. The 2PC wire types — the coordinator requests, the driver
// items, and the encoded read results that carry `projected` as a positional row — live in
// `shared/transaction-wire-types.ts`, so the public surface shows one read envelope and never the
// wire beneath it.
export type {
	FokosDBAPI,
	ItemTransactor,
	MaybeReadItem,
	RejectionReason,
	TransactGetItemKey,
	TransactGetItemsOptions,
	TransactGetItemsResult,
	TransactWriteItem,
	TransactWriteItemsOptions,
	TransactWriteItemsResult,
	TransactWriteOperationResult,
} from "../shared/transaction-api-types.js";

// ─── Expressions ──────────────────────────────────────────────────────────────

export type { JsonComposite, JsonPrimitive, JsonValue } from "../shared/json-types.js";

export { EXPRESSION_LIMITS } from "../shared/expression/limits.js";
export type { ExpressionLimitName } from "../shared/expression/limits.js";
export { EXPRESSION_NATIVE_TYPES } from "../shared/expression/types.js";
export type {
	ConditionExpression,
	ExpressionNativeType,
	ExpressionReference,
	ExpressionValue,
	ProjectionExpression,
	UpdateAction,
	UpdateExpression,
	UpdateTarget,
} from "../shared/expression/types.js";
export type { ProjectedItem, ProjectedValue } from "../shared/expression/projection.js";

export { ExpressionError } from "../shared/expression/errors.js";
export { compileConditionExpression, compileUpdateExpression } from "../shared/expression/compiler.js";

// ─── Partition topology ───────────────────────────────────────────────────────

export { PartitionContextCreator } from "../shared/partition-topology/partition-context.js";
export type { PartitionContext, PartitionContextResolved, SplitConditions } from "../shared/partition-topology/partition-context.js";

export { PartitionTopologyRouterImpl } from "../shared/partition-topology/router.js";
export type { PartitionTopologyRouter } from "../shared/partition-topology/router.js";

// ─── Errors ───────────────────────────────────────────────────────────────────

export {
	FokosError,
	FokosValidationError,
	FokosExpressionError,
	FokosConflictError,
	FokosUnavailableError,
	FokosTransactionPendingError,
	FokosRoutingError,
	FokosInternalError,
} from "../shared/errors.js";
export type { FokosErrorOrigin, FokosErrorWire } from "../shared/errors.js";
export { FokosConditionCheckError, FokosTransactionCancelledError, isFokosAnyError } from "../shared/errors-operations.js";
export type { FokosAnyError, FokosErrorCode } from "../shared/errors-operations.js";
