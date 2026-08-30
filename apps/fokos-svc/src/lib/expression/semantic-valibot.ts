import * as v from "valibot";
import type { JsonPrimitive } from "../json-types.js";
import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { validateReadJsonPath } from "./path.js";
import { isAllowedSqliteScalarFunction, SQLITE_MATH_FUNCTIONS } from "./sqlite-functions.js";
import { EXPRESSION_NATIVE_TYPES, type ExpressionNativeType } from "./types.js";
import {
	EXPRESSION_REQUIRED_COLUMNS,
	type ConditionExpressionAnalysis,
	type ExpressionRequiredColumn,
	type ExpressionValueAnalysis,
} from "./semantic.js";

type DirectReference = "hashKey" | "sortKey" | "v" | "ttl" | "data";

type ValueFacts = {
	types: ReadonlySet<ExpressionNativeType>;
	directReference?: DirectReference;
	literal?: JsonPrimitive;
};

type AnalysisNode = {
	requiredColumns: ReadonlySet<ExpressionRequiredColumn>;
	operations: number;
	depth: number;
};

type ValueNode = AnalysisNode & ValueFacts;
type ConditionNode = AnalysisNode;
type ValueSchema = v.GenericSchema<unknown, ValueNode>;
type ConditionSchema = v.GenericSchema<unknown, ConditionNode>;

const emptyColumns = new Set<ExpressionRequiredColumn>();
const equalityTypes = new Set<ExpressionNativeType>(["null", "boolean", "number", "text", "bytes"]);
const orderedTypes = new Set<ExpressionNativeType>(["number", "text", "bytes"]);
const prefixTypes = new Set<ExpressionNativeType>(["text", "bytes"]);
const sizeInputTypes = new Set<ExpressionNativeType>(["text", "bytes", "array", "object"]);
const arraySearchTypes = new Set<ExpressionNativeType>(["null", "boolean", "number", "text"]);
const nullTypes = new Set<ExpressionNativeType>(["null"]);
const booleanTypes = new Set<ExpressionNativeType>(["boolean"]);
const numberTypes = new Set<ExpressionNativeType>(["number"]);
const textTypes = new Set<ExpressionNativeType>(["text"]);
const missingNumberTypes = new Set<ExpressionNativeType>(["missing", "number"]);
const missingTextBytesTypes = new Set<ExpressionNativeType>(["missing", "text", "bytes"]);
const nullNumberTypes = new Set<ExpressionNativeType>(["null", "number"]);
const nullTextTypes = new Set<ExpressionNativeType>(["null", "text"]);
const nullBytesTypes = new Set<ExpressionNativeType>(["null", "bytes"]);
const allTypes = new Set<ExpressionNativeType>(EXPRESSION_NATIVE_TYPES);
const sqliteNumberFunctions = new Set([
	"abs",
	"glob",
	"instr",
	"length",
	"like",
	"octet_length",
	"round",
	"sign",
	"unicode",
	...SQLITE_MATH_FUNCTIONS,
]);
const sqliteTextFunctions = new Set([
	"char",
	"concat",
	"concat_ws",
	"hex",
	"lower",
	"ltrim",
	"quote",
	"replace",
	"rtrim",
	"substr",
	"substring",
	"trim",
	"typeof",
	"upper",
]);

const ScalarLiteralSchema = v.union([v.null(), v.string(), v.boolean(), v.pipe(v.number(), v.finite())]);

const LiteralSchema = v.pipe(
	v.strictObject({ val: ScalarLiteralSchema }),
	v.transform(({ val }) => valueNode(literalFacts(val), emptyColumns, 0, 1)),
) as ValueSchema;

const StringLiteralSchema = v.pipe(
	v.strictObject({ val: v.string() }),
	v.transform(({ val }) => valueNode(literalFacts(val), emptyColumns, 0, 1)),
) as ValueSchema;

const PatternLiteralSchema = v.pipe(
	v.strictObject({ val: v.pipe(v.string(), v.maxBytes(EXPRESSION_LIMITS.sqlitePatternBytes)) }),
	v.transform(({ val }) => valueNode(literalFacts(val), emptyColumns, 0, 1)),
) as ValueSchema;

const PathSchema = v.pipe(
	v.string(),
	v.transform((path) => {
		validateReadJsonPath(path);
		return path;
	}),
);

const ReferenceSchema = v.variant("ref", [
	v.pipe(
		v.strictObject({ ref: v.literal("hashKey") }),
		v.transform(() => valueNode({ types: missingTextBytesTypes, directReference: "hashKey" }, columns("hk"), 0, 1)),
	),
	v.pipe(
		v.strictObject({ ref: v.literal("sortKey") }),
		v.transform(() => valueNode({ types: missingTextBytesTypes, directReference: "sortKey" }, columns("sk"), 0, 1)),
	),
	v.pipe(
		v.strictObject({ ref: v.literal("v") }),
		v.transform(() => valueNode({ types: missingNumberTypes, directReference: "v" }, columns("v"), 0, 1)),
	),
	v.pipe(
		v.strictObject({ ref: v.literal("ttl") }),
		v.transform(() => valueNode({ types: missingNumberTypes, directReference: "ttl" }, columns("ttl_epoch_utc_seconds"), 0, 1)),
	),
	v.pipe(
		v.strictObject({ ref: v.literal("data"), path: v.exactOptional(PathSchema) }),
		v.transform(() => valueNode({ types: allTypes, directReference: "data" }, columns("data_kind", "data"), 0, 1)),
	),
]) as ValueSchema;

const InvalidValueSchema = v.never("invalid expression value") as unknown as ValueSchema;

const ExpressionValueSchema = v.lazy((input): ValueSchema => {
	if (isNode(input)) {
		if (Object.hasOwn(input, "val")) return LiteralSchema;
		if (Object.hasOwn(input, "ref")) return ReferenceSchema;
		if (Object.hasOwn(input, "fn")) return FunctionSchema;
	}
	return InvalidValueSchema;
}) as ValueSchema;

const SizeSchema = v.pipe(
	v.strictObject({ fn: v.literal("size"), args: v.strictTuple([ExpressionValueSchema]) }),
	v.check(({ args }) => hasAnyType(args[0].types, sizeInputTypes), "invalid size input type"),
	v.transform(({ args }) => {
		const input = args[0];
		let outputTypes: ReadonlySet<ExpressionNativeType> = numberTypes;
		for (const type of input.types) {
			if (!sizeInputTypes.has(type)) {
				outputTypes = missingNumberTypes;
				break;
			}
		}
		return derivedValue({ types: outputTypes }, args);
	}),
) as ValueSchema;

const AttributeTypeSchema = v.pipe(
	v.strictObject({ fn: v.literal("attribute_type"), args: v.strictTuple([ExpressionValueSchema]) }),
	v.transform(({ args }) => derivedValue({ types: textTypes }, args)),
) as ValueSchema;

const SqliteFunctionNameSchema = v.pipe(
	v.string(),
	v.startsWith("sqlite."),
	v.transform((fullName) => fullName.slice("sqlite.".length)),
	v.check(isAllowedSqliteScalarFunction, "SQLite function is not allowed"),
);

const SqliteFunctionSchema = v.pipe(
	v.strictObject({
		fn: SqliteFunctionNameSchema,
		args: v.pipe(v.array(ExpressionValueSchema), v.maxLength(EXPRESSION_LIMITS.sqliteFunctionArguments)),
	}),
	v.transform(({ fn, args }) => derivedValue({ types: sqliteResultTypes(fn, args) }, args)),
) as ValueSchema;

const GlobFunctionSchema = v.pipe(
	v.strictObject({ fn: v.literal("sqlite.glob"), args: v.strictTuple([PatternLiteralSchema, ExpressionValueSchema]) }),
	v.transform(({ args }) => derivedValue({ types: nullNumberTypes }, args)),
) as ValueSchema;

const LikeFunctionSchema = v.pipe(
	v.strictObject({
		fn: v.literal("sqlite.like"),
		args: v.union([
			v.strictTuple([PatternLiteralSchema, ExpressionValueSchema]),
			v.strictTuple([PatternLiteralSchema, ExpressionValueSchema, StringLiteralSchema]),
		]),
	}),
	v.transform(({ args }) => derivedValue({ types: nullNumberTypes }, args)),
) as ValueSchema;

const FunctionSchema = v.lazy((input): ValueSchema => {
	if (isNode(input)) {
		if (input.fn === "size") return SizeSchema;
		if (input.fn === "attribute_type") return AttributeTypeSchema;
		if (input.fn === "sqlite.glob") return GlobFunctionSchema;
		if (input.fn === "sqlite.like") return LikeFunctionSchema;
	}
	return SqliteFunctionSchema;
}) as ValueSchema;

const InvalidConditionSchema = v.never("unknown condition operator") as unknown as ConditionSchema;

const RecursiveConditionSchema = v.lazy((input): ConditionSchema => {
	if (!isNode(input)) return InvalidConditionSchema;
	switch (input.op) {
		case "eq":
		case "ne":
		case "lt":
		case "lte":
		case "gt":
		case "gte":
			return ComparisonSchema;
		case "between":
			return BetweenSchema;
		case "in":
			return InSchema;
		case "and":
		case "or":
			return LogicalSchema;
		case "not":
			return NotSchema;
		case "exists":
		case "not_exists":
			return ExistenceSchema;
		case "begins_with":
			return BeginsWithSchema;
		case "contains":
			return ContainsSchema;
		default:
			return InvalidConditionSchema;
	}
}) as ConditionSchema;

const ComparisonSchema = v.pipe(
	v.strictObject({
		op: v.picklist(["eq", "ne", "lt", "lte", "gt", "gte"]),
		args: v.strictTuple([ExpressionValueSchema, ExpressionValueSchema]),
	}),
	v.check(({ args }) => hasValidKeyLiterals(args[0], args[1]), "empty key literal is not allowed"),
	v.check(
		({ op, args }) => areCompatible(args[0], args[1], op === "eq" || op === "ne" ? equalityTypes : orderedTypes),
		"incompatible expression types",
	),
	v.transform(({ args }) => derivedCondition(args)),
) as ConditionSchema;

const BetweenSchema = v.pipe(
	v.strictObject({ op: v.literal("between"), args: v.strictTuple([ExpressionValueSchema, ExpressionValueSchema, ExpressionValueSchema]) }),
	v.check(({ args }) => hasValidKeyLiterals(args[0], args[1]) && hasValidKeyLiterals(args[0], args[2]), "empty key literal is not allowed"),
	v.check(({ args }) => hasCommonType3(args[0], args[1], args[2], orderedTypes), "incompatible expression types"),
	v.transform(({ args }) => derivedCondition(args)),
) as ConditionSchema;

const InSchema = v.pipe(
	v.strictObject({
		op: v.literal("in"),
		args: v.pipe(
			v.tupleWithRest([ExpressionValueSchema, ExpressionValueSchema], ExpressionValueSchema),
			v.maxLength(EXPRESSION_LIMITS.inChoices + 1),
		),
	}),
	v.check(({ args }) => hasValidInKeyLiterals(args), "empty key literal is not allowed"),
	v.check(({ args }) => hasCompatibleInTypes(args), "incompatible expression types"),
	v.transform(({ args }) => derivedCondition(args)),
) as ConditionSchema;

const LogicalSchema = v.pipe(
	v.strictObject({
		op: v.picklist(["and", "or"]),
		args: v.tupleWithRest([RecursiveConditionSchema, RecursiveConditionSchema], RecursiveConditionSchema),
	}),
	v.transform(({ args }) => derivedCondition(args)),
) as ConditionSchema;

const NotSchema = v.pipe(
	v.strictObject({ op: v.literal("not"), args: v.strictTuple([RecursiveConditionSchema]) }),
	v.transform(({ args }) => derivedCondition(args)),
) as ConditionSchema;

const ExistenceSchema = v.pipe(
	v.strictObject({ op: v.picklist(["exists", "not_exists"]), args: v.strictTuple([ReferenceSchema]) }),
	v.transform(({ args }) => derivedCondition(args)),
) as ConditionSchema;

const BeginsWithSchema = v.pipe(
	v.strictObject({ op: v.literal("begins_with"), args: v.strictTuple([ExpressionValueSchema, ExpressionValueSchema]) }),
	v.check(({ args }) => hasValidKeyLiterals(args[0], args[1]), "empty key literal is not allowed"),
	v.check(({ args }) => areCompatible(args[0], args[1], prefixTypes), "incompatible expression types"),
	v.transform(({ args }) => derivedCondition(args)),
) as ConditionSchema;

const ContainsSchema = v.pipe(
	v.strictObject({ op: v.literal("contains"), args: v.strictTuple([ExpressionValueSchema, ExpressionValueSchema]) }),
	v.check(({ args }) => hasValidKeyLiterals(args[0], args[1]), "empty key literal is not allowed"),
	v.check(({ args }) => canContain(args[0].types, args[1].types), "incompatible contains types"),
	v.transform(({ args }) => derivedCondition(args)),
) as ConditionSchema;

const BoundedExpressionValueSchema = v.pipe(
	ExpressionValueSchema,
	v.check((result) => result.operations <= EXPRESSION_LIMITS.operatorsAndFunctions, "operator and function limit exceeded"),
	v.check((result) => result.depth <= EXPRESSION_LIMITS.astDepth, "AST depth exceeds the limit"),
) as ValueSchema;

const BoundedConditionSchema = v.pipe(
	RecursiveConditionSchema,
	v.check((result) => result.operations <= EXPRESSION_LIMITS.operatorsAndFunctions, "operator and function limit exceeded"),
	v.check((result) => result.depth <= EXPRESSION_LIMITS.astDepth, "AST depth exceeds the limit"),
) as ConditionSchema;

export function analyzeExpressionValueValibot(expression: unknown): ExpressionValueAnalysis {
	const result = parse(BoundedExpressionValueSchema, expression);
	return { nativeTypes: orderedTypesFrom(result.types), requiredColumns: requiredColumnsFrom(result.requiredColumns) };
}

export function validateConditionExpressionValibot(expression: unknown): ConditionExpressionAnalysis {
	const result = parse(BoundedConditionSchema, expression);
	return { requiredColumns: requiredColumnsFrom(result.requiredColumns) };
}

function parse<TOutput>(schema: v.GenericSchema<unknown, TOutput>, input: unknown): TOutput {
	try {
		return v.parse(schema, input, { abortEarly: true });
	} catch (error) {
		if (error instanceof ExpressionError) throw error;
		if (v.isValiError(error)) throw new ExpressionError("invalid_ast", error.issues[0].message);
		throw error;
	}
}

function isNode(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function valueNode(
	facts: ValueFacts,
	requiredColumns: ReadonlySet<ExpressionRequiredColumn>,
	operations: number,
	depth: number,
): ValueNode {
	return { ...facts, requiredColumns, operations, depth };
}

function derivedValue(facts: ValueFacts, children: readonly ValueNode[]): ValueNode {
	return valueNode(facts, mergeColumns(children), 1 + operationCount(children), 1 + maximumDepth(children));
}

function derivedCondition(children: readonly AnalysisNode[]): ConditionNode {
	return {
		requiredColumns: mergeColumns(children),
		operations: 1 + operationCount(children),
		depth: 1 + maximumDepth(children),
	};
}

function operationCount(nodes: readonly AnalysisNode[]): number {
	let count = 0;
	for (const node of nodes) count += node.operations;
	return count;
}

function maximumDepth(nodes: readonly AnalysisNode[]): number {
	let depth = 0;
	for (const node of nodes) depth = Math.max(depth, node.depth);
	return depth;
}

function columns(...values: ExpressionRequiredColumn[]): ReadonlySet<ExpressionRequiredColumn> {
	return new Set(values);
}

function mergeColumns(nodes: readonly AnalysisNode[]): ReadonlySet<ExpressionRequiredColumn> {
	const result = new Set<ExpressionRequiredColumn>();
	for (const node of nodes) {
		for (const column of node.requiredColumns) result.add(column);
	}
	return result;
}

function requiredColumnsFrom(columns: ReadonlySet<ExpressionRequiredColumn>): readonly ExpressionRequiredColumn[] {
	return EXPRESSION_REQUIRED_COLUMNS.filter((column) => columns.has(column));
}

function orderedTypesFrom(types: ReadonlySet<ExpressionNativeType>): readonly ExpressionNativeType[] {
	return EXPRESSION_NATIVE_TYPES.filter((type) => types.has(type));
}

function literalFacts(value: JsonPrimitive): ValueFacts {
	if (value === null) return { types: nullTypes, literal: value };
	if (typeof value === "boolean") return { types: booleanTypes, literal: value };
	if (typeof value === "number") return { types: numberTypes, literal: value };
	return { types: textTypes, literal: value };
}

function sqliteResultTypes(name: string, args: readonly ValueNode[]): ReadonlySet<ExpressionNativeType> {
	if (sqliteNumberFunctions.has(name)) return nullNumberTypes;
	if (sqliteTextFunctions.has(name)) return nullTextTypes;
	if (name === "unhex") return nullBytesTypes;

	const result = new Set<ExpressionNativeType>(["null"]);
	const length = name === "nullif" ? Math.min(1, args.length) : args.length;
	for (let i = 0; i < length; i++) {
		for (const type of args[i].types) {
			if (type !== "missing") result.add(type);
		}
	}
	return result;
}

function hasValidKeyLiterals(left: ValueNode, right: ValueNode): boolean {
	const leftIsKey = left.directReference === "hashKey" || left.directReference === "sortKey";
	const rightIsKey = right.directReference === "hashKey" || right.directReference === "sortKey";
	return (!leftIsKey || right.literal !== "") && (!rightIsKey || left.literal !== "");
}

function hasValidInKeyLiterals(args: readonly ValueNode[]): boolean {
	for (let i = 1; i < args.length; i++) {
		if (!hasValidKeyLiterals(args[0], args[i])) return false;
	}
	return true;
}

function areCompatible(left: ValueNode, right: ValueNode, allowed: ReadonlySet<ExpressionNativeType>): boolean {
	for (const type of allowed) {
		if (left.types.has(type) && right.types.has(type)) return true;
	}
	return false;
}

function hasCompatibleInTypes(args: readonly ValueNode[]): boolean {
	for (let i = 1; i < args.length; i++) {
		if (!areCompatible(args[0], args[i], equalityTypes)) return false;
	}
	return true;
}

function hasCommonType3(first: ValueNode, second: ValueNode, third: ValueNode, allowed: ReadonlySet<ExpressionNativeType>): boolean {
	for (const type of allowed) {
		if (first.types.has(type) && second.types.has(type) && third.types.has(type)) return true;
	}
	return false;
}

function hasAnyType(actual: ReadonlySet<ExpressionNativeType>, accepted: ReadonlySet<ExpressionNativeType>): boolean {
	for (const type of accepted) {
		if (actual.has(type)) return true;
	}
	return false;
}

function canContain(container: ReadonlySet<ExpressionNativeType>, search: ReadonlySet<ExpressionNativeType>): boolean {
	if (container.has("text") && search.has("text")) return true;
	if (container.has("bytes") && search.has("bytes")) return true;
	if (container.has("array") && hasAnyType(search, arraySearchTypes)) return true;
	return false;
}
