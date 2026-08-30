import type { JsonPrimitive } from "../json-types.js";
import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { validateScalarLiteral } from "./literal.js";
import { validateReadJsonPath } from "./path.js";
import { SQLITE_FUNCTION_ARITY, SQLITE_MATH_FUNCTIONS } from "./sqlite-functions.js";
import { EXPRESSION_NATIVE_TYPES, type ExpressionNativeType } from "./types.js";
import { utf8WithinLimit } from "./utf8.js";

export const EXPRESSION_REQUIRED_COLUMNS = ["hk", "sk", "v", "ttl_epoch_utc_seconds", "data_kind", "data"] as const;

export type ExpressionRequiredColumn = (typeof EXPRESSION_REQUIRED_COLUMNS)[number];

export type ExpressionValueAnalysis = {
	nativeTypes: readonly ExpressionNativeType[];
	requiredColumns: readonly ExpressionRequiredColumn[];
};

export type ConditionExpressionAnalysis = {
	requiredColumns: readonly ExpressionRequiredColumn[];
};

type DirectReference = "hashKey" | "sortKey" | "v" | "ttl" | "data";

type ValueFacts = {
	types: ReadonlySet<ExpressionNativeType>;
	directReference?: DirectReference;
	emptyStringLiteral?: true;
};

type AnalysisContext = {
	operatorsAndFunctions: number;
	requiredColumns: Set<ExpressionRequiredColumn>;
};

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
const nullValue: ValueFacts = { types: nullTypes };
const booleanValue: ValueFacts = { types: booleanTypes };
const numberValue: ValueFacts = { types: numberTypes };
const textValue: ValueFacts = { types: textTypes };
const emptyTextValue: ValueFacts = { types: textTypes, emptyStringLiteral: true };
const missingNumberValue: ValueFacts = { types: missingNumberTypes };
const nullNumberValue: ValueFacts = { types: nullNumberTypes };
const nullTextValue: ValueFacts = { types: nullTextTypes };
const nullBytesValue: ValueFacts = { types: nullBytesTypes };
const hashKeyValue: ValueFacts = { types: missingTextBytesTypes, directReference: "hashKey" };
const sortKeyValue: ValueFacts = { types: missingTextBytesTypes, directReference: "sortKey" };
const versionValue: ValueFacts = { types: missingNumberTypes, directReference: "v" };
const ttlValue: ValueFacts = { types: missingNumberTypes, directReference: "ttl" };
const dataValue: ValueFacts = { types: allTypes, directReference: "data" };
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

export function analyzeExpressionValue(expression: unknown): ExpressionValueAnalysis {
	const context = createContext();
	const facts = analyzeValue(expression, 1, context);
	return { nativeTypes: orderedTypesFrom(facts.types), requiredColumns: requiredColumnsFrom(context) };
}

export function validateConditionExpression(expression: unknown): ConditionExpressionAnalysis {
	const context = createContext();
	analyzeCondition(expression, 1, context);
	return { requiredColumns: requiredColumnsFrom(context) };
}

function createContext(): AnalysisContext {
	return { operatorsAndFunctions: 0, requiredColumns: new Set() };
}

function requiredColumnsFrom(context: AnalysisContext): readonly ExpressionRequiredColumn[] {
	return EXPRESSION_REQUIRED_COLUMNS.filter((column) => context.requiredColumns.has(column));
}

function orderedTypesFrom(types: ReadonlySet<ExpressionNativeType>): readonly ExpressionNativeType[] {
	return EXPRESSION_NATIVE_TYPES.filter((type) => types.has(type));
}

function assertDepth(depth: number): void {
	if (depth > EXPRESSION_LIMITS.astDepth) throw new ExpressionError("complexity_limit", "AST depth exceeds the limit");
}

function countOperation(context: AnalysisContext): void {
	context.operatorsAndFunctions++;
	if (context.operatorsAndFunctions > EXPRESSION_LIMITS.operatorsAndFunctions) {
		throw new ExpressionError("complexity_limit", "operator and function limit exceeded");
	}
}

function assertNode(value: unknown, message: string): asserts value is Record<string, unknown> {
	if (typeof value !== "object" || value === null || Array.isArray(value)) throw new ExpressionError("invalid_ast", message);
}

function assertFields(record: Record<string, unknown>, required1: string, required2?: string, optional?: string): void {
	if (!Object.hasOwn(record, required1) || (required2 !== undefined && !Object.hasOwn(record, required2))) {
		throw new ExpressionError("invalid_ast", "invalid expression fields");
	}
	for (const field in record) {
		if (Object.hasOwn(record, field) && field !== required1 && field !== required2 && field !== optional) {
			throw new ExpressionError("invalid_ast", "invalid expression fields");
		}
	}
}

function assertArgs(value: unknown): asserts value is readonly unknown[] {
	if (!Array.isArray(value)) throw new ExpressionError("invalid_ast", "expression args must be an array");
}

function assertArity(args: readonly unknown[], minimum: number, maximum = minimum): void {
	if (args.length < minimum || args.length > maximum) throw new ExpressionError("invalid_arity", "invalid expression argument count");
}

function analyzeCondition(expression: unknown, depth: number, context: AnalysisContext): void {
	assertDepth(depth);
	assertNode(expression, "invalid condition expression");
	assertFields(expression, "op", "args");
	if (typeof expression.op !== "string") throw new ExpressionError("invalid_ast", "invalid condition operator");
	assertArgs(expression.args);
	const args = expression.args;
	countOperation(context);

	switch (expression.op) {
		case "eq":
		case "ne":
		case "lt":
		case "lte":
		case "gt":
		case "gte": {
			assertArity(args, 2);
			const left = analyzeValue(args[0], depth + 1, context);
			const right = analyzeValue(args[1], depth + 1, context);
			assertNoEmptyKeyLiteral(left, right);
			assertCompatible(left, right, expression.op === "eq" || expression.op === "ne" ? equalityTypes : orderedTypes);
			return;
		}
		case "between": {
			assertArity(args, 3);
			const value = analyzeValue(args[0], depth + 1, context);
			const lower = analyzeValue(args[1], depth + 1, context);
			const upper = analyzeValue(args[2], depth + 1, context);
			assertNoEmptyKeyLiteral(value, lower);
			assertNoEmptyKeyLiteral(value, upper);
			if (!hasCommonType3(value, lower, upper, orderedTypes)) throw new ExpressionError("invalid_type", "incompatible expression types");
			return;
		}
		case "in": {
			if (args.length < 2) throw new ExpressionError("invalid_arity", "invalid expression argument count");
			if (args.length - 1 > EXPRESSION_LIMITS.inChoices) throw new ExpressionError("complexity_limit", "in choice limit exceeded");
			const target = analyzeValue(args[0], depth + 1, context);
			for (let i = 1; i < args.length; i++) {
				const choice = analyzeValue(args[i], depth + 1, context);
				assertNoEmptyKeyLiteral(target, choice);
				assertCompatible(target, choice, equalityTypes);
			}
			return;
		}
		case "and":
		case "or":
			assertArity(args, 2, Number.POSITIVE_INFINITY);
			for (const arg of args) analyzeCondition(arg, depth + 1, context);
			return;
		case "not":
			assertArity(args, 1);
			analyzeCondition(args[0], depth + 1, context);
			return;
		case "exists":
		case "not_exists":
			assertArity(args, 1);
			analyzeReference(args[0], depth + 1, context);
			return;
		case "begins_with": {
			assertArity(args, 2);
			const left = analyzeValue(args[0], depth + 1, context);
			const right = analyzeValue(args[1], depth + 1, context);
			assertNoEmptyKeyLiteral(left, right);
			assertCompatible(left, right, prefixTypes);
			return;
		}
		case "contains": {
			assertArity(args, 2);
			const container = analyzeValue(args[0], depth + 1, context);
			const search = analyzeValue(args[1], depth + 1, context);
			assertNoEmptyKeyLiteral(container, search);
			if (!canContain(container.types, search.types)) throw new ExpressionError("invalid_type", "incompatible contains types");
			return;
		}
		default:
			throw new ExpressionError("invalid_ast", "unknown condition operator");
	}
}

function analyzeValue(expression: unknown, depth: number, context: AnalysisContext): ValueFacts {
	assertDepth(depth);
	assertNode(expression, "invalid expression value");
	if (Object.hasOwn(expression, "val")) {
		assertFields(expression, "val");
		return literalFacts(validateScalarLiteral(expression.val));
	}
	if (Object.hasOwn(expression, "ref")) return analyzeReferenceNode(expression, context);
	if (Object.hasOwn(expression, "fn")) return analyzeFunction(expression, depth, context);
	throw new ExpressionError("invalid_ast", "invalid expression value");
}

function analyzeReference(expression: unknown, depth: number, context: AnalysisContext): ValueFacts {
	assertDepth(depth);
	assertNode(expression, "invalid expression reference");
	return analyzeReferenceNode(expression, context);
}

function analyzeReferenceNode(expression: Record<string, unknown>, context: AnalysisContext): ValueFacts {
	assertFields(expression, "ref", undefined, "path");
	if (typeof expression.ref !== "string") throw new ExpressionError("invalid_ast", "invalid expression reference");
	const hasPath = Object.hasOwn(expression, "path");

	switch (expression.ref) {
		case "hashKey":
			if (hasPath) throw new ExpressionError("invalid_ast", "only data references can have a path");
			context.requiredColumns.add("hk");
			return hashKeyValue;
		case "sortKey":
			if (hasPath) throw new ExpressionError("invalid_ast", "only data references can have a path");
			context.requiredColumns.add("sk");
			return sortKeyValue;
		case "v":
			if (hasPath) throw new ExpressionError("invalid_ast", "only data references can have a path");
			context.requiredColumns.add("v");
			return versionValue;
		case "ttl":
			if (hasPath) throw new ExpressionError("invalid_ast", "only data references can have a path");
			context.requiredColumns.add("ttl_epoch_utc_seconds");
			return ttlValue;
		case "data":
			context.requiredColumns.add("data_kind");
			context.requiredColumns.add("data");
			if (hasPath) validateReadJsonPath(expression.path);
			return dataValue;
		default:
			throw new ExpressionError("invalid_ast", "unknown expression reference");
	}
}

function analyzeFunction(expression: Record<string, unknown>, depth: number, context: AnalysisContext): ValueFacts {
	assertFields(expression, "fn", "args");
	if (typeof expression.fn !== "string") throw new ExpressionError("invalid_ast", "invalid function name");
	assertArgs(expression.args);
	const args = expression.args;
	countOperation(context);

	if (expression.fn === "size" || expression.fn === "attribute_type") {
		assertArity(args, 1);
		const input = analyzeValue(args[0], depth + 1, context);
		if (expression.fn === "attribute_type") return textValue;
		if (!hasAnyType(input.types, sizeInputTypes)) throw new ExpressionError("invalid_type", "invalid size input type");
		for (const type of input.types) {
			if (!sizeInputTypes.has(type)) return missingNumberValue;
		}
		return numberValue;
	}

	if (!expression.fn.startsWith("sqlite.")) throw new ExpressionError("invalid_function", "unknown expression function");
	const name = expression.fn.slice("sqlite.".length);
	const arity = SQLITE_FUNCTION_ARITY.get(name);
	if (arity === undefined) throw new ExpressionError("invalid_function", "SQLite function is not allowed");
	if (args.length > EXPRESSION_LIMITS.sqliteFunctionArguments) {
		throw new ExpressionError("complexity_limit", "SQLite function argument limit exceeded");
	}
	assertArity(args, arity[0], arity[1]);

	const fixedResult = fixedSqliteResult(name);
	let dynamicTypes: Set<ExpressionNativeType> | undefined;
	for (let i = 0; i < args.length; i++) {
		const argument = analyzeValue(args[i], depth + 1, context);
		if (fixedResult === undefined && (name !== "nullif" || i === 0)) {
			dynamicTypes ??= new Set(["null"]);
			for (const type of argument.types) {
				if (type !== "missing") dynamicTypes.add(type);
			}
		}
	}
	if (name === "glob" || name === "like") validatePatternArguments(name, args);
	return fixedResult ?? { types: dynamicTypes ?? nullTypes };
}

function validatePatternArguments(name: string, args: readonly unknown[]): void {
	const pattern = stringLiteral(args[0]);
	if (pattern === undefined) throw new ExpressionError("invalid_type", `${name} pattern must be a string literal`);
	if (!utf8WithinLimit(pattern, EXPRESSION_LIMITS.sqlitePatternBytes)) {
		throw new ExpressionError("complexity_limit", "SQLite pattern limit exceeded");
	}
	if (name === "like" && args.length === 3 && stringLiteral(args[2]) === undefined) {
		throw new ExpressionError("invalid_type", "like escape must be a string literal");
	}
}

function stringLiteral(value: unknown): string | undefined {
	if (typeof value !== "object" || value === null || Array.isArray(value)) return;
	const literal = (value as { val?: unknown }).val;
	return typeof literal === "string" ? literal : undefined;
}

function fixedSqliteResult(name: string): ValueFacts | undefined {
	if (sqliteNumberFunctions.has(name)) return nullNumberValue;
	if (sqliteTextFunctions.has(name)) return nullTextValue;
	if (name === "unhex") return nullBytesValue;
}

function literalFacts(value: JsonPrimitive): ValueFacts {
	if (value === null) return nullValue;
	if (typeof value === "boolean") return booleanValue;
	if (typeof value === "number") return numberValue;
	return value.length === 0 ? emptyTextValue : textValue;
}

function assertNoEmptyKeyLiteral(left: ValueFacts, right: ValueFacts): void {
	if ((isKeyReference(left) && right.emptyStringLiteral) || (isKeyReference(right) && left.emptyStringLiteral)) {
		throw new ExpressionError("invalid_literal", "empty key literal is not allowed");
	}
}

function isKeyReference(value: ValueFacts): boolean {
	return value.directReference === "hashKey" || value.directReference === "sortKey";
}

function assertCompatible(left: ValueFacts, right: ValueFacts, allowed: ReadonlySet<ExpressionNativeType>): void {
	for (const type of allowed) {
		if (left.types.has(type) && right.types.has(type)) return;
	}
	throw new ExpressionError("invalid_type", "incompatible expression types");
}

function hasCommonType3(first: ValueFacts, second: ValueFacts, third: ValueFacts, allowed: ReadonlySet<ExpressionNativeType>): boolean {
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
