import type { JsonPrimitive } from "../json-types.js";
import { decodeByteLiteral } from "./byte-literal.js";
import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { validateScalarLiteral } from "./literal.js";
import {
	type ExpressionContext,
	type ValueFacts,
	EXPRESSION_CONTEXT_ALL,
	arraySearchTypes,
	booleanValue,
	byteLiteralValue,
	dataPathValue,
	dataValue,
	emptyTextValue,
	equalityTypes,
	getOperationDefinition,
	hashKeyValue,
	matchesContext,
	nullValue,
	numberValue,
	orderedTypes,
	prefixTypes,
	sortKeyValue,
	textValue,
	ttlValue,
	versionValue,
} from "./operation-registry.js";
import { validateReadJsonPath } from "./path.js";
import { EXPRESSION_NATIVE_TYPES, type ExpressionNativeType } from "./types.js";

export const EXPRESSION_REQUIRED_COLUMNS = ["hk", "sk", "v", "ttl_epoch_utc_seconds", "data_kind", "data"] as const;

export type ExpressionRequiredColumn = (typeof EXPRESSION_REQUIRED_COLUMNS)[number];

export type ExpressionValueAnalysis = {
	nativeTypes: readonly ExpressionNativeType[];
	requiredColumns: readonly ExpressionRequiredColumn[];
};

export type ConditionExpressionAnalysis = {
	requiredColumns: readonly ExpressionRequiredColumn[];
};

type AnalysisContext = {
	operatorsAndFunctions: number;
	requiredColumns: Set<ExpressionRequiredColumn>;
	expressionContext: ExpressionContext;
};

export function analyzeExpressionValue(expression: unknown, expressionContext: ExpressionContext = "condition"): ExpressionValueAnalysis {
	const context = createContext(expressionContext);
	const facts = analyzeValue(expression, 1, context);
	return { nativeTypes: orderedTypesFrom(facts.types), requiredColumns: requiredColumnsFrom(context) };
}

export function validateConditionExpression(expression: unknown): ConditionExpressionAnalysis {
	const context = createContext("condition");
	analyzeCondition(expression, 1, context);
	return { requiredColumns: requiredColumnsFrom(context) };
}

function createContext(expressionContext: ExpressionContext = "condition"): AnalysisContext {
	return { operatorsAndFunctions: 0, requiredColumns: new Set(), expressionContext };
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
			let byteChoice = false;
			let scalarChoice = false;
			for (let i = 1; i < args.length; i++) {
				const choice = analyzeValue(args[i], depth + 1, context);
				assertNoEmptyKeyLiteral(target, choice);
				assertCompatible(target, choice, equalityTypes);
				if (choice.byteLiteral) byteChoice = true;
				else if (Object.hasOwn(args[i] as object, "val")) scalarChoice = true;
			}
			if (byteChoice && scalarChoice) throw new ExpressionError("invalid_type", "in choices must not mix byte and scalar literals");
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
	if (Object.hasOwn(expression, "b64")) {
		assertFields(expression, "b64");
		decodeByteLiteral(expression as { b64: unknown });
		return byteLiteralValue;
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
		case "ttlAt":
			if (hasPath) throw new ExpressionError("invalid_ast", "only data references can have a path");
			context.requiredColumns.add("ttl_epoch_utc_seconds");
			return ttlValue;
		case "data":
			context.requiredColumns.add("data_kind");
			context.requiredColumns.add("data");
			if (!hasPath) return dataValue;
			validateReadJsonPath(expression.path);
			return dataPathValue;
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

	const operation = getOperationDefinition(expression.fn);
	if (!operation || !matchesContext(operation.contexts ?? EXPRESSION_CONTEXT_ALL, context.expressionContext)) {
		if (expression.fn.startsWith("sqlite.")) {
			throw new ExpressionError("invalid_function", "SQLite function is not allowed");
		}
		throw new ExpressionError("invalid_function", "unknown expression function");
	}

	if (operation.name.startsWith("sqlite.") && args.length > EXPRESSION_LIMITS.sqliteFunctionArguments) {
		throw new ExpressionError("complexity_limit", "SQLite function argument limit exceeded");
	}
	assertArity(args, operation.arity[0], operation.arity[1]);

	const argFacts: ValueFacts[] = [];
	for (let i = 0; i < args.length; i++) {
		argFacts.push(analyzeValue(args[i], depth + 1, context));
	}

	return operation.typeRule(argFacts, args);
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
