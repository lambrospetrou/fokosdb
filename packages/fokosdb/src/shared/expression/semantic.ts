import type { JsonPrimitive } from "../json-types.js";
import { getActionDefinition } from "./action-registry.js";
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
import { type PathSegment, isParentPath, pathsEqual, validateReadJsonPath, validateWriteJsonPath } from "./path.js";
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

export type UpdateExpressionAnalysis = {
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

export function validateUpdateExpression(expression: unknown): UpdateExpressionAnalysis {
	const context = createContext("update-value");
	analyzeUpdate(expression, context);
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

// The types a JSON document can hold. `null` and `missing` are absent on purpose: neither is a value
// that decides whether an expression can produce a document member, and `null` is written as JSON null.
const JSON_VALUE_TYPES: readonly ExpressionNativeType[] = ["boolean", "number", "text", "array", "object"];

/**
 * Refuses a `set` value that a JSON document can never hold.
 *
 * JSON has no byte type. A value whose only non-null outcome is bytes — a byte literal, or `unhex` —
 * can never write a valid document, so it fails here, at compile time, with a message the caller can
 * act on. A value that is bytes for SOME items only — a key reference, which is text for a text key
 * and bytes for a binary one — passes here, and the compiler carries a per-item type test into
 * `applicableSql` instead.
 *
 * A byte literal is still a valid ARGUMENT: `size` over one is a number, and that is a valid value.
 * The test is therefore over the type of the whole value, never over the nodes inside it.
 */
function assertUpdateValueTypes(facts: ValueFacts): void {
	if (facts.types.has("bytes") && !JSON_VALUE_TYPES.some((type) => facts.types.has(type))) {
		throw new ExpressionError("invalid_type", "an update value must not be bytes");
	}
}

function analyzeUpdate(expression: unknown, context: AnalysisContext): void {
	if (!Array.isArray(expression) || expression.length === 0) {
		throw new ExpressionError("invalid_ast", "update expression must contain at least one action");
	}
	if (expression.length > EXPRESSION_LIMITS.updateActions) {
		throw new ExpressionError("complexity_limit", "update action limit exceeded");
	}

	context.requiredColumns.add("data_kind");
	context.requiredColumns.add("data");

	type ParsedTarget = {
		action: string;
		segments: readonly PathSegment[];
	};
	const targets: ParsedTarget[] = [];

	for (const item of expression) {
		assertNode(item, "invalid update action");
		if (typeof item.action !== "string") {
			throw new ExpressionError("invalid_ast", "invalid update action");
		}
		const actionDef = getActionDefinition(item.action);
		if (!actionDef) {
			throw new ExpressionError("invalid_ast", "unknown update action");
		}
		if (actionDef.hasValue) {
			if (!Object.hasOwn(item, "value")) {
				throw new ExpressionError("invalid_ast", "set action requires value");
			}
			assertFields(item, "action", "target", "value");
		} else {
			assertFields(item, "action", "target");
		}
		assertNode(item.target, "invalid update target");
		assertFields(item.target, "ref", "path");
		if (item.target.ref !== "data" || typeof item.target.path !== "string") {
			throw new ExpressionError("invalid_ast", "invalid update target");
		}
		const segments = validateWriteJsonPath(item.target.path, { allowAppend: actionDef.allowAppend });
		if (segments.length === 0) {
			throw new ExpressionError("invalid_path", "target path must not be root");
		}
		targets.push({ action: item.action, segments });

		if (actionDef.hasValue) {
			assertUpdateValueTypes(analyzeValue(item.value, 1, context));
		}
	}

	for (let i = 0; i < targets.length; i++) {
		for (let j = i + 1; j < targets.length; j++) {
			if (pathsEqual(targets[i].segments, targets[j].segments)) {
				throw new ExpressionError("invalid_path", "duplicate target path");
			}
			if (isParentPath(targets[i].segments, targets[j].segments) || isParentPath(targets[j].segments, targets[i].segments)) {
				throw new ExpressionError("invalid_path", "overlapping target paths");
			}
		}
	}

	const removalsByParent: { parent: readonly PathSegment[]; hasPlain: boolean; hasReverse: boolean }[] = [];
	for (const t of targets) {
		if (t.action !== "remove") continue;
		const last = t.segments[t.segments.length - 1];
		if (last.kind !== "index" && last.kind !== "reverseIndex") continue;
		const parent = t.segments.slice(0, -1);
		let entry = removalsByParent.find((e) => pathsEqual(e.parent, parent));
		if (!entry) {
			entry = { parent, hasPlain: false, hasReverse: false };
			removalsByParent.push(entry);
		}
		if (last.kind === "index") entry.hasPlain = true;
		if (last.kind === "reverseIndex") entry.hasReverse = true;
		if (entry.hasPlain && entry.hasReverse) {
			throw new ExpressionError("invalid_path", "cannot mix plain and reverse index removals under the same parent");
		}
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
