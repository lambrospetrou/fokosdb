import type { JsonPrimitive } from "../json-types.js";
import { decodeByteLiteral } from "./byte-literal.js";
import { ExpressionError } from "./errors.js";
import { canonicalConditionIdentity } from "./identity.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import {
	type ExpressionContext,
	type OperationRenderers,
	EXPRESSION_CONTEXT_ALL,
	getOperationDefinition,
	matchesContext,
} from "./operation-registry.js";
import {
	composeConditionStatement,
	CONDITION_FIXED_BINDING_COUNT,
	CONDITION_PLAN_VERSION,
	type CompiledConditionPlan,
	type ExpressionBindingDescriptor,
} from "./plan.js";
import { validateConditionExpression } from "./semantic.js";
import type { ConditionExpression, ExpressionReference, ExpressionValue } from "./types.js";
import { utf8WithinLimit } from "./utf8.js";

type CompileContext = {
	bindings: ExpressionBindingDescriptor[];
	bindingIndexByKey: Map<string, number>;
	paramOffset: number;
	completeData: boolean;
	paths: Set<string>;
	expressionContext: ExpressionContext;
};

type ValueMode = "logical" | "key" | "sqlite";

const BYTES_KIND = 0;
const TEXT_KIND = 1;
const JSON_KIND = 2;
const EQUALITY_TYPE_NAMES: readonly string[] = ["null", "boolean", "number", "text", "bytes"];
const ORDERED_TYPE_NAMES: readonly string[] = ["number", "text", "bytes"];
const PREFIX_TYPE_NAMES: readonly string[] = ["text", "bytes"];
const ARRAY_SEARCH_TYPE_NAMES: readonly string[] = ["null", "boolean", "number", "text"];
const JSON_EACH_TYPE_SQL =
	"CASE je.type WHEN 'null' THEN 'null' WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number' WHEN 'text' THEN 'text' ELSE 'missing' END";

export function compileConditionExpression(
	condition: ConditionExpression,
	fixedBindingCount = CONDITION_FIXED_BINDING_COUNT,
): CompiledConditionPlan {
	const analysis = validateConditionExpression(condition);
	const context: CompileContext = {
		bindings: [],
		bindingIndexByKey: new Map(),
		paramOffset: fixedBindingCount,
		completeData: false,
		paths: new Set(),
		expressionContext: "condition",
	};
	const sql = compactParameters(compileCondition(condition, context), context);
	if (!utf8WithinLimit(composeConditionStatement(sql), EXPRESSION_LIMITS.compiledSqlBytes)) {
		throw new ExpressionError("sql_limit", "compiled SQL exceeds the SQL limit");
	}
	const completeBindingCount = fixedBindingCount + context.bindings.length;
	if (completeBindingCount > EXPRESSION_LIMITS.completeStatementBindings) {
		throw new ExpressionError("sql_limit", "complete statement exceeds the binding limit");
	}
	return {
		version: CONDITION_PLAN_VERSION,
		kind: "condition",
		sql,
		bindings: context.bindings,
		bindingCount: context.bindings.length,
		completeBindingCount,
		requiredColumns: analysis.requiredColumns,
		dataDependencies: { completeData: context.completeData, paths: [...context.paths] },
		result: { nativeTypes: ["boolean"], canBeMissing: false },
		identity: canonicalConditionIdentity(condition),
	};
}

/**
 * Removes bindings whose numbered parameter no longer appears in the SQL. Constant folding can
 * discard a rendered fragment after its binding was registered; Workers SQLite requires the
 * provided binding count to match the statement parameter count, so survivors are renumbered
 * densely and in their original order.
 */
function compactParameters(sql: string, context: CompileContext): string {
	const used = new Set<number>();
	for (const match of sql.matchAll(/\?(\d+)/g)) used.add(Number(match[1]));
	if (used.size === context.bindings.length) return sql;
	const remap = new Map<number, number>();
	const survivors: ExpressionBindingDescriptor[] = [];
	for (const index of [...used].sort((a, b) => a - b)) {
		survivors.push(context.bindings[index - context.paramOffset - 1]);
		remap.set(index, context.paramOffset + survivors.length);
	}
	context.bindings = survivors;
	return sql.replace(/\?(\d+)/g, (_, digits: string) => `?${remap.get(Number(digits))}`);
}

function compileCondition(condition: ConditionExpression, context: CompileContext): string {
	switch (condition.op) {
		case "eq":
		case "ne":
		case "lt":
		case "lte":
		case "gt":
		case "gte":
			return compileComparison(condition.op, condition.args[0], condition.args[1], context);
		case "between":
			return `(${compileComparison("gte", condition.args[0], condition.args[1], context)} AND ${compileComparison("lte", condition.args[0], condition.args[2], context)})`;
		case "in":
			return compileIn(condition.args, context);
		case "and":
		case "or": {
			const operator = condition.op === "and" ? " AND " : " OR ";
			return `(${condition.args.map((arg) => compileCondition(arg, context)).join(operator)})`;
		}
		case "not":
			return `(NOT ${compileCondition(condition.args[0], context)})`;
		case "exists":
			return `(${renderPresent(condition.args[0], context)})`;
		case "not_exists":
			return `(NOT ${renderPresent(condition.args[0], context)})`;
		case "begins_with":
			return compileBeginsWith(condition.args[0], condition.args[1], context);
		case "contains":
			return compileContains(condition.args[0], condition.args[1], context);
	}
}

/** Appends one AND term; drops constant-true terms and reports a constant-false term. */
function addTerm(terms: string[], term: string): boolean {
	if (term === "0") return false;
	if (term !== "1") terms.push(term);
	return true;
}

/** Returns the native type name when the rendered type SQL is a compile-time constant like `'text'`. */
function constTypeName(typeSql: string): string | undefined {
	return typeSql.startsWith("'") ? typeSql.slice(1, -1) : undefined;
}

function typeListSql(types: readonly string[]): string {
	return types.map((type) => `'${type}'`).join(", ");
}

/**
 * Appends the type guards for one comparison: both sides must have the same native type and that
 * type must be one of `allowed`. Guards that are decidable at compile time are folded away.
 * Returns false when the guards can never pass.
 */
function pushTypeGuards(terms: string[], leftType: string, rightType: string, allowed: readonly string[]): boolean {
	const leftConst = constTypeName(leftType);
	const rightConst = constTypeName(rightType);
	if (leftConst !== undefined && rightConst !== undefined) {
		if (leftConst !== rightConst) return false;
	} else {
		terms.push(`${leftType} = ${rightType}`);
	}
	const knownType = leftConst ?? rightConst;
	if (knownType !== undefined) return allowed.includes(knownType);
	terms.push(`${leftType} IN (${typeListSql(allowed)})`);
	return true;
}

function compileComparison(
	op: "eq" | "ne" | "lt" | "lte" | "gt" | "gte",
	left: ExpressionValue,
	right: ExpressionValue,
	context: CompileContext,
): string {
	const [leftMode, rightMode] = comparisonModes(left, right);
	const terms: string[] = [];
	if (!addTerm(terms, renderPresent(left, context))) return "(0)";
	if (!addTerm(terms, renderPresent(right, context))) return "(0)";
	const leftType = renderType(left, context);
	const rightType = renderType(right, context);
	const allowedTypes = op === "eq" || op === "ne" ? EQUALITY_TYPE_NAMES : ORDERED_TYPE_NAMES;
	if (!pushTypeGuards(terms, leftType, rightType, allowedTypes)) return "(0)";
	const leftValue = renderValue(left, leftMode, context);
	const rightValue = renderValue(right, rightMode, context);
	const comparison =
		op === "eq"
			? `(${leftValue} IS ${rightValue})`
			: op === "ne"
				? `(NOT (${leftValue} IS ${rightValue}))`
				: `${leftValue} ${comparisonOperator(op)} ${rightValue}`;
	terms.push(comparison);
	return `(${terms.join(" AND ")})`;
}

function comparisonOperator(op: "lt" | "lte" | "gt" | "gte"): string {
	switch (op) {
		case "lt":
			return "<";
		case "lte":
			return "<=";
		case "gt":
			return ">";
		case "gte":
			return ">=";
	}
}

function compileIn(args: readonly ExpressionValue[], context: CompileContext): string {
	const target = args[0];
	const choices = args.slice(1);
	const firstType = literalNativeType(choices[0]);
	if (firstType !== undefined && firstType !== "null" && choices.every((choice) => literalNativeType(choice) === firstType)) {
		const mode: ValueMode = isDirectKeyReference(target) && (firstType === "text" || firstType === "bytes") ? "key" : "logical";
		const terms: string[] = [];
		if (!addTerm(terms, renderPresent(target, context))) return "(0)";
		const type = renderType(target, context);
		const typeConst = constTypeName(type);
		if (typeConst !== undefined) {
			if (typeConst !== firstType) return "(0)";
		} else {
			terms.push(`${type} = '${firstType}'`);
		}
		const choiceSql = choices.map((choice) => renderValue(choice, mode, context)).join(", ");
		terms.push(`${renderValue(target, mode, context)} IN (${choiceSql})`);
		return `(${terms.join(" AND ")})`;
	}
	return `(${choices.map((choice) => compileComparison("eq", target, choice, context)).join(" OR ")})`;
}

function compileBeginsWith(value: ExpressionValue, prefix: ExpressionValue, context: CompileContext): string {
	const [valueMode, prefixMode] = comparisonModes(value, prefix);
	const terms: string[] = [];
	if (!addTerm(terms, renderPresent(value, context))) return "(0)";
	if (!addTerm(terms, renderPresent(prefix, context))) return "(0)";
	const valueType = renderType(value, context);
	const prefixType = renderType(prefix, context);
	if (!pushTypeGuards(terms, valueType, prefixType, PREFIX_TYPE_NAMES)) return "(0)";
	const valueSql = renderValue(value, valueMode, context);
	const prefixSql = renderValue(prefix, prefixMode, context);
	terms.push(`substr(${valueSql}, 1, length(${prefixSql})) IS ${prefixSql}`);
	return `(${terms.join(" AND ")})`;
}

/**
 * Containment always compares logical values, never canonical key bytes: the `0xFF` binary-key tag
 * must not become part of the searched content, or a subsequence search on a key would only match at
 * the start. The logical key type guard still keeps text keys and binary keys apart.
 */
function compileContains(container: ExpressionValue, search: ExpressionValue, context: CompileContext): string {
	const containerPresent = renderPresent(container, context);
	const searchPresent = renderPresent(search, context);
	const containerType = renderType(container, context);
	const searchType = renderType(search, context);

	let scalar = "0";
	const scalarTerms: string[] = [];
	if (
		addTerm(scalarTerms, containerPresent) &&
		addTerm(scalarTerms, searchPresent) &&
		pushTypeGuards(scalarTerms, containerType, searchType, PREFIX_TYPE_NAMES)
	) {
		scalarTerms.push(`instr(${renderValue(container, "logical", context)}, ${renderValue(search, "logical", context)}) > 0`);
		scalar = `(${scalarTerms.join(" AND ")})`;
	}

	let array = "0";
	const arrayGuards: string[] = [];
	let arrayPossible = addTerm(arrayGuards, containerPresent) && addTerm(arrayGuards, searchPresent);
	const containerConst = constTypeName(containerType);
	if (arrayPossible && containerConst !== undefined) arrayPossible = containerConst === "array";
	else if (arrayPossible) arrayGuards.push(`${containerType} = 'array'`);
	const searchConst = constTypeName(searchType);
	if (arrayPossible && searchConst !== undefined) arrayPossible = ARRAY_SEARCH_TYPE_NAMES.includes(searchConst);
	else if (arrayPossible) arrayGuards.push(`${searchType} IN (${typeListSql(ARRAY_SEARCH_TYPE_NAMES)})`);
	if (arrayPossible) {
		const exists = `EXISTS (SELECT 1 FROM json_each(${renderValue(container, "logical", context)}) AS je WHERE ${JSON_EACH_TYPE_SQL} = ${searchType} AND je.value IS ${renderValue(search, "logical", context)})`;
		array = arrayGuards.length > 0 ? `(CASE WHEN ${arrayGuards.join(" AND ")} THEN ${exists} ELSE 0 END)` : `(${exists})`;
	}

	if (scalar === "0" && array === "0") return "(0)";
	if (array === "0") return scalar;
	if (scalar === "0") return array;
	return `(${scalar} OR ${array})`;
}

function makeRenderers(context: CompileContext): OperationRenderers {
	return {
		renderValue: (val, mode) => renderValue(val, mode, context),
		renderType: (val) => renderType(val, context),
		renderPresent: (val) => renderPresent(val, context),
		renderSize: (val) => renderSize(val, context),
	};
}

function renderPresent(value: ExpressionValue, context: CompileContext): string {
	if ("val" in value) return "1";
	if ("b64" in value) return "1";
	if ("ref" in value) return referencePresent(value, context);
	const operation = getOperationDefinition(value.fn);
	if (operation && matchesContext(operation.contexts ?? EXPRESSION_CONTEXT_ALL, context.expressionContext)) {
		if (operation.renderPresent) return operation.renderPresent(value.args, makeRenderers(context));
		return "1";
	}
	throw new ExpressionError("invalid_function", "unknown expression function");
}

function renderType(value: ExpressionValue, context: CompileContext): string {
	if ("val" in value) return `'${literalType(value.val as JsonPrimitive)}'`;
	if ("b64" in value) return "'bytes'";
	if ("ref" in value) return referenceType(value, context);
	const operation = getOperationDefinition(value.fn);
	if (operation && matchesContext(operation.contexts ?? EXPRESSION_CONTEXT_ALL, context.expressionContext)) {
		if (operation.renderType) return operation.renderType(value.args, makeRenderers(context));
		const call = `${operation.name.slice("sqlite.".length)}(${value.args.map((arg) => renderValue(arg, "sqlite", context)).join(", ")})`;
		return `CASE typeof(${call}) WHEN 'null' THEN 'null' WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number' WHEN 'text' THEN 'text' WHEN 'blob' THEN 'bytes' ELSE 'missing' END`;
	}
	throw new ExpressionError("invalid_function", "unknown expression function");
}

function renderValue(value: ExpressionValue, mode: ValueMode, context: CompileContext): string {
	if ("val" in value) return bindLiteral(value.val as JsonPrimitive, mode, context);
	if ("b64" in value) return bindByteLiteral(value, mode, context);
	if ("ref" in value) return referenceValue(value, mode, context);
	const operation = getOperationDefinition(value.fn);
	if (operation && matchesContext(operation.contexts ?? EXPRESSION_CONTEXT_ALL, context.expressionContext)) {
		return operation.renderValue(value.args, makeRenderers(context));
	}
	throw new ExpressionError("invalid_function", "unknown expression function");
}

function renderSize(value: ExpressionValue, context: CompileContext): string {
	const type = renderType(value, context);
	const valueSql = renderValue(value, "logical", context);
	const typeConst = constTypeName(type);
	if (typeConst === "text" || typeConst === "bytes") return `octet_length(${valueSql})`;
	if (typeConst === "array" || typeConst === "object") return `(SELECT count(*) FROM json_each(${valueSql}))`;
	if (typeConst !== undefined) return "NULL";
	return `CASE WHEN ${type} IN ('text', 'bytes') THEN octet_length(${valueSql}) WHEN ${type} IN ('array', 'object') THEN (SELECT count(*) FROM json_each(${valueSql})) END`;
}

function referencePresent(reference: ExpressionReference, context: CompileContext): string {
	switch (reference.ref) {
		case "hashKey":
		case "v":
		case "data":
			if (reference.ref === "data" && reference.path !== undefined) {
				recordDataReference(reference, context);
				return `CASE WHEN i.hk IS NOT NULL AND i.data_kind = ${JSON_KIND} THEN json_type(i.data, ${bindPath(reference.path, context)}) IS NOT NULL ELSE 0 END`;
			}
			if (reference.ref === "data") context.completeData = true;
			return "(i.hk IS NOT NULL)";
		case "sortKey":
			return `(i.hk IS NOT NULL AND length(i.sk) > 0)`;
		case "ttlAt":
			return `(i.hk IS NOT NULL AND i.ttl_epoch_utc_seconds IS NOT NULL)`;
	}
}

function referenceType(reference: ExpressionReference, context: CompileContext): string {
	switch (reference.ref) {
		case "hashKey":
			return keyType("i.hk", "i.hk IS NOT NULL");
		case "sortKey":
			return keyType("i.sk", "i.hk IS NOT NULL AND length(i.sk) > 0");
		case "v":
			return `CASE WHEN i.hk IS NOT NULL THEN 'number' ELSE 'missing' END`;
		case "ttlAt":
			return `CASE WHEN i.hk IS NOT NULL AND i.ttl_epoch_utc_seconds IS NOT NULL THEN 'number' ELSE 'missing' END`;
		case "data":
			if (reference.path !== undefined) {
				recordDataReference(reference, context);
				const path = bindPath(reference.path, context);
				return `CASE WHEN i.hk IS NULL OR i.data_kind <> ${JSON_KIND} THEN 'missing' ELSE ${jsonTypeSql(`json_type(i.data, ${path})`)} END`;
			}
			context.completeData = true;
			return `CASE WHEN i.hk IS NULL THEN 'missing' WHEN i.data_kind = ${BYTES_KIND} THEN 'bytes' WHEN i.data_kind = ${TEXT_KIND} THEN 'text' WHEN i.data_kind = ${JSON_KIND} THEN ${jsonTypeSql("json_type(i.data)")} ELSE 'missing' END`;
	}
}

function referenceValue(reference: ExpressionReference, mode: ValueMode, context: CompileContext): string {
	switch (reference.ref) {
		case "hashKey":
			return mode === "key" ? "i.hk" : logicalKeyValue("i.hk");
		case "sortKey":
			return mode === "key" ? "i.sk" : logicalKeyValue("i.sk");
		case "v":
			return "i.v";
		case "ttlAt":
			return "i.ttl_epoch_utc_seconds";
		case "data":
			if (reference.path !== undefined) {
				recordDataReference(reference, context);
				return `CASE WHEN i.hk IS NOT NULL AND i.data_kind = ${JSON_KIND} THEN json_extract(i.data, ${bindPath(reference.path, context)}) END`;
			}
			context.completeData = true;
			if (mode === "sqlite") return "i.data";
			return `CASE WHEN i.data_kind = ${JSON_KIND} AND json_type(i.data) IN ('array', 'object') THEN i.data WHEN i.data_kind = ${JSON_KIND} THEN json_extract(i.data, '$') ELSE i.data END`;
	}
}

function keyType(column: string, present: string): string {
	return `CASE WHEN ${present} THEN CASE WHEN substr(${column}, 1, 1) = x'ff' THEN 'bytes' ELSE 'text' END ELSE 'missing' END`;
}

function logicalKeyValue(column: string): string {
	return `CASE WHEN substr(${column}, 1, 1) = x'ff' THEN substr(${column}, 2) ELSE CAST(${column} AS TEXT) END`;
}

function jsonTypeSql(jsonType: string): string {
	return `CASE ${jsonType} WHEN 'null' THEN 'null' WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number' WHEN 'text' THEN 'text' WHEN 'array' THEN 'array' WHEN 'object' THEN 'object' ELSE 'missing' END`;
}

function comparisonModes(left: ExpressionValue, right: ExpressionValue): readonly [ValueMode, ValueMode] {
	if ((isDirectKeyReference(left) && isKeyLiteral(right)) || (isDirectKeyReference(right) && isKeyLiteral(left))) return ["key", "key"];
	if (isDirectKeyReference(left) && isDirectKeyReference(right)) return ["key", "key"];
	return ["logical", "logical"];
}

function isDirectKeyReference(value: ExpressionValue): boolean {
	return "ref" in value && (value.ref === "hashKey" || value.ref === "sortKey");
}

function isKeyLiteral(value: ExpressionValue): boolean {
	return ("val" in value && typeof value.val === "string") || "b64" in value;
}

function literalNativeType(value: ExpressionValue): "null" | "boolean" | "number" | "text" | "bytes" | undefined {
	if ("b64" in value) return "bytes";
	if (!("val" in value)) return;
	return literalType(value.val as JsonPrimitive);
}

function literalType(value: JsonPrimitive): "null" | "boolean" | "number" | "text" {
	if (value === null) return "null";
	if (typeof value === "boolean") return "boolean";
	if (typeof value === "number") return "number";
	return "text";
}

function bindLiteral(value: JsonPrimitive, mode: ValueMode, context: CompileContext): string {
	const descriptor: ExpressionBindingDescriptor =
		mode === "key" && typeof value === "string"
			? { kind: "keyText", value }
			: { kind: "val", value: typeof value === "number" && Object.is(value, -0) ? 0 : value };
	return bindDescriptor(descriptor, context);
}

function bindByteLiteral(value: { b64: string }, mode: ValueMode, context: CompileContext): string {
	const { canonical } = decodeByteLiteral(value);
	return bindDescriptor({ kind: mode === "key" ? "keyB64" : "b64", value: canonical }, context);
}

function bindPath(path: string, context: CompileContext): string {
	return bindDescriptor({ kind: "path", value: path }, context);
}

/**
 * Registers one binding and returns its numbered SQLite parameter (`?N`), so a fragment rendered
 * many times reuses one binding. `N` counts from after the fixed statement bindings.
 */
function bindDescriptor(descriptor: ExpressionBindingDescriptor, context: CompileContext): string {
	// The kind names contain no ":" and the value is JSON text, so this key cannot collide.
	const key = `${descriptor.kind}:${JSON.stringify(descriptor.value)}`;
	let index = context.bindingIndexByKey.get(key);
	if (index === undefined) {
		index = context.bindings.length;
		context.bindings.push(descriptor);
		context.bindingIndexByKey.set(key, index);
	}
	return `?${context.paramOffset + index + 1}`;
}

function recordDataReference(reference: Extract<ExpressionReference, { ref: "data" }>, context: CompileContext): void {
	if (reference.path === undefined) context.completeData = true;
	else context.paths.add(reference.path);
}
