import type { ExpressionNativeType, ExpressionValue } from "./types.js";
import { EXPRESSION_NATIVE_TYPES } from "./types.js";
import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { SQLITE_FUNCTION_ARITY, SQLITE_MATH_FUNCTIONS, SQLITE_SCALAR_FUNCTIONS } from "./sqlite-functions.js";
import { utf8WithinLimit } from "./utf8.js";

/**
 * Context bitflags that control where an operation is valid.
 * A single bitflag integer avoids allocations across registry lookups.
 */

/** Valid in write condition expressions. */
export const EXPRESSION_CONTEXT_CONDITION = 1 << 0;
/** Valid in update action value expressions. */
export const EXPRESSION_CONTEXT_UPDATE_VALUE = 1 << 1;
/** Valid in read filter expressions. */
export const EXPRESSION_CONTEXT_FILTER = 1 << 2;
/** Valid in read projection expressions. */
export const EXPRESSION_CONTEXT_PROJECTION = 1 << 3;

/** Bitmask that matches all expression contexts. */
export const EXPRESSION_CONTEXT_ALL =
	EXPRESSION_CONTEXT_CONDITION | EXPRESSION_CONTEXT_UPDATE_VALUE | EXPRESSION_CONTEXT_FILTER | EXPRESSION_CONTEXT_PROJECTION;

/** Named expression context threaded through validation and compilation. */
export type ExpressionContext = "condition" | "update-value" | "filter" | "projection";

/** Converts a named expression context into its integer bitflag. */
export function contextToFlag(context: ExpressionContext): number {
	switch (context) {
		case "condition":
			return EXPRESSION_CONTEXT_CONDITION;
		case "update-value":
			return EXPRESSION_CONTEXT_UPDATE_VALUE;
		case "filter":
			return EXPRESSION_CONTEXT_FILTER;
		case "projection":
			return EXPRESSION_CONTEXT_PROJECTION;
	}
}

/** Returns true if the operation's context mask contains the given expression context. */
export function matchesContext(operationContexts: number, context: ExpressionContext): boolean {
	return (operationContexts & contextToFlag(context)) !== 0;
}

export type DirectReference = "hashKey" | "sortKey" | "v" | "ttlAt" | "data";

/** Inferred type facts for an expression value node during semantic analysis. */
export type ValueFacts = {
	types: ReadonlySet<ExpressionNativeType>;
	directReference?: DirectReference;
	emptyStringLiteral?: true;
	byteLiteral?: true;
};

/** Minimum and maximum argument count range: [minimum, maximum]. */
export type OperationArity = readonly [minimum: number, maximum: number];

export type OperationTypeRule = (args: readonly ValueFacts[], rawArgs: readonly unknown[]) => ValueFacts;

/**
 * Callbacks an operation uses to render its arguments as SQL.
 *
 * `renderValue` has a mode that tells it what the generated SQL value is for:
 * - "logical": a value for a comparison or a condition. It uses the natural SQLite type for the
 *   expression, such as TEXT for a string or INTEGER for a number, and it may bind a parameter.
 * - "key": a value for a hash or sort key comparison, encoded as key bytes.
 * - "sqlite": a raw SQLite scalar for arithmetic or for one of the built-in SQLite functions.
 * - "json": a value that is safe to pass to `jsonb_set` as the new document value. This mode must
 *   preserve the JSON type, so a string stays a string, a number stays a number, and a boolean stays
 *   a boolean.
 */
export type OperationRenderers = {
	renderValue: (value: ExpressionValue, mode: "logical" | "key" | "sqlite" | "json") => string;
	renderType: (value: ExpressionValue) => string;
	renderPresent: (value: ExpressionValue) => string;
	renderSize: (value: ExpressionValue) => string;
};

export type OperationRenderValue = (args: readonly ExpressionValue[], renderers: OperationRenderers) => string;

export type OperationRenderPresent = (args: readonly ExpressionValue[], renderers: OperationRenderers) => string;

export type OperationRenderType = (args: readonly ExpressionValue[], renderers: OperationRenderers) => string;

/**
 * Declarative definition and rules for one expression operation.
 */
export type OperationDefinition = {
	/** Unique operation identifier or operator symbol. */
	readonly name: string;
	/** Context bitmask where the operation is permitted. Defaults to all contexts. */
	readonly contexts?: number;
	/** Accepted argument count range. */
	readonly arity: OperationArity;
	/** Computes output value facts from input argument facts. */
	readonly typeRule: OperationTypeRule;
	/** Compiles the SQL value expression. */
	readonly renderValue: OperationRenderValue;
	/**
	 * Compiles the SQL value expression when the result becomes a member of a JSON document.
	 * Defaults to `renderValue`.
	 *
	 * Only an operation that returns one of its arguments unchanged needs this. Such an operation must
	 * render that argument in "json" mode too, or a boolean argument reaches it as SQLite's 1 or 0 and
	 * lands in the document as a number. An operation that computes a NEW value — text, a number —
	 * needs nothing, because the computed value is already what the document should hold.
	 */
	readonly renderJsonValue?: OperationRenderValue;
	/** Compiles the SQL presence test. Defaults to always present ("1"). */
	readonly renderPresent?: OperationRenderPresent;
	/** Compiles the SQL native type expression. Defaults to SQLite typeof CASE. */
	readonly renderType?: OperationRenderType;
};

export const equalityTypes = new Set<ExpressionNativeType>(["null", "boolean", "number", "text", "bytes"]);
export const orderedTypes = new Set<ExpressionNativeType>(["number", "text", "bytes"]);
export const prefixTypes = new Set<ExpressionNativeType>(["text", "bytes"]);
export const sizeInputTypes = new Set<ExpressionNativeType>(["text", "bytes", "array", "object"]);
export const arraySearchTypes = new Set<ExpressionNativeType>(["null", "boolean", "number", "text"]);
export const nullTypes = new Set<ExpressionNativeType>(["null"]);
export const booleanTypes = new Set<ExpressionNativeType>(["boolean"]);
export const numberTypes = new Set<ExpressionNativeType>(["number"]);
export const textTypes = new Set<ExpressionNativeType>(["text"]);
export const bytesTypes = new Set<ExpressionNativeType>(["bytes"]);
export const missingNumberTypes = new Set<ExpressionNativeType>(["missing", "number"]);
export const missingTextBytesTypes = new Set<ExpressionNativeType>(["missing", "text", "bytes"]);
export const nullNumberTypes = new Set<ExpressionNativeType>(["null", "number"]);
export const nullTextTypes = new Set<ExpressionNativeType>(["null", "text"]);
export const nullBytesTypes = new Set<ExpressionNativeType>(["null", "bytes"]);
export const allTypes = new Set<ExpressionNativeType>(EXPRESSION_NATIVE_TYPES);
export const dataPathTypes = new Set<ExpressionNativeType>(EXPRESSION_NATIVE_TYPES.filter((type) => type !== "bytes"));

export const nullValue: ValueFacts = { types: nullTypes };
export const booleanValue: ValueFacts = { types: booleanTypes };
export const numberValue: ValueFacts = { types: numberTypes };
export const textValue: ValueFacts = { types: textTypes };
export const emptyTextValue: ValueFacts = { types: textTypes, emptyStringLiteral: true };
export const byteLiteralValue: ValueFacts = { types: bytesTypes, byteLiteral: true };
export const missingNumberValue: ValueFacts = { types: missingNumberTypes };
export const nullNumberValue: ValueFacts = { types: nullNumberTypes };
export const nullTextValue: ValueFacts = { types: nullTextTypes };
export const nullBytesValue: ValueFacts = { types: nullBytesTypes };
export const hashKeyValue: ValueFacts = { types: missingTextBytesTypes, directReference: "hashKey" };
export const sortKeyValue: ValueFacts = { types: missingTextBytesTypes, directReference: "sortKey" };
export const versionValue: ValueFacts = { types: missingNumberTypes, directReference: "v" };
export const ttlValue: ValueFacts = { types: missingNumberTypes, directReference: "ttlAt" };
export const dataValue: ValueFacts = { types: allTypes, directReference: "data" };
export const dataPathValue: ValueFacts = { types: dataPathTypes, directReference: "data" };

const SIZE_INPUT_TYPE_NAMES: readonly string[] = ["text", "bytes", "array", "object"];

function constTypeName(typeSql: string): string | undefined {
	return typeSql.startsWith("'") ? typeSql.slice(1, -1) : undefined;
}

function typeListSql(types: readonly string[]): string {
	return types.map((type) => `'${type}'`).join(", ");
}

function hasAnyType(actual: ReadonlySet<ExpressionNativeType>, accepted: ReadonlySet<ExpressionNativeType>): boolean {
	for (const type of accepted) {
		if (actual.has(type)) return true;
	}
	return false;
}

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

function stringLiteral(value: unknown): string | undefined {
	if (typeof value !== "object" || value === null || Array.isArray(value)) return;
	const literal = (value as { val?: unknown }).val;
	return typeof literal === "string" ? literal : undefined;
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

/**
 * The SQLite functions that return one of their arguments unchanged, mapped to the index of their
 * first VALUE argument. Everything before that index is a test, not a value: `iif` takes its
 * condition first.
 *
 * These are the only built-in functions that need a "json" rendering, because they are the only ones
 * whose result IS an argument. See `OperationDefinition.renderJsonValue`.
 */
const SQLITE_VALUE_PASSTHROUGH: ReadonlyMap<string, number> = new Map([
	["coalesce", 0],
	["ifnull", 0],
	["nullif", 0],
	["iif", 1],
]);

function buildSqliteOperations(): OperationDefinition[] {
	const operations: OperationDefinition[] = [];
	for (const name of SQLITE_SCALAR_FUNCTIONS) {
		const arity = SQLITE_FUNCTION_ARITY.get(name);
		if (!arity) continue;
		const valueArgsFrom = SQLITE_VALUE_PASSTHROUGH.get(name);

		operations.push({
			name: `sqlite.${name}`,
			contexts: EXPRESSION_CONTEXT_ALL,
			arity,
			typeRule: (argFacts, rawArgs) => {
				if (name === "glob" || name === "like") validatePatternArguments(name, rawArgs);
				if (sqliteNumberFunctions.has(name)) return nullNumberValue;
				if (sqliteTextFunctions.has(name)) return nullTextValue;
				if (name === "unhex") return nullBytesValue;

				let dynamicTypes: Set<ExpressionNativeType> | undefined;
				for (let i = 0; i < argFacts.length; i++) {
					if (name !== "nullif" || i === 0) {
						dynamicTypes ??= new Set(["null"]);
						for (const type of argFacts[i].types) {
							if (type !== "missing") dynamicTypes.add(type);
						}
					}
				}
				return { types: dynamicTypes ?? nullTypes };
			},
			renderValue: (args, renderers) => `${name}(${args.map((arg) => renderers.renderValue(arg, "sqlite")).join(", ")})`,
			renderJsonValue:
				valueArgsFrom === undefined
					? undefined
					: (args, renderers) =>
							`${name}(${args.map((arg, i) => renderers.renderValue(arg, i < valueArgsFrom ? "sqlite" : "json")).join(", ")})`,
			renderPresent: () => "1",
			renderType: (args, renderers) => {
				const call = `${name}(${args.map((arg) => renderers.renderValue(arg, "sqlite")).join(", ")})`;
				return `CASE typeof(${call}) WHEN 'null' THEN 'null' WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number' WHEN 'text' THEN 'text' WHEN 'blob' THEN 'bytes' ELSE 'missing' END`;
			},
		});
	}
	return operations;
}

// SQLite evaluates 1e999 as +Infinity. In SQLite, `abs(x) < 1e999` evaluates to 1 for finite
// numbers, 0 for +/-Infinity, and NULL for NaN. This guard verifies both operands are numbers
// and the arithmetic result is finite, because JSON cannot store NaN or Infinity.
function renderArithmeticPresent(symbol: "+" | "-" | "*", args: readonly ExpressionValue[], renderers: OperationRenderers): string {
	const op = `(${renderers.renderValue(args[0], "sqlite")} ${symbol} ${renderers.renderValue(args[1], "sqlite")})`;
	return `(${renderers.renderPresent(args[0])} AND ${renderers.renderPresent(args[1])} AND ${renderers.renderType(args[0])} = 'number' AND ${renderers.renderType(args[1])} = 'number' AND abs(${op}) < 1e999)`;
}

const FOKOS_OPERATIONS: readonly OperationDefinition[] = [
	{
		name: "attribute_type",
		contexts: EXPRESSION_CONTEXT_ALL,
		arity: [1, 1],
		typeRule: () => textValue,
		renderValue: (args, renderers) => renderers.renderType(args[0]),
		renderPresent: () => "1",
		renderType: () => "'text'",
	},
	{
		name: "size",
		contexts: EXPRESSION_CONTEXT_ALL,
		arity: [1, 1],
		typeRule: (argFacts) => {
			const input = argFacts[0];
			if (!hasAnyType(input.types, sizeInputTypes)) throw new ExpressionError("invalid_type", "invalid size input type");
			for (const type of input.types) {
				if (!sizeInputTypes.has(type)) return missingNumberValue;
			}
			return numberValue;
		},
		renderValue: (args, renderers) => renderers.renderSize(args[0]),
		renderPresent: (args, renderers) => {
			const inputType = renderers.renderType(args[0]);
			const inputConst = constTypeName(inputType);
			if (inputConst !== undefined) return SIZE_INPUT_TYPE_NAMES.includes(inputConst) ? "1" : "0";
			return `(${inputType} IN (${typeListSql(SIZE_INPUT_TYPE_NAMES)}))`;
		},
		renderType: () => "'number'",
	},
	{
		name: "if_not_exists",
		contexts: EXPRESSION_CONTEXT_UPDATE_VALUE,
		arity: [2, 2],
		typeRule: (argFacts) => {
			const pathTypes = argFacts[0].types;
			const fallbackTypes = argFacts[1].types;
			const resultTypes = new Set<ExpressionNativeType>();
			for (const t of pathTypes) {
				if (t !== "missing") resultTypes.add(t);
			}
			for (const t of fallbackTypes) {
				resultTypes.add(t);
			}
			return { types: resultTypes };
		},
		renderValue: (args, renderers) =>
			`CASE WHEN ${renderers.renderPresent(args[0])} THEN ${renderers.renderValue(args[0], "sqlite")} ELSE ${renderers.renderValue(args[1], "sqlite")} END`,
		renderJsonValue: (args, renderers) =>
			`CASE WHEN ${renderers.renderPresent(args[0])} THEN ${renderers.renderValue(args[0], "json")} ELSE ${renderers.renderValue(args[1], "json")} END`,
		renderPresent: (args, renderers) => {
			// The result is present when either branch is. A branch that is always present therefore
			// decides the whole test, and folding it to "1" keeps a constant-true term out of the SQL.
			const pathPresent = renderers.renderPresent(args[0]);
			const fallbackPresent = renderers.renderPresent(args[1]);
			if (pathPresent === "1" || fallbackPresent === "1") return "1";
			return `(${pathPresent} OR ${fallbackPresent})`;
		},
		renderType: (args, renderers) =>
			`CASE WHEN ${renderers.renderPresent(args[0])} THEN ${renderers.renderType(args[0])} ELSE ${renderers.renderType(args[1])} END`,
	},
	{
		name: "+",
		contexts: EXPRESSION_CONTEXT_UPDATE_VALUE,
		arity: [2, 2],
		typeRule: (argFacts) => {
			if (!argFacts[0].types.has("number") || !argFacts[1].types.has("number")) {
				throw new ExpressionError("invalid_type", "incompatible expression types");
			}
			const nullable = argFacts[0].types.has("null") || argFacts[1].types.has("null");
			return nullable ? nullNumberValue : numberValue;
		},
		renderValue: (args, renderers) => `(${renderers.renderValue(args[0], "sqlite")} + ${renderers.renderValue(args[1], "sqlite")})`,
		renderPresent: (args, renderers) => renderArithmeticPresent("+", args, renderers),
		renderType: () => "'number'",
	},
	{
		name: "-",
		contexts: EXPRESSION_CONTEXT_UPDATE_VALUE,
		arity: [2, 2],
		typeRule: (argFacts) => {
			if (!argFacts[0].types.has("number") || !argFacts[1].types.has("number")) {
				throw new ExpressionError("invalid_type", "incompatible expression types");
			}
			const nullable = argFacts[0].types.has("null") || argFacts[1].types.has("null");
			return nullable ? nullNumberValue : numberValue;
		},
		renderValue: (args, renderers) => `(${renderers.renderValue(args[0], "sqlite")} - ${renderers.renderValue(args[1], "sqlite")})`,
		renderPresent: (args, renderers) => renderArithmeticPresent("-", args, renderers),
		renderType: () => "'number'",
	},
	{
		name: "*",
		contexts: EXPRESSION_CONTEXT_UPDATE_VALUE,
		arity: [2, 2],
		typeRule: (argFacts) => {
			if (!argFacts[0].types.has("number") || !argFacts[1].types.has("number")) {
				throw new ExpressionError("invalid_type", "incompatible expression types");
			}
			const nullable = argFacts[0].types.has("null") || argFacts[1].types.has("null");
			return nullable ? nullNumberValue : numberValue;
		},
		renderValue: (args, renderers) => `(${renderers.renderValue(args[0], "sqlite")} * ${renderers.renderValue(args[1], "sqlite")})`,
		renderPresent: (args, renderers) => renderArithmeticPresent("*", args, renderers),
		renderType: () => "'number'",
	},
];

/** Declarative registry of all supported expression operations and functions. */
export const OPERATION_REGISTRY: ReadonlyMap<string, OperationDefinition> = new Map([
	...buildSqliteOperations().map((op) => [op.name, op] as const),
	...FOKOS_OPERATIONS.map((op) => [op.name, op] as const),
]);

/** Returns the operation definition for the given name or symbol, if registered. */
export function getOperationDefinition(name: string): OperationDefinition | undefined {
	return OPERATION_REGISTRY.get(name);
}
