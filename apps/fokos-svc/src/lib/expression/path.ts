import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";

const textEncoder = new TextEncoder();
const CONTROL_CHAR = /^[\u0000-\u001f]$/;
const DECIMAL_DIGIT = /^[0-9]$/;
const HEX_DIGIT = /^[0-9A-Fa-f]$/;
const UNQUOTED_FORBIDDEN = /^[\u0000-\u0020"\\]$/;

/** Validates one bounded SQLite JSON read path without retaining parsed path segments. */
export function validateReadJsonPath(path: unknown): asserts path is string {
	if (typeof path !== "string" || path[0] !== "$") invalidPath();
	if (path.length > EXPRESSION_LIMITS.jsonPathBytes) {
		throw new ExpressionError("complexity_limit", "JSON path exceeds the path limit");
	}
	if (path.isWellFormed?.() === false || path.includes("\0")) invalidPath();
	if (textEncoder.encode(path).byteLength > EXPRESSION_LIMITS.jsonPathBytes) {
		throw new ExpressionError("complexity_limit", "JSON path exceeds the path limit");
	}

	let i = 1;
	let dereferences = 0;
	while (i < path.length) {
		if (path[i] === ".") {
			i = scanLabel(path, i + 1);
		} else if (path[i] === "[") {
			i = scanArrayIndex(path, i + 1);
		} else {
			invalidPath();
		}
		dereferences++;
		if (dereferences > EXPRESSION_LIMITS.jsonPathDereferences) {
			throw new ExpressionError("complexity_limit", "JSON path exceeds the dereference limit");
		}
	}
}

function invalidPath(): never {
	throw new ExpressionError("invalid_path", "invalid SQLite JSON path");
}

function scanQuotedLabel(path: string, start: number): number {
	let i = start;
	while (i < path.length) {
		const char = path[i];
		if (char === '"') return i + 1;
		if (CONTROL_CHAR.test(char)) invalidPath();
		if (char !== "\\") {
			i++;
			continue;
		}
		i++;
		if (i >= path.length) invalidPath();
		const escape = path[i];
		if ('"\\/bfnrt'.includes(escape)) {
			i++;
			continue;
		}
		if (escape !== "u" || i + 4 >= path.length) invalidPath();
		for (let j = 1; j <= 4; j++) {
			if (!HEX_DIGIT.test(path[i + j])) invalidPath();
		}
		if (path[i + 1] === "0" && path[i + 2] === "0" && path[i + 3] === "0" && path[i + 4] === "0") invalidPath();
		i += 5;
	}
	return invalidPath();
}

function scanLabel(path: string, start: number): number {
	if (path[start] === '"') return scanQuotedLabel(path, start + 1);
	let i = start;
	while (i < path.length && path[i] !== "." && path[i] !== "[") {
		if (UNQUOTED_FORBIDDEN.test(path[i])) invalidPath();
		i++;
	}
	if (i === start) invalidPath();
	return i;
}

function scanArrayIndex(path: string, start: number): number {
	let i = start;
	if (path[i] === "#") {
		if (path[i + 1] !== "-") invalidPath();
		i += 2;
		const digitStart = i;
		let nonZero = false;
		while (i < path.length && DECIMAL_DIGIT.test(path[i])) {
			if (path[i] !== "0") nonZero = true;
			i++;
		}
		if (i === digitStart || !nonZero) invalidPath();
	} else {
		const digitStart = i;
		while (i < path.length && DECIMAL_DIGIT.test(path[i])) i++;
		if (i === digitStart) invalidPath();
	}
	if (path[i] !== "]") invalidPath();
	return i + 1;
}
