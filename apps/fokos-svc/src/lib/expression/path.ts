import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { utf8WithinLimit } from "./utf8.js";

/** Validates one bounded SQLite JSON read path without retaining parsed path segments. */
export function validateReadJsonPath(path: unknown): asserts path is string {
	if (typeof path !== "string" || path[0] !== "$") invalidPath();
	if (!utf8WithinLimit(path, EXPRESSION_LIMITS.jsonPathBytes)) {
		throw new ExpressionError("complexity_limit", "JSON path exceeds the path limit");
	}
	if (path.isWellFormed?.() === false || path.includes("\0")) invalidPath();

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

// Single-character range comparisons compare one UTF-16 code unit each, so they classify ASCII
// grammar characters without per-character regular expression calls.
function isDecimalDigit(char: string): boolean {
	return char >= "0" && char <= "9";
}

function isHexDigit(char: string): boolean {
	return isDecimalDigit(char) || (char >= "A" && char <= "F") || (char >= "a" && char <= "f");
}

function scanQuotedLabel(path: string, start: number): number {
	let i = start;
	while (i < path.length) {
		const char = path[i];
		if (char === '"') return i + 1;
		if (char <= "\u001f") invalidPath();
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
			if (!isHexDigit(path[i + j])) invalidPath();
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
		const char = path[i];
		if (char <= " " || char === '"' || char === "\\") invalidPath();
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
		while (i < path.length && isDecimalDigit(path[i])) {
			if (path[i] !== "0") nonZero = true;
			i++;
		}
		if (i === digitStart || !nonZero) invalidPath();
	} else {
		const digitStart = i;
		while (i < path.length && isDecimalDigit(path[i])) i++;
		if (i === digitStart) invalidPath();
	}
	if (path[i] !== "]") invalidPath();
	return i + 1;
}
