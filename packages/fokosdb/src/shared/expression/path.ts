import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { utf8WithinLimit } from "./utf8.js";

/** Parsed segment of a SQLite JSON path. */
export type PathSegment =
	| { readonly kind: "member"; readonly key: string }
	| { readonly kind: "index"; readonly index: number }
	| { readonly kind: "reverseIndex"; readonly offset: number }
	| { readonly kind: "append" };

/** Compares two path segments for structural equality. */
export function segmentsEqual(a: PathSegment, b: PathSegment): boolean {
	if (a.kind !== b.kind) return false;
	switch (a.kind) {
		case "member":
			return a.key === (b as typeof a).key;
		case "index":
			return a.index === (b as typeof a).index;
		case "reverseIndex":
			return a.offset === (b as typeof a).offset;
		case "append":
			return true;
	}
}

/** Returns true if parent is a strict prefix (parent path) of child. */
export function isParentPath(parent: readonly PathSegment[], child: readonly PathSegment[]): boolean {
	if (parent.length >= child.length) return false;
	for (let i = 0; i < parent.length; i++) {
		if (!segmentsEqual(parent[i], child[i])) return false;
	}
	return true;
}

/** Returns true if two segment lists describe identical paths. */
export function pathsEqual(a: readonly PathSegment[], b: readonly PathSegment[]): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (!segmentsEqual(a[i], b[i])) return false;
	}
	return true;
}

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
			i = scanArrayIndex(path, i + 1, false);
		} else {
			invalidPath();
		}
		dereferences++;
		if (dereferences > EXPRESSION_LIMITS.jsonPathDereferences) {
			throw new ExpressionError("complexity_limit", "JSON path exceeds the dereference limit");
		}
	}
}

/** Validates one bounded SQLite JSON write path and returns its parsed segments. */
export function validateWriteJsonPath(path: unknown, options?: { allowAppend?: boolean }): readonly PathSegment[] {
	if (typeof path !== "string" || path[0] !== "$") invalidPath();
	if (!utf8WithinLimit(path, EXPRESSION_LIMITS.jsonPathBytes)) {
		throw new ExpressionError("complexity_limit", "JSON path exceeds the path limit");
	}
	if (path.isWellFormed?.() === false || path.includes("\0")) invalidPath();

	const allowAppend = options?.allowAppend ?? false;
	const segments: PathSegment[] = [];
	let i = 1;
	let dereferences = 0;

	while (i < path.length) {
		if (path[i] === ".") {
			const start = i + 1;
			if (path[start] === '"') {
				const next = scanQuotedLabel(path, start + 1);
				const decoded = JSON.parse(path.slice(start, next)) as string;
				segments.push({ kind: "member", key: decoded });
				i = next;
			} else {
				const next = scanLabel(path, start);
				segments.push({ kind: "member", key: path.slice(start, next) });
				i = next;
			}
		} else if (path[i] === "[") {
			const start = i + 1;
			if (path[start] === "#") {
				if (path[start + 1] === "]") {
					if (!allowAppend) invalidPath();
					if (start + 2 < path.length) invalidPath();
					segments.push({ kind: "append" });
					i = start + 2;
				} else if (path[start + 1] === "-") {
					const next = scanArrayIndex(path, start, false);
					const offset = Number(path.slice(start + 2, next - 1));
					segments.push({ kind: "reverseIndex", offset });
					i = next;
				} else {
					invalidPath();
				}
			} else {
				const next = scanArrayIndex(path, start, false);
				const index = Number(path.slice(start, next - 1));
				segments.push({ kind: "index", index });
				i = next;
			}
		} else {
			invalidPath();
		}
		dereferences++;
		if (dereferences > EXPRESSION_LIMITS.jsonPathDereferences) {
			throw new ExpressionError("complexity_limit", "JSON path exceeds the dereference limit");
		}
	}

	return segments;
}

function invalidPath(): never {
	throw new ExpressionError("invalid_path", "invalid SQLite JSON path");
}

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

function scanArrayIndex(path: string, start: number, allowAppend: boolean): number {
	let i = start;
	if (path[i] === "#") {
		if (allowAppend && path[i + 1] === "]") return i + 2;
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
