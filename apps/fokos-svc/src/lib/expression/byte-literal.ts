import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { utf8WithinLimit } from "./utf8.js";

export type ByteLiteral = {
	bytes: Uint8Array;
	/** Re-encoded base64, so two encodings of one value give one binding descriptor and one identity. */
	canonical: string;
};

// Semantic validation, compilation, and identity each traverse the same caller AST, so the decode is
// memoized on the literal node and runs once per node.
const decoded = new WeakMap<object, ByteLiteral>();

export function decodeBase64Bytes(text: unknown): Uint8Array {
	if (typeof text !== "string") throw new ExpressionError("invalid_literal", "byte literal must be base64 text");
	if (!utf8WithinLimit(text, EXPRESSION_LIMITS.canonicalPayloadBytes)) {
		throw new ExpressionError("complexity_limit", "byte literal exceeds the payload limit");
	}
	let bytes: Uint8Array;
	try {
		bytes = Uint8Array.fromBase64(text);
	} catch (error) {
		throw new ExpressionError("invalid_literal", "byte literal is not valid base64", { cause: error });
	}
	if (bytes.byteLength === 0) throw new ExpressionError("invalid_literal", "empty byte literal is not allowed");
	return bytes;
}

export function decodeByteLiteral(node: { b64: unknown }): ByteLiteral {
	const cached = decoded.get(node);
	if (cached !== undefined) return cached;
	const bytes = decodeBase64Bytes(node.b64);
	const literal: ByteLiteral = { bytes, canonical: bytes.toBase64() };
	decoded.set(node, literal);
	return literal;
}
