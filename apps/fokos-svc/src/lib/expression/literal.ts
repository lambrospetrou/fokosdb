import type { JsonPrimitive } from "../json-types.js";
import { ExpressionError } from "./errors.js";

/** Returns a JSON scalar unchanged and rejects values that need composite or binary literal support. */
export function validateScalarLiteral(value: unknown): JsonPrimitive {
	if (value === null || typeof value === "string" || typeof value === "boolean") return value;
	if (typeof value === "number" && Number.isFinite(value)) return value;
	throw new ExpressionError("invalid_literal", "invalid scalar literal");
}
