import type { JsonPrimitive } from "../json-types.js";
import { decodeByteLiteral } from "./byte-literal.js";
import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { validateScalarLiteral } from "./literal.js";
import { validateReadJsonPath, validateWriteJsonPath } from "./path.js";
import type {
	ConditionExpression,
	ExpressionReference,
	ExpressionValue,
	ProjectionExpression,
	UpdateAction,
	UpdateTarget,
} from "./types.js";
import { utf8WithinLimit } from "./utf8.js";

class IdentityWriter {
	#out = "";

	#writeScalar(value: unknown): void {
		const scalar: JsonPrimitive = validateScalarLiteral(value);
		this.#out += JSON.stringify(scalar);
	}

	#writeReference(reference: ExpressionReference): void {
		this.#out += `{"ref":${JSON.stringify(reference.ref)}`;
		if (reference.ref === "data" && reference.path !== undefined) {
			validateReadJsonPath(reference.path);
			this.#out += `,"path":${JSON.stringify(reference.path)}`;
		}
		this.#out += "}";
	}

	writeValue(value: ExpressionValue): void {
		if (typeof value !== "object" || value === null) throw new ExpressionError("invalid_ast", "invalid expression value");
		if ("val" in value) {
			this.#out += '{"val":';
			this.#writeScalar(value.val);
			this.#out += "}";
			return;
		}
		if ("b64" in value) {
			this.#out += `{"b64":${JSON.stringify(decodeByteLiteral(value).canonical)}}`;
			return;
		}
		if ("ref" in value) {
			this.#writeReference(value);
			return;
		}
		if (!("fn" in value) || !Array.isArray(value.args)) throw new ExpressionError("invalid_ast", "invalid expression value");
		this.#out += `{"fn":${JSON.stringify(value.fn)},"args":[`;
		for (let i = 0; i < value.args.length; i++) {
			if (i > 0) this.#out += ",";
			this.writeValue(value.args[i]);
		}
		this.#out += "]}";
	}

	writeCondition(condition: ConditionExpression): void {
		this.#out += `{"op":${JSON.stringify(condition.op)},"args":[`;
		const nestedConditions = condition.op === "and" || condition.op === "or" || condition.op === "not";
		for (let i = 0; i < condition.args.length; i++) {
			if (i > 0) this.#out += ",";
			if (nestedConditions) this.writeCondition(condition.args[i] as ConditionExpression);
			else this.writeValue(condition.args[i] as ExpressionValue);
		}
		this.#out += "]}";
	}

	writeProjection(projection: readonly ProjectionExpression[]): void {
		this.#out += "[";
		for (let i = 0; i < projection.length; i++) {
			const entry = projection[i];
			if (i > 0) this.#out += ",";
			this.#out += '{"expr":';
			this.writeValue(entry.expr);
			if (entry.as !== undefined) this.#out += `,"as":${JSON.stringify(entry.as)}`;
			this.#out += "}";
		}
		this.#out += "]";
	}

	#writeTarget(target: UpdateTarget, allowAppend: boolean): void {
		if (typeof target !== "object" || target === null || target.ref !== "data" || typeof target.path !== "string") {
			throw new ExpressionError("invalid_ast", "invalid update target");
		}
		for (const field in target) {
			if (field !== "ref" && field !== "path") throw new ExpressionError("invalid_ast", "invalid update target fields");
		}
		const segments = validateWriteJsonPath(target.path, { allowAppend });
		if (segments.length === 0) throw new ExpressionError("invalid_path", "target path must not be root");
		this.#out += `{"ref":"data","path":${JSON.stringify(target.path)}}`;
	}

	writeUpdate(update: readonly UpdateAction[]): void {
		if (!Array.isArray(update) || update.length === 0) {
			throw new ExpressionError("invalid_ast", "update expression must contain at least one action");
		}
		if (update.length > EXPRESSION_LIMITS.updateActions) {
			throw new ExpressionError("complexity_limit", "update action limit exceeded");
		}
		this.#out += "[";
		for (let i = 0; i < update.length; i++) {
			const action = update[i];
			if (typeof action !== "object" || action === null) throw new ExpressionError("invalid_ast", "invalid update action");
			if (i > 0) this.#out += ",";
			if (action.action === "set") {
				for (const field in action) {
					if (field !== "action" && field !== "target" && field !== "value") {
						throw new ExpressionError("invalid_ast", "invalid action fields");
					}
				}
				this.#out += '{"action":"set","target":';
				this.#writeTarget(action.target, true);
				this.#out += ',"value":';
				this.writeValue(action.value);
				this.#out += "}";
			} else if (action.action === "remove") {
				for (const field in action) {
					if (field !== "action" && field !== "target") {
						throw new ExpressionError("invalid_ast", "invalid action fields");
					}
				}
				this.#out += '{"action":"remove","target":';
				this.#writeTarget(action.target, false);
				this.#out += "}";
			} else {
				throw new ExpressionError("invalid_ast", "unknown update action");
			}
		}
		this.#out += "]";
	}

	finish(): string {
		if (!utf8WithinLimit(this.#out, EXPRESSION_LIMITS.canonicalPayloadBytes)) {
			throw new ExpressionError("complexity_limit", "canonical identity exceeds the payload limit");
		}
		return this.#out;
	}
}

/** Returns the canonical identity text for one scalar expression value. */
export function canonicalValueIdentity(value: ExpressionValue): string {
	const writer = new IdentityWriter();
	writer.writeValue(value);
	return writer.finish();
}

/** Returns canonical identity text for condition idempotency and cursor fingerprints. */
export function canonicalConditionIdentity(condition: ConditionExpression): string {
	const writer = new IdentityWriter();
	writer.writeCondition(condition);
	return writer.finish();
}

/** Returns canonical identity text for an ordered projection list. */
export function canonicalProjectionIdentity(projection: readonly ProjectionExpression[]): string {
	const writer = new IdentityWriter();
	writer.writeProjection(projection);
	return writer.finish();
}

/** Returns canonical identity text for an update expression. */
export function canonicalUpdateIdentity(update: readonly UpdateAction[]): string {
	const writer = new IdentityWriter();
	writer.writeUpdate(update);
	return writer.finish();
}
