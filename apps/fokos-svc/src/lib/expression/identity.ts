import type { JsonPrimitive } from "../json-types.js";
import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { validateScalarLiteral } from "./literal.js";
import { validateReadJsonPath } from "./path.js";
import type { ConditionExpression, ExpressionReference, ExpressionValue, ProjectionExpression } from "./types.js";

const textEncoder = new TextEncoder();

class IdentityWriter {
	readonly #chunks: string[] = [];

	#append(value: string): void {
		this.#chunks.push(value);
	}

	#writeScalar(value: unknown): void {
		const scalar: JsonPrimitive = validateScalarLiteral(value);
		this.#append(JSON.stringify(scalar));
	}

	#writeReference(reference: ExpressionReference): void {
		this.#append('{"ref":');
		this.#append(JSON.stringify(reference.ref));
		if (reference.ref === "data" && reference.path !== undefined) {
			validateReadJsonPath(reference.path);
			this.#append(',"path":');
			this.#append(JSON.stringify(reference.path));
		}
		this.#append("}");
	}

	writeValue(value: ExpressionValue): void {
		if (typeof value !== "object" || value === null) throw new ExpressionError("invalid_ast", "invalid expression value");
		if ("val" in value) {
			this.#append('{"val":');
			this.#writeScalar(value.val);
			this.#append("}");
			return;
		}
		if ("ref" in value) {
			this.#writeReference(value);
			return;
		}
		if (!("fn" in value) || !Array.isArray(value.args)) throw new ExpressionError("invalid_ast", "invalid expression value");
		this.#append('{"fn":');
		this.#append(JSON.stringify(value.fn));
		this.#append(',"args":[');
		for (let i = 0; i < value.args.length; i++) {
			if (i > 0) this.#append(",");
			this.writeValue(value.args[i]);
		}
		this.#append("]}");
	}

	writeCondition(condition: ConditionExpression): void {
		this.#append('{"op":');
		this.#append(JSON.stringify(condition.op));
		this.#append(',"args":[');
		const nestedConditions = condition.op === "and" || condition.op === "or" || condition.op === "not";
		for (let i = 0; i < condition.args.length; i++) {
			if (i > 0) this.#append(",");
			if (nestedConditions) this.writeCondition(condition.args[i] as ConditionExpression);
			else this.writeValue(condition.args[i] as ExpressionValue);
		}
		this.#append("]}");
	}

	writeProjection(projection: readonly ProjectionExpression[]): void {
		this.#append("[");
		for (let i = 0; i < projection.length; i++) {
			if (i > 0) this.#append(",");
			this.#append('{"expr":');
			this.writeValue(projection[i].expr);
			if (projection[i].as !== undefined) {
				this.#append(',"as":');
				this.#append(JSON.stringify(projection[i].as));
			}
			this.#append("}");
		}
		this.#append("]");
	}

	finish(): string {
		const identity = this.#chunks.join("");
		if (textEncoder.encode(identity).byteLength > EXPRESSION_LIMITS.canonicalPayloadBytes) {
			throw new ExpressionError("complexity_limit", "canonical identity exceeds the payload limit");
		}
		return identity;
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
