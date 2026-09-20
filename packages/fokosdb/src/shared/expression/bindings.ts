import { KeyCodec } from "../partition-topology/key-codec.js";
import { decodeBase64Bytes } from "./byte-literal.js";
import type { ExpressionBindingDescriptor, ExpressionBindingLayout } from "./plan.js";

const directBindingsCache = new WeakMap<readonly ExpressionBindingDescriptor[], readonly unknown[]>();
const poolBindingsCache = new WeakMap<readonly ExpressionBindingDescriptor[], readonly unknown[]>();

/**
 * The bound values of a plan, materialized once per plan object and shared by every statement that
 * runs the plan. A plan arrives once per request and several statements bind it (a probe, then a
 * lock or a write; one leaf scan per child), so the descriptors are decoded one time instead of one
 * time per statement. The cache is keyed by the descriptor array, which no code mutates after the
 * compiler builds it, and it holds the plan weakly so a finished request releases its values.
 * Callers must not mutate the returned array.
 */
export function materializedPlanBindings(
	plan: { bindings: readonly ExpressionBindingDescriptor[] },
	layout: ExpressionBindingLayout = "direct",
): readonly unknown[] {
	const cache = layout === "pool" ? poolBindingsCache : directBindingsCache;
	let values = cache.get(plan.bindings);
	if (values === undefined) {
		values = materializeExpressionBindings(plan.bindings, layout);
		cache.set(plan.bindings, values);
	}
	return values;
}

/**
 * Turns a plan's binding descriptors into the values a statement binds.
 *
 * Under the "direct" layout each descriptor becomes one bound value, in descriptor order. Under the
 * "pool" layout every descriptor becomes one element of a single JSON array, and the function returns
 * that array's JSON text as the only bound value: the SQL reads its element with
 * `json_extract(?P, '$[i]')`, wrapped in `unhex` for the descriptors that carry hex-encoded bytes.
 * KeyCodec.encode is pure, so the client and the partition build the same text.
 */
export function materializeExpressionBindings(
	descriptors: readonly ExpressionBindingDescriptor[],
	layout: ExpressionBindingLayout = "direct",
): unknown[] {
	if (layout === "pool") {
		return [
			JSON.stringify(
				descriptors.map((descriptor) => {
					switch (descriptor.kind) {
						case "val":
							return descriptor.value;
						case "path":
							return descriptor.value;
						case "keyText":
							return KeyCodec.encode(descriptor.value).toHex();
						case "keyB64":
							return KeyCodec.encode(decodeBase64Bytes(descriptor.value)).toHex();
						case "b64":
							return decodeBase64Bytes(descriptor.value).toHex();
					}
				}),
			),
		];
	}
	return descriptors.map((descriptor) => {
		switch (descriptor.kind) {
			case "val":
				return typeof descriptor.value === "boolean" ? Number(descriptor.value) : descriptor.value;
			case "path":
				return descriptor.value;
			case "keyText":
				return KeyCodec.encode(descriptor.value);
			case "keyB64":
				return KeyCodec.encode(decodeBase64Bytes(descriptor.value));
			case "b64":
				return decodeBase64Bytes(descriptor.value);
		}
	});
}
