import { FokosError } from "../errors.js";
import type { PartitionInfoInternal } from "./types.js";

/**
 * An error that carries the routing meta of the partition that raised it, as the own data property
 * `meta`, so the meta crosses an RPC hop. Only `stampRoutingMeta` writes the field.
 */
export type RoutedError = FokosError & { meta: PartitionInfoInternal };

export function stampRoutingMeta(err: FokosError, meta: PartitionInfoInternal): RoutedError {
	return Object.assign(err, { meta });
}

/**
 * `e` as a `RoutedError`, or undefined when it carries no routing meta. A hop keeps own properties and
 * only `stampRoutingMeta` writes `meta`, so the type holds after a hop, as the type of a result meta does.
 */
export function routedError(e: unknown): RoutedError | undefined {
	return FokosError.is(e) && "meta" in e && e.meta !== undefined ? (e as RoutedError) : undefined;
}

/**
 * The meta change that one forwarding level applies to the meta of its target: one more forward, and
 * the hash depth of the forwarding partition when `hashDepth` is set. The success path applies it to
 * the meta of a result, and the error path to the meta of a `RoutedError`, so the two metas stay equal
 * for the same route.
 */
export function forwardedMeta<M extends PartitionInfoInternal>(meta: M, hashDepth?: number): M {
	return { ...meta, forwardCount: meta.forwardCount + 1, ...(hashDepth === undefined ? {} : { hashDepth }) };
}

/**
 * The error path of one forwarding level. When `e` carries a routing meta, this learns from it as the
 * success path learns from the meta of a result, then applies `forwardedMeta` to it.
 *
 * The learning is best effort: a failure in it must never replace the error that the caller rethrows.
 * A lost cache update costs one extra hop later.
 */
export function learnFromErrorMeta(e: unknown, learn: (meta: PartitionInfoInternal) => void, hashDepth?: number): void {
	const err = routedError(e);
	if (!err) return;
	try {
		learn(err.meta);
	} catch {}
	err.meta = forwardedMeta(err.meta, hashDepth);
}
