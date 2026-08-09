/**
 * The message passed to `ctx.abort()` when a DO wipes itself as part of `FokosDB.destroy()`.
 *
 * `abort()` is the only way to evict the instance so the next caller constructs a fresh one and
 * re-runs the SQL migrations against the now-empty storage. It surfaces on the CALLING side as a
 * thrown error carrying this message, which the destroy path recognises and ignores.
 */
export const DESTROY_ABORT_SENTINEL = "__special_destroy_sentinel";

/** True for the error `ctx.abort(DESTROY_ABORT_SENTINEL)` raises on the caller side. */
export function isDestroyAbortError(e: unknown): boolean {
	return String(e).includes(DESTROY_ABORT_SENTINEL);
}

export type ColoInfo = {
	cfColo: string;
	cfLoc: string;
	cfFl: string;
};

// Module-scoped cache. In Cloudflare Workers, module top-level state is shared
// across every Durable Object instance that runs in the same V8 isolate, so once
// this is populated it is reused by all co-located DOs.
//
// We deliberately cache the *resolved value*, not the in-flight promise: a pending
// `fetch()` promise is bound to the I/O context of the request/DO that started it,
// and awaiting it from another DO throws "Cannot perform I/O on behalf of a
// different request". A plain resolved object has no I/O context, so it is safe to
// share. The tradeoff is that constructors racing before the first fetch resolves
// each do their own trace fetch — cheap and rare (once per isolate lifetime).
let coloInfo: ColoInfo | undefined;

/**
 * Returns the isolate's colo info, memoized across all DOs in the same isolate.
 *
 * Safe to call (without awaiting) from a DO constructor. On a cache miss it starts
 * a fetch scoped to the *calling* context — it never hands a foreign in-flight
 * promise to another DO — and populates the shared cache when it resolves. The colo
 * an isolate runs in is fixed for its lifetime, so the value is cached indefinitely.
 */
export async function getColoInfo(): Promise<ColoInfo> {
	if (coloInfo !== undefined) return coloInfo;
	return await fetchColoInfo().then((info) => {
		coloInfo = info;
		return info;
	});
}

export async function fetchColoInfo(): Promise<ColoInfo> {
	return await fetch("https://cloudflare.com/cdn-cgi/trace")
		.then(async (res) => await res.text())
		.then((text) => {
			// The trace endpoint returns newline-separated `key=value` pairs.
			const info: Record<string, string> = {};
			for (const line of text.split("\n")) {
				const idx = line.indexOf("=");
				if (idx === -1) continue;
				info[line.slice(0, idx)] = line.slice(idx + 1);
			}
			// `colo` is airport code, `loc` is the 2-letter country code: https://www.iso.org/obp/ui/#search
			// `fl` is an internal code, which is a finer-grained identifier than the airport code.
			// It is not documented, but it is used in Cloudflare's internal telemetry and logs and could be useful for debugging.
			return {
				cfColo: info.colo || "",
				cfLoc: info.loc || "",
				cfFl: info.fl || "",
			};
		});
}
