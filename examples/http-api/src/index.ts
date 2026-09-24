import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import { FokosError, FokosTransactionCancelledError } from "fokosdb/client";
import { PartitionDO } from "fokosdb/server";
import { databasesRoutes } from "./databases/routes.js";
import { demo2Routes } from "./demo2/index.js";
import { helloRoutes } from "./hello/routes.js";
import { rpcRoutes } from "./rpc/routes.js";
import { serializeTransactWriteResults } from "./rpc/serialize.js";
import type { AppEnv } from "./shared.js";

// Wrangler resolves Durable Object bindings against this module, so the classes must be re-exported
// from the worker entry even though the implementations live in the library.
export { PartitionDO, TransactionCoordinatorDO } from "fokosdb/server";
export { CounterPartitionDO, SearchPartitionDO } from "./demo2/index.js";

const api = new Hono<AppEnv>().basePath("/api");

let cachedValidTokens: Set<string> | null = null;

api.onError((err, c) => {
	if (err instanceof HTTPException) {
		return err.getResponse();
	}
	// Every error that FokosDB raises carries its category, its code, its error_id and an HTTP status hint.
	if (FokosError.is(err)) {
		if (err.origin === "internal") {
			console.error({ message: "FokosDB internal error", error: String(err), errorProps: err });
		}
		return c.json(
			{
				error: err._tag,
				code: err.code,
				error_id: err.error_id,
				message: err.message,
				...(FokosTransactionCancelledError.is(err) ? { results: serializeTransactWriteResults(err.results) } : {}),
			},
			err.httpStatusHint as ContentfulStatusCode,
		);
	}
	console.error({
		message: "Unexpected error in catch-all",
		error: String(err),
		errorProps: err,
	});
	return c.json({ error: "Internal Server Error" }, 500);
});

api.use(async (c, next) => {
	const token = c.req.header("x-fokos-secret-token");
	if (!token) {
		throw new HTTPException(401, { message: "Missing x-fokos-secret-token header" });
	}
	cachedValidTokens ??= new Set(
		c.env.FOKOS_API_TOKENS.split(",")
			.map((t) => t.trim())
			.filter(Boolean),
	);
	const validTokens = cachedValidTokens;
	if (!validTokens.has(token)) {
		throw new HTTPException(401, { message: "Invalid token" });
	}
	await next();
});

api.use(async (c, next) => {
	const start = Date.now();
	await next();
	const durationMs = Date.now() - start;
	c.header("Server-Timing", `worker;dur=${durationMs}`);
	console.log({
		message: `${c.req.method} ${c.req.path} - ${c.res.status}`,
		status: c.res.status,
		path: c.req.path,
		durationMs,
		dbItemMeta: c.get("dbItemMeta"),
	});
});

api.route("/hello", helloRoutes);
api.route("/databases", databasesRoutes);
api.route("/rpc", rpcRoutes);
api.route("/demo2", demo2Routes);

export default {
	async fetch(request, env, ctx): Promise<Response> {
		return api.fetch(request, env, ctx);
	},
} satisfies ExportedHandler<Env>;

// TESTING THE PartitionDO override capabilities.

export class CustomPartitionDO extends PartitionDO {}
