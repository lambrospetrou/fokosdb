import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import * as v from "valibot";
import type { QueryItemsProjectedOptions } from "fokosdb/client";
import { makeFokosDB, type AppEnv } from "../shared.js";
import {
	DeleteItemBodySchema,
	GetItemBodySchema,
	PutItemBodySchema,
	QueryItemsBodySchema,
	TransactGetItemsBodySchema,
	TransactWriteItemsBodySchema,
} from "./schemas.js";
import {
	serializeGetItemResult,
	serializeProjectedQueryItemsResult,
	serializeQueryItemsResult,
	serializeTransactGetItemsResult,
} from "./serialize.js";

export const rpcRoutes = new Hono<AppEnv>();

rpcRoutes.post("/:tableName/:rpcAction", async (c) => {
	const tableName = c.req.param("tableName");
	const rpcAction = c.req.param("rpcAction");

	let rawBody: unknown;
	try {
		rawBody = await c.req.json();
	} catch {
		throw new HTTPException(400, { message: "Invalid JSON body" });
	}

	function parseBody<S extends v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>(schema: S): v.InferOutput<S> {
		const result = v.safeParse(schema, rawBody);
		if (!result.success) {
			throw new HTTPException(400, {
				message: JSON.stringify({ error: "Validation failed", issues: v.flatten(result.issues) }),
			});
		}
		return result.output as v.InferOutput<S>;
	}

	// TODO: This builds a new FokosDB instance on every request, which rebuilds the partition topology
	// every time. Cache the instances by tableName + partitionOptions.
	switch (rpcAction) {
		case "putItem": {
			const { partitionOptions, ...opts } = parseBody(PutItemBodySchema);
			const result = await makeFokosDB(c.env, tableName, partitionOptions).putItem(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(result);
		}
		case "getItem": {
			const { partitionOptions, ...opts } = parseBody(GetItemBodySchema);
			const result = await makeFokosDB(c.env, tableName, partitionOptions).getItem(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(serializeGetItemResult(result));
		}
		case "deleteItem": {
			const { partitionOptions, ...opts } = parseBody(DeleteItemBodySchema);
			const result = await makeFokosDB(c.env, tableName, partitionOptions).deleteItem(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(result);
		}
		case "transactWriteItems": {
			const { partitionOptions, ...opts } = parseBody(TransactWriteItemsBodySchema);
			return c.json(await makeFokosDB(c.env, tableName, partitionOptions).transactWriteItems(opts));
		}
		case "transactGetItems": {
			const { partitionOptions, ...opts } = parseBody(TransactGetItemsBodySchema);
			return c.json(serializeTransactGetItemsResult(await makeFokosDB(c.env, tableName, partitionOptions).transactGetItems(opts)));
		}
		case "queryItems": {
			const { partitionOptions, ...opts } = parseBody(QueryItemsBodySchema);
			if (opts.projection !== undefined) {
				const result = await makeFokosDB(c.env, tableName, partitionOptions).queryItems({
					...opts,
					projection: opts.projection,
				} as QueryItemsProjectedOptions);
				c.set("dbItemMeta", result.meta);
				return c.json(serializeProjectedQueryItemsResult(result));
			}
			const result = await makeFokosDB(c.env, tableName, partitionOptions).queryItems(opts);
			c.set("dbItemMeta", result.meta);
			return c.json(serializeQueryItemsResult(result));
		}
		default:
			throw new HTTPException(404, { message: `Unknown rpcAction: ${rpcAction}` });
	}
});
