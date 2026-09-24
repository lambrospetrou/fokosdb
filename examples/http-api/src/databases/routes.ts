import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import * as v from "valibot";
import { makeFokosDB, PartitionOptionsSchema, type AppEnv, type PartitionOptionsInput } from "../shared.js";

export const databasesRoutes = new Hono<AppEnv>();

databasesRoutes.delete("/:tableName", async (c) => {
	const tableName = c.req.param("tableName");
	let partitionOptions: PartitionOptionsInput | undefined;
	try {
		const body = await c.req.json();
		const result = v.safeParse(v.object({ partitionOptions: PartitionOptionsSchema }), body);
		if (!result.success) {
			throw new HTTPException(400, {
				message: JSON.stringify({ error: "Validation failed", issues: v.flatten(result.issues) }),
			});
		}
		partitionOptions = result.output.partitionOptions;
	} catch (e) {
		if (e instanceof HTTPException) throw e;
		// No body or non-JSON body is fine — use defaults.
	}
	await makeFokosDB(c.env, tableName, partitionOptions).destroy();
	return c.json({ destroyed: true });
});
