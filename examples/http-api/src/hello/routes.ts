import { Hono } from "hono";
import type { AppEnv } from "../shared.js";

export const helloRoutes = new Hono<AppEnv>();

helloRoutes.get("/:name", async (c) => {
	const name = c.req.param("name");
	return c.json({ message: `Hello, ${name}!` });
});
