import { Hono } from "hono";
import { counterRoutes } from "./counter-routes.js";
import { fokosdbRoutes } from "./fokosdb-routes.js";
import { searchRoutes } from "./search-routes.js";

/**
 * The Worker of the demo suite. Each demo has its own routes under `/api/<demo>`:
 *
 * - `GET /api/<demo>/topology` gives the partition tree that the UI draws.
 * - `POST /api/<demo>/<action>` runs one button of the control panel.
 *
 * `public/index.html` is the UI. Wrangler serves it as a static asset.
 */

export { PartitionDO, TransactionCoordinatorDO } from "fokosdb/server";
export { CounterPartitionDO } from "./counter-host.js";
export { SearchPartitionDO } from "./search-host.js";

const app = new Hono<{ Bindings: Env }>().basePath("/api");

app.get("/health", (c) => c.json({ status: "ok" }));
app.route("/demo1", counterRoutes);
app.route("/demo2", searchRoutes);
app.route("/demo3", fokosdbRoutes);

export default app;
