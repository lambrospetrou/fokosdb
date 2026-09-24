import { Hono } from "hono";
import { counterRoutes } from "./counter-routes.js";
import { searchRoutes } from "./search-routes.js";

/**
 * The routes of the sharding demo suite. Each demo has its own routes under `/api/demo2/<demo>`:
 *
 * - `GET /api/demo2/<demo>/topology` gives the partition tree that the UI draws.
 * - `POST /api/demo2/<demo>/<action>` runs one button of the control panel.
 *
 * `public/demo2/index.html` is the UI. Wrangler serves it as a static asset.
 */

export { CounterPartitionDO } from "./counter-host.js";
export { SearchPartitionDO } from "./search-host.js";

export const demo2Routes = new Hono<{ Bindings: Env }>();

demo2Routes.route("/demo1", counterRoutes);
demo2Routes.route("/demo2", searchRoutes);
