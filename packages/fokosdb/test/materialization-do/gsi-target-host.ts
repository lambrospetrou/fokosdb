import { FokosDBMaterializationDO } from "../../src/server/materialization/do-materialization.js";
import type { FokosTargetDefinition } from "../../src/server/materialization/targets.js";

/**
 * A materialization class with two GSI targets over json items:
 * - `byStatus`: hash key `$.status`, sort key `$.at`, and the full item.
 * - `byOwner`: hash key `$.owner`, no sort key, and a projection of `$.status`.
 */
export class GsiMaterializationDO extends FokosDBMaterializationDO {
	protected override fokosTargets(): readonly FokosTargetDefinition[] {
		return [
			{ kind: "gsi", targetId: "byStatus", hashKey: { ref: "data", path: "$.status" }, sortKey: { ref: "data", path: "$.at" } },
			{
				kind: "gsi",
				targetId: "byOwner",
				hashKey: { ref: "data", path: "$.owner" },
				projection: [{ expr: { ref: "data", path: "$.status" }, as: "status" }],
			},
		];
	}
}
