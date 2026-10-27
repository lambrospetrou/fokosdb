/**
 * The target definitions of `FokosDBMaterializationDO` and the wire types of a push target.
 */
import type { ExpressionValue, ProjectionExpression } from "../../shared/expression/types.js";
import invariant from "../../shared/invariant.js";
import { analyzeExpressionValue, validateProjectionExpression } from "../../shared/expression/semantic.js";
import type { HashKey, ReadItemValue, SortKey } from "../../shared/types.js";

export type GsiTargetDefinition = {
	kind: "gsi";
	targetId: string;
	hashKey: ExpressionValue;
	sortKey?: ExpressionValue;
	projection?: readonly ProjectionExpression[];
};

export type PushSinkTargetDefinition = {
	kind: "push_sink";
	targetId: string;
	/** Name of the Worker env binding: a service binding (`WorkerEntrypoint`) or a `DurableObjectNamespace`. */
	binding: string;
	/** RPC method name that receives a `PushTargetBatch`. */
	method: string;
	/** Target DO name when `binding` is a `DurableObjectNamespace`. */
	objectName?: string;
};

export type FokosTargetDefinition = GsiTargetDefinition | PushSinkTargetDefinition;

export type PushTargetItem = {
	baseHashKey: HashKey;
	/** Absent when the base item has no sort key. */
	baseSortKey?: SortKey;
	revision: number;
	deleted: boolean;
	/** The complete item. Absent when `deleted` is true. */
	value?: ReadItemValue;
	/** Epoch UTC seconds. Absent when the item has no TTL or when `deleted` is true. */
	ttlAt?: number;
};

export type PushTargetBatch = {
	/** The `DurableObjectId` of the sending `FokosDBMaterializationDO`, as `DurableObjectId.toString()` gives it (hex). */
	producerId: string;
	epoch: number;
	ancestorFences: readonly { ancestorId: string; minEpoch: number }[];
	items: readonly PushTargetItem[];
};

/** One result for each item of the batch, in batch order. */
export type PushTargetResult = { outcome: "applied" } | { outcome: "retry"; code?: string } | { outcome: "rejected"; code: string };

/** The most GSI targets of one table. */
const MAX_GSI_TARGETS = 10;

/**
 * Throws when the target list is not valid. A `targetId` is not empty, has no `~`, and is unique.
 * The expressions of a GSI target are valid, and no projection value of a GSI target is only bytes.
 * Push projections are not supported yet.
 */
export function validateTargets(targets: readonly FokosTargetDefinition[]): void {
	const ids = new Set<string>();
	let gsiCount = 0;
	for (const target of targets) {
		invariant(
			typeof target.targetId === "string" && target.targetId.length > 0 && !target.targetId.includes("~"),
			() => `fokos/mat: the targetId ${JSON.stringify(target.targetId)} must be a non-empty string with no "~"`,
		);
		invariant(!ids.has(target.targetId), () => `fokos/mat: the targetId ${target.targetId} is not unique`);
		ids.add(target.targetId);
		switch (target.kind) {
			case "gsi":
				gsiCount += 1;
				invariant(gsiCount <= MAX_GSI_TARGETS, () => `fokos/mat: a table has at most ${MAX_GSI_TARGETS} GSI targets`);
				analyzeExpressionValue(target.hashKey, "projection");
				if (target.sortKey !== undefined) {
					analyzeExpressionValue(target.sortKey, "projection");
				}
				if (target.projection !== undefined) {
					validateProjectionExpression(target.projection, { jsonDocument: true });
				}
				break;
			case "push_sink":
				invariant(
					typeof target.binding === "string" && target.binding.length > 0,
					() => `fokos/mat: the push target ${target.targetId} has no binding`,
				);
				invariant(
					typeof target.method === "string" && target.method.length > 0,
					() => `fokos/mat: the push target ${target.targetId} has no method`,
				);
				invariant(
					target.objectName === undefined || (typeof target.objectName === "string" && target.objectName.length > 0),
					() => `fokos/mat: the objectName of the push target ${target.targetId} must be a non-empty string`,
				);
				invariant(
					!("projection" in target) || (target as { projection?: unknown }).projection === undefined,
					() => `fokos/mat: the push target ${target.targetId} has a projection, which is not supported yet`,
				);
				break;
			default:
				invariant(false, () => `fokos/mat: the target kind ${String((target as { kind?: unknown }).kind)} is not known`);
		}
	}
}
