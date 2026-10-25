/**
 * The desired GSI entry of a base item: the GSI key and the entry data that the latest image gives,
 * from the compiled key and projection expressions of a GSI target.
 */
import { ExpressionError } from "../../shared/expression/errors.js";
import { compileProjectionExpression } from "../../shared/expression/compiler.js";
import type { CompiledProjectionPlan } from "../../shared/expression/plan.js";
import { projectedItemFromWireRow, type ProjectedWireCell } from "../../shared/expression/projection.js";
import { FokosError } from "../../shared/errors.js";
import {
	encodeHashKey,
	encodeSortKey,
	validateItemDataSize,
	validateKeyContent,
	type FokosDBLimits,
} from "../../shared/transaction-limits.js";
import type { DataKind } from "../../shared/types.js";
import { KeyCodec } from "../../sharding/key-codec.js";
import type { GsiKey, MaterializationStore, TargetImage } from "./materialization-store.js";
import type { GsiTargetDefinition } from "./targets.js";

/** The violation code of a GSI key that is not a non-empty string or a non-empty `Uint8Array`. */
export const GSI_KEY_TYPE_INVALID = "gsi_key_type_invalid";
/** The violation code of a projected `Uint8Array` value, which a json entry cannot store. */
export const GSI_PROJECTION_BYTES = "update_value_is_bytes";

/**
 * The codes of a failure that the data of the item causes, so that each resend fails again. Only these
 * park an item as a violation: a violation keeps the item out of the GSI until the next write of the
 * base item. Any other failure retries.
 */
export const GSI_VIOLATION_CODES: ReadonlySet<string> = new Set([
	GSI_KEY_TYPE_INVALID,
	GSI_PROJECTION_BYTES,
	"key_contains_nul",
	"key_not_well_formed_utf16",
	"hash_key_too_large",
	"sort_key_too_large",
	"item_data_too_large",
]);

export type GsiDesiredEntry =
	| { kind: "absent" }
	| { kind: "present"; key: GsiKey; data: string | Uint8Array; dataKind: DataKind; ttlAt: number | null }
	| { kind: "violation"; code: string };

export type GsiPlans = { keys: CompiledProjectionPlan; projection: CompiledProjectionPlan | undefined };

export function compileGsiPlans(target: GsiTargetDefinition): GsiPlans {
	return {
		keys: compileProjectionExpression([
			{ expr: target.hashKey, as: "hk" },
			...(target.sortKey === undefined ? [] : [{ expr: target.sortKey, as: "sk" }]),
		]),
		projection: target.projection === undefined ? undefined : compileProjectionExpression(target.projection),
	};
}

/**
 * The desired entry of an item. A tombstone, a missing or `null` key attribute, and a GSI with a sort
 * key whose attribute is missing or `null` give no entry. A key that is not valid, a projected
 * `Uint8Array`, and entry data above the item limit are violations.
 */
export function desiredGsiEntry(store: MaterializationStore, plans: GsiPlans, image: TargetImage, limits: FokosDBLimits): GsiDesiredEntry {
	if (image.deleted) {
		return { kind: "absent" };
	}
	try {
		const keys = store.evaluateProjection(plans.keys, image.hk, image.sk);
		if (keys === undefined) {
			return { kind: "absent" };
		}
		const [hkCell, skCell] = keys;
		const hasSortKey = plans.keys.names.length > 1;
		if (isAbsent(hkCell) || (hasSortKey && isAbsent(skCell))) {
			return { kind: "absent" };
		}
		const hkValue = keyValue(hkCell, "hashKey");
		const skValue = hasSortKey ? keyValue(skCell, "sortKey") : undefined;
		if (hkValue === undefined || (hasSortKey && skValue === undefined)) {
			return { kind: "violation", code: GSI_KEY_TYPE_INVALID };
		}
		const key: GsiKey = { hk: encodeHashKey(hkValue, limits), sk: encodeSortKey(skValue, limits) };
		let data: string | Uint8Array = image.data;
		let dataKind: DataKind = image.kind;
		if (plans.projection !== undefined) {
			const row = store.evaluateProjection(plans.projection, image.hk, image.sk) ?? [];
			if (row.some((cell) => cell instanceof Uint8Array)) {
				return { kind: "violation", code: GSI_PROJECTION_BYTES };
			}
			data = JSON.stringify(projectedItemFromWireRow(plans.projection.names, row));
			dataKind = "json";
		}
		validateItemDataSize(data, "gsiWriteEntries");
		return { kind: "present", key, data, dataKind, ttlAt: image.ttlAt };
	} catch (error) {
		if ((error instanceof FokosError || error instanceof ExpressionError) && GSI_VIOLATION_CODES.has(String(error.code))) {
			return { kind: "violation", code: String(error.code) };
		}
		throw error;
	}
}

function isAbsent(cell: ProjectedWireCell): boolean {
	return cell === undefined || cell === null;
}

/** The key value of a cell, or undefined when its type cannot be a key. Throws on key content that is not valid. */
function keyValue(cell: ProjectedWireCell, name: "hashKey" | "sortKey"): string | Uint8Array | undefined {
	if (typeof cell === "string" && cell.length > 0) {
		validateKeyContent(name, cell);
		return cell;
	}
	if (cell instanceof Uint8Array && cell.length > 0) {
		return cell;
	}
	return undefined;
}

export function sameGsiKey(a: GsiKey, b: GsiKey): boolean {
	return KeyCodec.compare(a.hk, b.hk) === 0 && KeyCodec.compare(a.sk, b.sk) === 0;
}
