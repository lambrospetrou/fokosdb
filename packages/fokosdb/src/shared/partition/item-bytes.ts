/**
 * The stored shape of one item: the on-disk json code, the SQL that produces the stored `data` value,
 * and the `item_bytes` definition measured over it. The client bundle imports this module through the
 * expression plan, so it holds no storage model code.
 */

import { DATA_KINDS, type DataKind } from "../types.js";

// The on-disk `data_kind` code for json rows. json is stored as JSONB (a BLOB); a public read must
// decode it to JSON text in SQL (`json(data)`) so JS never touches raw JSONB, while a migration read
// copies the JSONB blob verbatim. This fixed integer is safe to interpolate into SQL.
export const JSON_KIND_CODE = DATA_KINDS.indexOf("json");

/**
 * The ONLY definition of `item_bytes`: the encoded bytes of the stored data, the hash key, and the
 * sort key. `dataExpr` must be the same SQL that produces the stored `data` value (e.g. `jsonb(?6)`),
 * so the size is always measured on what SQLite writes. `null` gives the size of an item with no
 * data, a tombstone.
 */
export function itemBytesExpr(dataExpr: string | null, hkExpr: string, skExpr: string): string {
	const keys = `octet_length(${hkExpr}) + octet_length(${skExpr})`;
	return dataExpr === null ? keys : `octet_length(${dataExpr}) + ${keys}`;
}

/**
 * The SQL that turns a bound parameter into the stored `data` value.
 *
 * `kind` alone cannot decide it, because a json value reaches a write site in either representation:
 * JSON text from a client put, or a raw JSONB blob from a commit apply and a migration ingest. A
 * `bytes` value is also a Uint8Array, so the pair of the kind and the JavaScript type is the rule.
 * The result is a fixed fragment chosen by that pair, never user input, so it is injection-safe.
 */
export function itemDataExpr(kind: DataKind, data: string | Uint8Array, param: string): string {
	return kind === "json" && typeof data === "string" ? `jsonb(${param})` : param;
}
