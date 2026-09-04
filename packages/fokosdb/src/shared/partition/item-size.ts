/**
 * The stored shape of one `items` row: the on-disk json code, the SQL that produces the stored
 * `data` value, and the est_row_bytes formula measured over it.
 *
 * It lives on its own because two modules need the same three definitions: `partition-store.ts`
 * writes the rows, and `expression/runtime.ts` measures a compiled update against them. Importing
 * one from the other would make the store and the expression runtime mutually dependent.
 */

import { DATA_KINDS, type DataKind } from "../types.js";

// The on-disk `data_kind` code for json rows. json is stored as JSONB (a BLOB); a public read must
// decode it to JSON text in SQL (`json(data)`) so JS never touches raw JSONB, while a migration read
// copies the JSONB blob verbatim. This fixed integer is safe to interpolate into SQL.
export const JSON_KIND_CODE = DATA_KINDS.indexOf("json");

// Fixed per-row overhead added to est_row_bytes. See the "items" migration for what K covers in PartitionStore.
export const EST_ROW_BYTES_K = 100;

/**
 * The ONLY definition of the est_row_bytes formula. `dataExpr` must be the same SQL that produces the
 * stored `data` value (e.g. `jsonb(?6)`), so the size is always measured on what SQLite writes.
 */
export function estRowBytesExpr(dataExpr: string, hkParam: string, skParam: string): string {
	return `octet_length(${dataExpr}) + octet_length(${hkParam}) + octet_length(${skParam}) + ${EST_ROW_BYTES_K}`;
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
