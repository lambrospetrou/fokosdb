import type { ProjectedWireRow } from "../expression/projection.js";
import { wireBytes } from "../transaction-limits.js";
import type { ReadForTransactionItemResultEncoded } from "../transaction-wire-types.js";
import type { StoredItem } from "./partition-store.js";

/**
 * The bytes the RPC serialization adds around one item, complete item or projected row, on top of
 * the item's own key, data, and cell bytes. It stands for the object or array itself, its property
 * names or cell slots, and one type tag and one length prefix per field or cell. It is an allowance,
 * not a measurement: a StoredItem has 8 named fields and a projected row has at most
 * `EXPRESSION_LIMITS.projectionEntries` cells, and 64 bytes is in the range both produce.
 *
 * The estimate only feeds the response-byte budget, which keeps one RPC response under
 * `WORKERS_RPC_MAX_BYTES`. `MAX_RESPONSE_BYTES_PER_PAGE` (half of it) leaves a 2x margin: even an error of 64
 * bytes per item on a page of 100,000 tiny items is about 6 MiB, inside that margin. Change this
 * value only when the per-item wire shape grows well past what it stands for (many more named
 * fields, or a much larger cell limit) or when that margin shrinks. Changing it moves page boundaries
 * only; a cursor does not depend on it.
 */
const ITEM_ENVELOPE_BYTES = 64;

export function estimateItemBytes(item: StoredItem): number {
	const dataSize = typeof item.data === "string" ? item.data.length * 2 : item.data.byteLength;
	return item.hk.byteLength + item.sk.byteLength + dataSize + 16 + ITEM_ENVELOPE_BYTES;
}

/** Estimated RPC bytes of one item of a `transactGetItems` answer, as `estimateItemBytes` counts them. */
export function estimateReadItemBytes(item: ReadForTransactionItemResultEncoded): number {
	const keys = item.hashKey.byteLength + item.sortKey.byteLength;
	if (!item.found) {
		return keys + 16 + ITEM_ENVELOPE_BYTES;
	}
	if (item.kind === "projected") {
		return keys + estimateProjectedRowBytes(item.projected);
	}
	return keys + wireBytes(item.data) + 16 + ITEM_ENVELOPE_BYTES;
}

/**
 * Estimated RPC bytes of one projected row: the item envelope plus per-cell sizes, `length * 2`
 * for a string and for the JSON text of an array or an object, `byteLength` for a `Uint8Array`, and
 * 8 for every other cell. It does not use `item_bytes`: a projected row carries only its cells.
 */
export function estimateProjectedRowBytes(row: ProjectedWireRow): number {
	let bytes = ITEM_ENVELOPE_BYTES;
	for (const cell of row) {
		if (typeof cell === "string") {
			bytes += cell.length * 2;
		} else if (cell instanceof Uint8Array) {
			bytes += cell.byteLength;
		} else if (cell !== null && typeof cell === "object") {
			bytes += cell.json.length * 2;
		} else {
			bytes += 8;
		}
	}
	return bytes;
}
