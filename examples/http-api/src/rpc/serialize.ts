import { HTTPException } from "hono/http-exception";
import type {
	GetItemResult,
	JsonValue,
	ProjectedItem,
	QueryItemsProjectedResult,
	QueryItemsResult,
	ReadItemValue,
	TransactGetItemsResult,
	TransactWriteOperationResult,
} from "fokosdb/client";

// The HTTP write surface only accepts string `data` (PutItemBodySchema), so over HTTP items are always
// text; but a json/bytes row created via the programmatic API can still be read back here, so all three
// kinds are serialized. json values are re-stringified with a distinct `dataEncoding` discriminant.
function encodeData(data: string | Uint8Array | JsonValue): { data: string; dataEncoding: "utf8" | "base64" | "json" } {
	if (data instanceof Uint8Array) {
		return { data: Buffer.from(data).toString("base64"), dataEncoding: "base64" };
	}
	if (typeof data === "string") {
		return { data, dataEncoding: "utf8" };
	}
	return { data: JSON.stringify(data), dataEncoding: "json" };
}

export function serializeGetItemResult(result: GetItemResult) {
	if (!result.found) return result;
	const { data: _data, ...itemRest } = result.item;
	return { ...result, item: { ...itemRest, ...encodeReadItemData(result.item) } };
}

export function serializeQueryItemsResult(result: QueryItemsResult) {
	return {
		...result,
		items: result.items.map((item) => {
			const { data: _data, hashKey, sortKey, ...rest } = item;
			// The HTTP surface is string-only for keys (every endpoint uses v.string()), so writes can
			// only produce UTF-8 keys and a scan can only decode strings back. A Uint8Array key here
			// means a binary key reached the store via the programmatic/RPC API — it would serialize to
			// `{"0":..}` over c.json. Fail loudly rather than emit broken JSON; binary keys over HTTP
			// would need a keyEncoding discriminator, not yet wired.
			if (hashKey instanceof Uint8Array || sortKey instanceof Uint8Array) {
				throw new HTTPException(500, { message: "fokos/queryItems: binary keys are not supported over the HTTP API" });
			}
			return { ...rest, hashKey, sortKey, ...encodeReadItemData(item) };
		}),
	};
}

// One encoder for every read result: `getItem`, `queryItems` and `transactGetItems` return one item
// envelope, so each of them serializes its value the same way. A projected record gets its own
// `dataEncoding`, and every stored kind takes encodeData.
function encodeReadItemData(item: ReadItemValue): {
	data: string | Record<string, unknown>;
	dataEncoding: "utf8" | "base64" | "json" | "projected";
} {
	return item.kind === "projected" ? { data: serializeProjectedItem(item.data), dataEncoding: "projected" } : encodeData(item.data);
}

// A projected record can hold a Uint8Array cell (a `b64` literal or byte data). It serializes as
// `{ b64 }`. Every other projected value is already JSON-serializable.
function serializeProjectedItem(item: ProjectedItem): Record<string, unknown> {
	return Object.fromEntries(
		Object.entries(item).map(([name, value]) => [
			name,
			value instanceof Uint8Array ? { b64: Buffer.from(value).toString("base64") } : value,
		]),
	);
}

export function serializeProjectedQueryItemsResult(result: QueryItemsProjectedResult) {
	return { ...result, items: result.items.map(serializeProjectedItem) };
}

export function serializeTransactGetItemsResult(result: TransactGetItemsResult) {
	return {
		...result,
		items: result.items.map((item) => {
			if (!item.found) return item;
			const { data: _data, ...rest } = item;
			return { ...rest, ...encodeReadItemData(item) };
		}),
	};
}

// The results of a cancelled transaction. A rejected entry can carry the old item image, whose data
// takes the same encodeData step that a read takes.
export function serializeTransactWriteResults(results: TransactWriteOperationResult[]) {
	return results.map((result) => {
		if (result.outcome !== "rejected" || result.reason.code !== "condition_failed" || !result.reason.item) return result;
		const { data, ...item } = result.reason.item;
		return { ...result, reason: { ...result.reason, item: { ...item, ...encodeData(data) } } };
	});
}
