import { HTTPException } from "hono/http-exception";
import type {
	BatchGetItemsResult,
	BatchWriteItemsResult,
	DeleteItemResult,
	FokosConditionCheckError,
	GetItemProjectedResult,
	GetItemResult,
	JsonValue,
	ProjectedItem,
	PutItemResult,
	QueryItemsProjectedResult,
	QueryItemsResult,
	ReadResultItem,
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

export function serializeGetItemResult(result: GetItemResult | GetItemProjectedResult) {
	return { ...result, ...(result.item === undefined ? {} : { item: serializeReadResultItem(result.item) }) };
}

// The old item of a write result, when the write asked for it, takes the encodeData step of a read.
export function serializeWriteResult(result: PutItemResult | DeleteItemResult) {
	return { ...result, ...(result.item === undefined ? {} : { item: serializeReadResultItem(result.item) }) };
}

export function serializeQueryItemsResult(result: QueryItemsResult | QueryItemsProjectedResult) {
	return { ...result, items: (result.items as ReadResultItem[]).map(serializeReadResultItem) };
}

// One encoder for the item of every read. `kind` tells a projected item from a complete item.
function serializeReadResultItem(item: ReadResultItem) {
	if (item.kind === "projected") {
		return { kind: item.kind, data: serializeProjectedRecord(item.data) };
	}
	const { data, hashKey, sortKey, ...rest } = item;
	// The HTTP surface is string-only for keys (every endpoint uses v.string()), so writes can
	// only produce UTF-8 keys and a scan can only decode strings back. A Uint8Array key here
	// means a binary key reached the store via the programmatic/RPC API — it would serialize to
	// `{"0":..}` over c.json. Fail loudly rather than emit broken JSON; binary keys over HTTP
	// would need a keyEncoding discriminator, not yet wired.
	if (hashKey instanceof Uint8Array || sortKey instanceof Uint8Array) {
		throw new HTTPException(500, { message: "fokos/read: binary keys are not supported over the HTTP API" });
	}
	return { ...rest, hashKey, sortKey, ...encodeData(data) };
}

// A projected record can hold a Uint8Array cell (a `b64` literal or byte data). It serializes as
// `{ b64 }`. Every other projected value is already JSON-serializable.
function serializeProjectedRecord(record: ProjectedItem): Record<string, unknown> {
	return Object.fromEntries(
		Object.entries(record).map(([name, value]) => [
			name,
			value instanceof Uint8Array ? { b64: Buffer.from(value).toString("base64") } : value,
		]),
	);
}

export function serializeTransactGetItemsResult(result: TransactGetItemsResult) {
	return {
		...result,
		items: result.items.map((entry) => (entry.item === undefined ? entry : { ...entry, item: serializeReadResultItem(entry.item) })),
	};
}

export function serializeBatchGetItemsResult(result: BatchGetItemsResult) {
	return {
		...result,
		items: result.items.map((entry) => (entry.item === undefined ? entry : { ...entry, item: serializeReadResultItem(entry.item) })),
	};
}

// The reason of a failed condition. Its old item image takes the same encodeData step that a read takes.
export function serializeConditionFailedReason(reason: FokosConditionCheckError["reason"]) {
	if (!reason.item) return reason;
	const { data, ...item } = reason.item;
	return { ...reason, item: { ...item, ...encodeData(data) } };
}

// The results of a cancelled transaction. A rejected entry can carry the old item image.
export function serializeTransactWriteResults(results: TransactWriteOperationResult[]) {
	return results.map((result) => {
		if (result.outcome !== "rejected" || result.reason.code !== "condition_failed") return result;
		return { ...result, reason: serializeConditionFailedReason(result.reason) };
	});
}

// A rejected batch write can carry the old item image of its failed condition.
export function serializeBatchWriteItemsResult(result: BatchWriteItemsResult) {
	return {
		...result,
		results: result.results.map((entry) =>
			entry.outcome === "rejected" && entry.reason ? { ...entry, reason: serializeConditionFailedReason(entry.reason) } : entry,
		),
	};
}
