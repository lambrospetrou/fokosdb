import type { FokosCompositeKey, KeyBytes } from "../sharding/key-codec.js";
import type { FokosRuntimeInspect } from "../sharding/runtime-types.js";
import { ROUTING_HASH_BYTES } from "../sharding/hash-primitives.js";
import { FokosValidationError, VALIDATION_CODES } from "./errors.js";

export type FokosInspectKind = "items" | "locks" | "transactions" | "repartition" | "jobs" | "selfCheck";

/**
 * A read of the protocol state of one Durable Object. `items`, `locks`, `transactions` and
 * `selfCheck` are paged. A request with `cursor` or `limit` names exactly one paged kind.
 */
export type FokosInspectRequest = {
	what: FokosInspectKind[];
	hashKey?: KeyBytes;
	range?: { start?: FokosCompositeKey; end?: FokosCompositeKey };
	transactionId?: string;
	cursor?: string;
	limit?: number;
};

export type FokosInspectItem = { hashKey: KeyBytes; sortKey: KeyBytes; version: number; data: Uint8Array | string; expiresAt?: number };
export type FokosInspectLock = { transactionId: string; hashKey: KeyBytes; sortKey: KeyBytes; op: string };
export type FokosInspectTransaction = { transactionId: string; state: string; participants: string[] };

export type FokosInspectResponse = {
	/** Committed state only. */
	items?: FokosInspectItem[];
	locks?: FokosInspectLock[];
	transactions?: FokosInspectTransaction[];
	repartition?: FokosRuntimeInspect["repartition"];
	jobs?: FokosRuntimeInspect["jobs"];
	selfCheck?: { violations: string[] };
	/** The position after the page of the paged kind. Absent when the page is the last one. */
	cursor?: string;
};

export type InspectPagedKind = "items" | "locks" | "transactions" | "selfCheck";

/**
 * One page of a paged kind, strictly after `after`. `next` is the position of the last row that the
 * page read, or null when no row follows. A position is a list of strings: the hex of a key, or a text.
 */
export type InspectPageReader<T> = (after: string[] | null, limit: number) => { entries: T[]; next: string[] | null };

export type InspectPageReaders = {
	items?: InspectPageReader<FokosInspectItem>;
	locks?: InspectPageReader<FokosInspectLock>;
	transactions?: InspectPageReader<FokosInspectTransaction>;
	selfCheck?: InspectPageReader<string>;
};

const PAGED_KINDS: readonly InspectPagedKind[] = ["items", "locks", "transactions", "selfCheck"];
/**
 * The bytes of one page of items, counted as two bytes for each stored byte plus the keys: a string can
 * go on the wire as two bytes for each character. It keeps a page below the RPC limit of 32 MiB.
 */
export const INSPECT_PAGE_BYTES = 16 * 1024 * 1024;
/** The most rows that one page of items or locks reads. A page can hold no entry and still have a cursor. */
export const INSPECT_SCAN_ROWS = 10_000;
/** The items that one page of `selfCheck` counts, by the stored facts. A page checks at least one bucket. */
export const INSPECT_CHECK_ITEMS = 50_000;
/** The routingHashes that one page of `selfCheck` checks for a missing bucket. */
export const INSPECT_CHECK_ROUTING_HASHES = 1_000;
/** The most violations that one check of `selfCheck` that is not paged returns. */
export const INSPECT_CHECK_LIMIT = 100;
const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1_000;

const KINDS: readonly FokosInspectKind[] = ["items", "locks", "transactions", "repartition", "jobs", "selfCheck"];
type CursorPart = "routingHash" | "hex" | "text";
/** The forms of the cursor position of each paged kind. Each part is the hex of a routingHash or a key, or a text. */
const CURSOR_PARTS: Record<InspectPagedKind, readonly (readonly CursorPart[])[]> = {
	items: [["routingHash", "hex", "hex"]],
	locks: [["routingHash", "hex", "hex", "text"]],
	transactions: [["hex", "text"]],
	// A bucket, or a routingHash after the last bucket.
	selfCheck: [["routingHash", "hex", "hex"], ["routingHash"]],
};

function isCursorPart(part: unknown, form: CursorPart): boolean {
	if (typeof part !== "string") {
		return false;
	}
	switch (form) {
		case "text":
			return true;
		case "hex":
			return HEX.test(part);
		case "routingHash":
			return HEX.test(part) && part.length === 2 * ROUTING_HASH_BYTES;
	}
}
const HEX = /^(?:[0-9a-f]{2})*$/;

/** A request that `parseInspect` accepted. */
export type ParsedInspect = { paged: InspectPagedKind[]; limit: number; after: string[] | null };

function invalid(message: string): FokosValidationError {
	return new FokosValidationError(VALIDATION_CODES.inspect_request_invalid, { message });
}

/**
 * Validates a request before any read. `kinds` are the paged kinds that the class reads, and `filters`
 * are the filters that it accepts.
 */
export function parseInspect(
	req: FokosInspectRequest,
	kinds: readonly InspectPagedKind[],
	filters: readonly ("hashKey" | "range" | "transactionId")[],
): ParsedInspect {
	if (!Array.isArray(req.what) || !req.what.every((kind) => KINDS.includes(kind))) {
		throw invalid(`what must be a list of ${KINDS.join(", ")}`);
	}
	for (const filter of ["hashKey", "range", "transactionId"] as const) {
		if (req[filter] !== undefined && !filters.includes(filter)) {
			throw invalid(`this Durable Object has no ${filter} filter`);
		}
	}
	const paged = PAGED_KINDS.filter((kind) => req.what.includes(kind));
	for (const kind of paged) {
		// Each class has a `selfCheck`. Only some classes page it.
		if (!kinds.includes(kind) && kind !== "selfCheck") {
			throw invalid(`this Durable Object has no ${kind}`);
		}
	}
	if ((req.cursor !== undefined || req.limit !== undefined) && paged.length !== 1) {
		throw invalid("a request with a cursor or a limit must name exactly one paged kind");
	}
	if (req.limit !== undefined && (!Number.isInteger(req.limit) || req.limit < 1 || req.limit > MAX_LIMIT)) {
		throw invalid(`limit must be an integer from 1 to ${MAX_LIMIT}`);
	}
	return { paged, limit: req.limit ?? DEFAULT_LIMIT, after: req.cursor === undefined ? null : parseCursor(req.cursor, paged[0]) };
}

/**
 * The answer of `fokosInspect`. `readers` holds the paged kinds of the class, and `checks` gives the
 * violations of `selfCheck` that are not paged. They go on the first page only.
 */
export function answerInspect(
	req: FokosInspectRequest,
	{ paged, limit, after }: ParsedInspect,
	runtime: FokosRuntimeInspect | null,
	readers: InspectPageReaders,
	checks: () => string[],
): FokosInspectResponse {
	const res: FokosInspectResponse = {};
	if (req.what.includes("repartition")) {
		res.repartition = runtime?.repartition ?? { role: "leaf" };
	}
	if (req.what.includes("jobs")) {
		res.jobs = runtime?.jobs ?? [];
	}
	if (runtime === null) {
		// No identity: the object holds no state.
		for (const kind of paged) {
			if (kind === "selfCheck") {
				res.selfCheck = { violations: [] };
			} else {
				res[kind] = [];
			}
		}
		return res;
	}
	for (const kind of paged) {
		const reader = readers[kind];
		const page = (reader ? reader(after, limit) : { entries: [], next: null }) as { entries: never[]; next: string[] | null };
		if (kind === "selfCheck") {
			const first = after === null ? [...runtime.alarmViolations, ...checks()] : [];
			res.selfCheck = { violations: [...first, ...page.entries] };
		} else {
			res[kind] = page.entries;
		}
		if (page.next !== null) {
			if (paged.length > 1) {
				throw invalid(`the ${kind} have more than one page, so request them alone`);
			}
			res.cursor = JSON.stringify({ kind, after: page.next });
		}
	}
	return res;
}

/** The position in a cursor that `answerInspect` made for `kind`. */
function parseCursor(cursor: string, kind: InspectPagedKind): string[] {
	let parsed: unknown;
	try {
		parsed = JSON.parse(cursor);
	} catch {
		parsed = null;
	}
	const { kind: cursorKind, after } = (typeof parsed === "object" && parsed !== null ? parsed : {}) as { kind?: unknown; after?: unknown };
	if (
		cursorKind !== kind ||
		!Array.isArray(after) ||
		!CURSOR_PARTS[kind].some((parts) => after.length === parts.length && after.every((part, i) => isCursorPart(part, parts[i])))
	) {
		throw invalid(`the cursor is not a cursor of the ${kind}`);
	}
	return after as string[];
}
