/**
 * A local fake of the Durable Object snapshot API: `ctx.storage.snapshot()` and
 * `ctx.storage.onNextSessionRestore()`.
 *
 * workerd has these methods behind the `durable_object_snapshots` flag, but its local storage
 * back-end throws for them. `installFakeSnapshots(ctx, env)` replaces the two methods on the real
 * `ctx.storage`, so the production code calls the same API locally and on Cloudflare.
 *
 * - `snapshot()` copies the SQL tables and the KV keys of the DO into a backend. A handle names the
 *   copy. The handle is a plain object, so it travels over RPC as the real handle does.
 * - `onNextSessionRestore()` reads the copy from the backend and keeps it for the next start of the
 *   target DO. The caller must restart the DO, for example with `ctx.abort()`, as on Cloudflare.
 * - At the next start, `installFakeSnapshots` replaces all the storage with the copy, writes
 *   `kvData`, and deletes the alarm. A subclass calls it before `super()`, thus the constructor of the
 *   production class sees only the restored storage. This step is synchronous, so the copy must be
 *   in memory before the restart.
 *
 * The backends:
 *
 * - `memory`: module state of the test isolate.
 * - `do`: the SQLite storage of `SnapshotStoreDO`.
 * - `host`: a directory of JSON pages for each copy on the disk of the host, through the Node service
 *   binding `FOKOS_SNAPSHOT_HOST` of `vitest.config.ts`.
 *
 * `FOKOS_SNAPSHOT_BACKEND` sets the default backend, and `setFakeSnapshotBackend` changes it. The
 * handle records its backend, thus a restore reads the copy from the backend that has it.
 *
 * `clearFakeSnapshots()` deletes every copy that this module made. `cleanup-setup.ts` calls it after
 * each test file. A copy stays until then, because the real handle stays valid while a request
 * holds it, and the fake cannot see when the last holder ends.
 */
import { env as testEnv } from "cloudflare:workers";

/** The limits that workerd applies to `kvData`. */
const MAX_RESTORE_KV_ENTRIES = 20;
const MAX_RESTORE_KV_BYTES = 3 * 1024;

/** The largest page of the text form of a copy, in UTF-16 units, unless one row is larger. */
const PAGE_CHARS = 1024 * 1024;

export type FakeSnapshotBackend = "memory" | "do" | "host";

type SchemaObject = { type: string; name: string; sql: string };
type TableRows = { name: string; columns: string[]; rows: SqlStorageValue[][] };
export type StorageCopy = { schema: SchemaObject[]; tables: TableRows[]; kv: [string, unknown][] };

/** The handle that the fake `snapshot()` returns. */
export type FakeSnapshotHandle = { readonly fakeSnapshotId: string; readonly backend: FakeSnapshotBackend };

type SnapshotEnv = {
	SNAPSHOT_STORE_DO: DurableObjectNamespace<import("./snapshot-store-do.js").SnapshotStoreDO>;
	FOKOS_SNAPSHOT_HOST: Fetcher;
};

// `vitest.config.ts` substitutes this constant into this module when it builds it.
declare const __FOKOS_SNAPSHOT_BACKEND__: string;

let currentBackend = parseBackend(__FOKOS_SNAPSHOT_BACKEND__ === "" ? "memory" : __FOKOS_SNAPSHOT_BACKEND__);
const memoryCopies = new Map<string, StorageCopy>();
/** Every copy that this module made, in any backend. `clearFakeSnapshots` deletes them. */
const madeCopies = new Map<string, FakeSnapshotBackend>();
const scheduledRestores = new Map<string, { copy: StorageCopy; kvData: [string, unknown][] }>();

function parseBackend(value: string): FakeSnapshotBackend {
	if (value !== "memory" && value !== "do" && value !== "host") {
		throw new Error(`FOKOS_SNAPSHOT_BACKEND must be memory, do or host, got ${JSON.stringify(value)}`);
	}
	return value;
}

export function setFakeSnapshotBackend(backend: FakeSnapshotBackend): void {
	currentBackend = backend;
}

// ─── the copy ─────────────────────────────────────────────────────────────────

/** The user schema objects. workerd owns the `_cf_` tables, and SQLite owns the `sqlite_` objects. */
function schemaObjects(sql: SqlStorage): SchemaObject[] {
	return sql
		.exec<SchemaObject>(
			`SELECT type, name, sql FROM sqlite_master
			 WHERE sql IS NOT NULL
			   AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND tbl_name NOT LIKE '\\_cf\\_%' ESCAPE '\\'
			   AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
			 ORDER BY rowid`,
		)
		.toArray();
}

function quote(name: string): string {
	return `"${name.replaceAll('"', '""')}"`;
}

function copyStorage(storage: DurableObjectStorage): StorageCopy {
	const schema = schemaObjects(storage.sql);
	const tables = schema
		.filter((o) => o.type === "table")
		.map((o) => {
			const cursor = storage.sql.exec(`SELECT * FROM ${quote(o.name)}`);
			const rows = [...cursor.raw()];
			return { name: o.name, columns: cursor.columnNames, rows };
		});
	return { schema, tables, kv: [...storage.kv.list()] };
}

/** Replaces all the storage with `copy`, in one transaction. */
function applyRestore(storage: DurableObjectStorage, copy: StorageCopy, kvData: [string, unknown][]): void {
	storage.transactionSync(() => {
		for (const o of schemaObjects(storage.sql)) {
			// A DROP TABLE also drops the indexes and the triggers of the table.
			if (o.type === "table" || o.type === "view") {
				storage.sql.exec(`DROP ${o.type.toUpperCase()} IF EXISTS ${quote(o.name)}`);
			}
		}
		// Collect the keys first. A delete must not change the list while it is read.
		const keys = Array.from(storage.kv.list(), ([key]) => key);
		for (const key of keys) {
			storage.kv.delete(key);
		}
		// Tables first, then their rows, then the indexes, triggers and views. A trigger must not
		// operate on the copied rows.
		for (const o of copy.schema.filter((o) => o.type === "table")) {
			storage.sql.exec(o.sql);
		}
		for (const t of copy.tables) {
			const insert = `INSERT INTO ${quote(t.name)} (${t.columns.map(quote).join(", ")}) VALUES (${t.columns.map(() => "?").join(", ")})`;
			for (const row of t.rows) {
				storage.sql.exec(insert, ...row);
			}
		}
		for (const o of copy.schema.filter((o) => o.type !== "table")) {
			storage.sql.exec(o.sql);
		}
		for (const [key, value] of [...copy.kv, ...kvData]) {
			storage.kv.put(key, value);
		}
	});
	// A restore does not restore alarms, and it clears the alarm of the target DO. The storage
	// orders this delete before every later write of the constructor.
	void storage.deleteAlarm();
}

// ─── the text form of a copy ──────────────────────────────────────────────────

/**
 * JSON with tags for the values that JSON cannot hold: SQL blobs, the typed arrays, Map and Set of
 * the KV values, and the numbers that JSON changes. A plain object with a key that starts with `$`
 * is wrapped in `$o`, thus no plain object reads as a tag. An unknown class throws, so that no value
 * is changed without an error.
 */
function encodeValue(v: unknown): unknown {
	if (v === null || typeof v === "string" || typeof v === "boolean") {
		return v;
	}
	if (typeof v === "number") {
		return Number.isFinite(v) && !Object.is(v, -0) ? v : { $n: Object.is(v, -0) ? "-0" : String(v) };
	}
	if (v === undefined) {
		return { $u: 1 };
	}
	if (typeof v === "bigint") {
		return { $big: v.toString() };
	}
	if (v instanceof ArrayBuffer) {
		return { $ab: new Uint8Array(v).toBase64() };
	}
	if (ArrayBuffer.isView(v)) {
		// Only the bytes of the view. The rest of its buffer is not part of the value.
		const bytes = new Uint8Array(v.buffer as ArrayBuffer, v.byteOffset, v.byteLength).toBase64();
		return { $view: v.constructor.name, b: bytes };
	}
	if (Array.isArray(v)) {
		return v.map(encodeValue);
	}
	if (v instanceof Map) {
		return { $map: [...v].map(([k, x]) => [encodeValue(k), encodeValue(x)]) };
	}
	if (v instanceof Set) {
		return { $set: [...v].map(encodeValue) };
	}
	if (v instanceof Date) {
		return { $date: v.getTime() };
	}
	const proto = Object.getPrototypeOf(v);
	if (typeof v === "object" && (proto === Object.prototype || proto === null)) {
		const entries = Object.entries(v).map(([k, x]) => [k, encodeValue(x)] as const);
		const obj = Object.fromEntries(entries);
		return entries.some(([k]) => k.startsWith("$")) ? { $o: obj } : obj;
	}
	throw new TypeError(`fake snapshots: cannot encode a value of type ${proto?.constructor?.name ?? typeof v}`);
}

const VIEWS: Record<string, new (buffer: ArrayBuffer) => ArrayBufferView> = {
	Int8Array,
	Uint8Array,
	Uint8ClampedArray,
	Int16Array,
	Uint16Array,
	Int32Array,
	Uint32Array,
	Float32Array,
	Float64Array,
	BigInt64Array,
	BigUint64Array,
	DataView,
};

function decodeValue(v: unknown): unknown {
	if (Array.isArray(v)) {
		return v.map(decodeValue);
	}
	if (v === null || typeof v !== "object") {
		return v;
	}
	const o = v as Record<string, unknown>;
	if ("$n" in o) {
		return Number(o.$n);
	}
	if ("$u" in o) {
		return undefined;
	}
	if ("$big" in o) {
		return BigInt(o.$big as string);
	}
	if ("$ab" in o) {
		return Uint8Array.fromBase64(o.$ab as string).buffer;
	}
	if ("$view" in o) {
		return new VIEWS[o.$view as string](Uint8Array.fromBase64(o.b as string).buffer as ArrayBuffer);
	}
	if ("$map" in o) {
		return new Map((o.$map as [unknown, unknown][]).map(([k, x]) => [decodeValue(k), decodeValue(x)]));
	}
	if ("$set" in o) {
		return new Set((o.$set as unknown[]).map(decodeValue));
	}
	if ("$date" in o) {
		return new Date(o.$date as number);
	}
	const plain = "$o" in o ? (o.$o as Record<string, unknown>) : o;
	return Object.fromEntries(Object.entries(plain).map(([k, x]) => [k, decodeValue(x)]));
}

/**
 * The text form of a copy is a sequence of pages, so that no string is too long for V8 and no
 * message is too large for one RPC. Page 0 holds the schema, the KV keys and the columns of each
 * table. Each later page holds rows of one table: `{ t, rows }`, with `t` the index of the table. A
 * page has at most `pageChars` characters, or one row when the row alone is larger.
 */
export function* encodeCopy(copy: StorageCopy, pageChars = PAGE_CHARS): Generator<string> {
	yield JSON.stringify(
		encodeValue({ schema: copy.schema, kv: copy.kv, tables: copy.tables.map((t) => ({ name: t.name, columns: t.columns, rows: [] })) }),
	);
	for (const [t, table] of copy.tables.entries()) {
		let rows: string[] = [];
		let chars = 0;
		for (const row of table.rows) {
			const text = JSON.stringify(encodeValue(row));
			if (rows.length > 0 && chars + text.length > pageChars) {
				yield `{"t":${t},"rows":[${rows.join(",")}]}`;
				rows = [];
				chars = 0;
			}
			rows.push(text);
			chars += text.length + 1;
		}
		if (rows.length > 0) {
			yield `{"t":${t},"rows":[${rows.join(",")}]}`;
		}
	}
}

/** Adds one page to a copy that is being decoded. The first page starts the copy. */
export function decodePage(copy: StorageCopy | null, text: string): StorageCopy {
	const page = decodeValue(JSON.parse(text));
	if (copy === null) {
		return page as StorageCopy;
	}
	const { t, rows } = page as { t: number; rows: SqlStorageValue[][] };
	for (const row of rows) {
		copy.tables[t].rows.push(row);
	}
	return copy;
}

export function decodeCopy(pages: Iterable<string>): StorageCopy {
	let copy: StorageCopy | null = null;
	for (const page of pages) {
		copy = decodePage(copy, page);
	}
	if (copy === null) {
		throw new Error("fake snapshots: a copy has no pages");
	}
	return copy;
}

// ─── the backends ─────────────────────────────────────────────────────────────

function store(env: SnapshotEnv) {
	return env.SNAPSHOT_STORE_DO.getByName("fake-snapshots");
}

async function hostCall(env: SnapshotEnv, method: string, path: string, body?: string): Promise<Response> {
	const res = await env.FOKOS_SNAPSHOT_HOST.fetch(`http://snapshot-host/${path}`, { method, body });
	if (!res.ok && res.status !== 404) {
		throw new Error(`fake snapshots: the host answered ${res.status} to ${method} ${path}: ${await res.text()}`);
	}
	return res;
}

/**
 * Writes the pages of a copy, and then the number of pages. A reader finds a copy only after the
 * number is written, thus it never reads a part of a copy.
 */
async function putCopy(env: SnapshotEnv, handle: FakeSnapshotHandle, copy: StorageCopy): Promise<void> {
	const id = handle.fakeSnapshotId;
	madeCopies.set(id, handle.backend);
	if (handle.backend === "memory") {
		memoryCopies.set(id, copy);
		return;
	}
	let pages = 0;
	for (const page of encodeCopy(copy)) {
		if (handle.backend === "do") {
			await store(env).putPage(id, pages, page);
		} else {
			await hostCall(env, "PUT", `${id}/${pages}`, page);
		}
		pages++;
	}
	if (handle.backend === "do") {
		await store(env).finish(id, pages);
	} else {
		await hostCall(env, "POST", id, String(pages));
	}
}

async function getCopy(env: SnapshotEnv, handle: FakeSnapshotHandle): Promise<StorageCopy | undefined> {
	const id = handle.fakeSnapshotId;
	if (handle.backend === "memory") {
		return memoryCopies.get(id);
	}
	let pages: number | null;
	if (handle.backend === "do") {
		pages = await store(env).pageCount(id);
	} else {
		const res = await hostCall(env, "GET", id);
		pages = res.status === 404 ? null : Number(await res.text());
	}
	if (pages === null) {
		return undefined;
	}
	let copy: StorageCopy | null = null;
	for (let seq = 0; seq < pages; seq++) {
		const page = handle.backend === "do" ? await store(env).getPage(id, seq) : await (await hostCall(env, "GET", `${id}/${seq}`)).text();
		copy = decodePage(copy, page);
	}
	return copy ?? undefined;
}

/**
 * Deletes every copy that this module made, in every backend. A handle of a deleted copy then fails
 * in `onNextSessionRestore`. A restore that is already scheduled keeps its copy.
 */
export async function clearFakeSnapshots(env: SnapshotEnv = testEnv as unknown as SnapshotEnv): Promise<void> {
	const ids = [...madeCopies];
	madeCopies.clear();
	for (const [id, backend] of ids) {
		if (backend === "memory") {
			memoryCopies.delete(id);
		} else if (backend === "host") {
			await hostCall(env, "DELETE", id);
		}
	}
	const inStore = ids.filter(([, b]) => b === "do").map(([id]) => id);
	if (inStore.length > 0) {
		await store(env).deleteCopies(inStore);
	}
}

// ─── the fake methods ─────────────────────────────────────────────────────────

/**
 * Applies the restore that this DO scheduled, if any, and installs the fake methods. Call it before
 * `super(ctx, env)` of a subclass of the production class.
 */
export function installFakeSnapshots(ctx: DurableObjectState, workerEnv: unknown): void {
	const env = workerEnv as SnapshotEnv;
	const self = ctx.id.toString();
	const scheduled = scheduledRestores.get(self);
	if (scheduled !== undefined) {
		applyRestore(ctx.storage, scheduled.copy, scheduled.kvData);
		scheduledRestores.delete(self);
	}

	const snapshot = async (options?: { bookmark?: string }): Promise<FakeSnapshotHandle> => {
		if (options?.bookmark !== undefined) {
			throw new Error("fake snapshots: a snapshot at a bookmark is not supported");
		}
		// The real snapshot includes the writes of the current atomic batch, also the writes after the
		// call starts. Thus the copy waits until the synchronous block of the caller ends.
		await Promise.resolve();
		const copy = copyStorage(ctx.storage);
		const handle: FakeSnapshotHandle = { fakeSnapshotId: crypto.randomUUID(), backend: currentBackend };
		await putCopy(env, handle, copy);
		return handle;
	};

	const onNextSessionRestore = async (
		target: FakeSnapshotHandle | string,
		options?: { kvData?: Record<string, unknown> },
	): Promise<string> => {
		if (typeof target === "string") {
			throw new Error("fake snapshots: a restore from a bookmark is not supported");
		}
		const kvData = Object.entries(options?.kvData ?? {});
		if (kvData.length > MAX_RESTORE_KV_ENTRIES) {
			throw new RangeError(`kvData can contain at most ${MAX_RESTORE_KV_ENTRIES} keys.`);
		}
		// workerd counts the V8 serialization of each value. JSON is an approximation.
		const bytes = kvData.reduce((n, [k, v]) => n + k.length + JSON.stringify(v ?? null).length, 0);
		if (bytes > MAX_RESTORE_KV_BYTES) {
			throw new RangeError(`kvData can contain at most ${MAX_RESTORE_KV_BYTES} bytes of keys and serialized values.`);
		}
		const copy = target?.fakeSnapshotId === undefined ? undefined : await getCopy(env, target);
		if (copy === undefined) {
			throw new TypeError("fake snapshots: unknown snapshot handle");
		}
		const undo = await ctx.storage.getCurrentBookmark();
		scheduledRestores.set(self, { copy, kvData });
		return undo;
	};

	Object.defineProperty(ctx.storage, "snapshot", { configurable: true, value: snapshot });
	Object.defineProperty(ctx.storage, "onNextSessionRestore", { configurable: true, value: onNextSessionRestore });
}
