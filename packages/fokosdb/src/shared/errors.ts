/**
 * The structured errors of FokosDB: the machinery, the generic categories, and the codes of each one.
 *
 * Every error the library raises extends `FokosError` and belongs to one category. A category is the
 * value of `name` and of `_tag`. The code is the fine-grained identifier of one failure. The category
 * and the code are contractual. The message is not.
 *
 * A Workers RPC hop carries the own properties of an error and drops its prototype. So the classes
 * hold data only: every field is an own data property that the constructor assigns, no class declares
 * an instance method or an accessor, and every helper is static and reads own properties. Nothing
 * classifies with `instanceof`, because it fails after a hop.
 *
 * A code is a value: a code definition carries its category, its `error_id` segment and its defaults,
 * so no central table limits which codes exist. Another module or package defines its own codes with
 * `defineCodes`, and can define its own categories as subclasses of `FokosError`. It then declares
 * the union of every error it raises and makes a guard for it with `defineErrorGuard`.
 *
 * - `FokosError.is(e)` holds for an error of any category, including one this module does not define.
 * - `FokosConflictError.is(e)` holds for one category.
 * - A guard from `defineErrorGuard` holds for the codes of one union, and a switch on `_tag` then
 *   narrows `code` to the codes of that category.
 *
 * This module imports nothing, so any package can reuse it. A category whose fields depend on the
 * types of the library, such as `FokosConditionCheckError`, lives in `errors-operations.ts`.
 */

import invariant from "./invariant.js";

export type FokosErrorOrigin = "caller" | "service" | "internal";

/**
 * The definition of one code. `defineCodes` makes it. The constructor takes it as its first argument,
 * with the defaults it carries, so a call site cannot pair a code with the wrong category.
 */
export type FokosCodeDef<T extends string = string, C extends string = string> = {
	readonly tag: T;
	readonly code: C;
	/** 6 characters from `a-hjkmnp-z2-9`, unique across every code and fixed for the life of the code. `pnpm error-segment` prints an unused one. */
	readonly segment: string;
	readonly origin: FokosErrorOrigin;
	readonly httpStatusHint: number;
};

/** The codes of one table that `defineCodes` made. */
export type FokosCodesOf<R extends Record<string, FokosCodeDef>> = R[keyof R]["code"];

/** Defines the codes of one category, with the origin and the hint they have in common. Each key is a code, and each value is its segment. */
export function defineCodes<T extends string, S extends Record<string, string>>(
	tag: T,
	origin: FokosErrorOrigin,
	httpStatusHint: number,
	segments: S,
): { readonly [C in keyof S & string]: FokosCodeDef<T, C> } {
	const defs: Record<string, FokosCodeDef> = {};
	for (const [code, segment] of Object.entries(segments)) {
		defs[code] = { tag, code, segment, origin, httpStatusHint };
	}
	return defs as { readonly [C in keyof S & string]: FokosCodeDef<T, C> };
}

/**
 * Makes the guard for one union of errors, from the code tables of that union. It holds only for a
 * code of those tables with the category of its definition, so the union type it narrows to is true
 * even when errors of other packages flow through the same code.
 */
export function defineErrorGuard<U extends FokosError>(...tables: ReadonlyArray<Record<string, FokosCodeDef>>): (e: unknown) => e is U {
	const tagOfCode = new Map<string, string>();
	for (const table of tables) {
		for (const def of Object.values(table)) {
			tagOfCode.set(def.code, def.tag);
		}
	}
	return (e: unknown): e is U => FokosError.is(e) && tagOfCode.get(e.code) === e._tag;
}

/** The options of an error. The code is the first argument of the constructor. */
export type FokosErrorOptions = {
	/** A fixed phrase. The constructor puts `fokos/<code>: ` in front of it. The dynamic detail goes in `attributes`. */
	message: string;
	attributes?: Record<string, unknown>;
	cause?: unknown;
	/** Replaces the default of the code definition. */
	origin?: FokosErrorOrigin;
	/** Replaces the default of the code definition. */
	httpStatusHint?: number;
	/** Set only when the error already has an identity, so that it keeps it. The constructor mints one otherwise. */
	error_id?: string;
};

/**
 * The plain record of an error, for storage. It survives `JSON.stringify` and any RPC hop, and
 * `FokosError.fromWire` builds the error again from it with all its fields.
 */
export type FokosErrorWire = {
	name: string;
	message: string;
	code: string;
	error_id: string;
	origin: FokosErrorOrigin;
	httpStatusHint: number;
	attributes: Record<string, unknown>;
	/** The own fields that a category adds to the base fields, for example `reason` and `meta`. */
	fields?: Record<string, unknown>;
	cause?: FokosCauseWire;
	/**
	 * Each Uint8Array of the record: its path from the root of the record, and its bytes in base64url.
	 * The value at the path is `null`. Only the root record has this field.
	 */
	bytes?: Array<[path: Array<string | number>, data: string]>;
};

/**
 * The record of a cause. Its key tells its kind, so `fromWire` gives back a value of the same kind,
 * and no value of the caller can look like a record of another kind.
 *
 * - `fokos`: a FokosError, which comes back as a FokosError.
 * - `error`: any other Error, which comes back as an Error with the same name, message and own fields.
 *   An Error stores as `{}` in JSON, so the record holds each part.
 * - `value`: any other value, as plain data.
 */
export type FokosCauseWire =
	| { fokos: FokosErrorWire }
	| { error: { name: string; message: string; fields: Record<string, unknown>; cause?: FokosCauseWire } }
	| { value: unknown };

/** `T` is the category and `C` is the union of the codes the error can carry. */
export abstract class FokosError<T extends string = string, C extends string = string> extends Error {
	readonly _tag: T;
	/** The category in snake case, for example `validation_error`. */
	readonly type: string;
	readonly code: C;
	/** `e_<segment>_<32 hex>`. The node that first detects the failure mints it, and every later hop keeps it. */
	readonly error_id: string;
	readonly origin: FokosErrorOrigin;
	readonly httpStatusHint: number;
	readonly attributes: Record<string, unknown>;

	/** `code` is a definition from a code table. It sets the code, and the defaults of the segment, the origin and the hint. */
	constructor(code: FokosCodeDef<T, C>, options: FokosErrorOptions) {
		super(`fokos/${code.code}: ${options.message}`, options.cause === undefined ? undefined : { cause: options.cause });
		this.name = code.tag;
		this._tag = code.tag;
		// "FokosConditionCheckError" becomes "condition_check_error".
		this.type = code.tag
			.replace(/^Fokos/, "")
			.replace(/\B[A-Z]/g, "_$&")
			.toLowerCase();
		this.code = code.code;
		this.error_id = options.error_id ?? `e_${code.segment}_${crypto.randomUUID().replaceAll("-", "")}`;
		this.origin = options.origin ?? code.origin;
		this.httpStatusHint = options.httpStatusHint ?? code.httpStatusHint;
		this.attributes = options.attributes ?? {};
	}

	/**
	 * True when `e` has the shape of a FokosError, after any number of hops. It reads own properties only.
	 * On `FokosError` it holds for every category, including one that another package defines. On a
	 * category class it holds for that category only, so `FokosConflictError.is(e)` narrows `e` to
	 * `FokosConflictError`. The code stays a `string`: use the guard of a union for its codes.
	 */
	static is<K extends abstract new (...args: never) => FokosError>(this: K, e: unknown): e is InstanceType<K> {
		const wanted = (this as unknown as { tag?: string }).tag;
		const fields = e as { _tag?: unknown; code?: unknown; error_id?: unknown } | null;
		return (
			typeof fields?._tag === "string" &&
			typeof fields.code === "string" &&
			typeof fields.error_id === "string" &&
			(wanted === undefined || fields._tag === wanted)
		);
	}

	/**
	 * True when `e` is a FokosError with the code `code`, after any number of hops. It narrows `code` to
	 * that literal.
	 *
	 * Pass a code definition from a code table: a misspelt code does not compile, and the check also
	 * compares the category. A plain string works for a code that arrives as data, but the compiler cannot
	 * check its spelling, and the check compares the code only. Codes are unique across every package.
	 */
	static isCode<T extends string, C extends string>(e: unknown, code: FokosCodeDef<T, C>): e is FokosError<T, C>;
	static isCode<C extends string>(e: unknown, code: C): e is FokosError<string, C>;
	static isCode(e: unknown, code: FokosCodeDef | string): boolean {
		if (!FokosError.is(e)) {
			return false;
		}
		return typeof code === "string" ? e.code === code : e._tag === code.tag && e.code === code.code;
	}

	/**
	 * Returns `e` unchanged when it is a FokosError, with or without its prototype. Wraps any other value
	 * as `foreign_error` and keeps it as `cause`.
	 *
	 * The own enumerable properties of an object `e` go into `attributes`, so the runtime markers
	 * `retryable` and `overloaded` stay reachable. `message`, `stack` and `cause` of a native error are
	 * not enumerable, so they stay out. The runtime sets `retryable: true` on a fault that clears on its
	 * own, so that fault gets the origin `service` and the hint 503: it is a service condition, not a
	 * defect.
	 */
	static wrap(e: unknown): FokosError {
		if (FokosError.is(e)) {
			return e;
		}
		const attributes: Record<string, unknown> = {};
		if (typeof e === "object" && e !== null) {
			for (const [key, value] of Object.entries(e)) {
				// Keep only values that can cross an RPC hop. One value that cannot makes the runtime drop
				// every field of the error, `code` and `error_id` included.
				// TODO: A cheaper clone, if the error rate ever makes this a bottleneck.
				try {
					attributes[key] = structuredClone(value);
				} catch {}
			}
		}
		return new FokosInternalError(CORE_INTERNAL_CODES.foreign_error, {
			message: "unexpected error occurred",
			cause: e,
			attributes,
			...(attributes.retryable === true ? { origin: "service", httpStatusHint: 503 } : {}),
		});
	}

	/**
	 * The plain record for storage. It accepts a class instance, an error that crossed a hop, or a
	 * foreign value, which it wraps first. Each own field of a category goes into `fields`, so the
	 * record needs no list of the categories. Each Uint8Array goes into `bytes`.
	 *
	 * `fromWire(toWire(e))` has every field of `e` except `stack`, and `toWire(fromWire(w))` is equal to `w`.
	 */
	static toWire(e: unknown): FokosErrorWire {
		const bytes: NonNullable<FokosErrorWire["bytes"]> = [];
		const wire = encodeBytes(plainWire(FokosError.wrap(e), 0), [], bytes) as FokosErrorWire;
		return bytes.length === 0 ? wire : { ...wire, bytes };
	}

	/**
	 * Builds the class in the calling isolate from a wire record, or from an error that crossed a hop. A
	 * category that this module does not define keeps its tag and its fields.
	 */
	static fromWire(w: FokosErrorWire | FokosError): FokosError {
		return buildError(FokosError.is(w) ? plainWire(w, 0) : decodeBytes(w));
	}
}

/** The own properties of every FokosError. Every other own enumerable property is a field of its category. */
const BASE_FIELD_NAMES = [
	"name",
	"message",
	"stack",
	"cause",
	"_tag",
	"type",
	"code",
	"error_id",
	"origin",
	"httpStatusHint",
	"attributes",
] as const satisfies ReadonlyArray<keyof FokosError>;
const BASE_FIELDS: ReadonlySet<string> = new Set(BASE_FIELD_NAMES);

// The build fails when FokosError gets a field that BASE_FIELD_NAMES does not list.
const _allBaseFields: [Exclude<keyof FokosError, (typeof BASE_FIELD_NAMES)[number]>] extends [never] ? true : never = true;

/** The deepest cause that keeps its own cause. A cause at this depth stores without its cause, so a cycle of causes ends. */
const CAUSE_DEPTH_MAX = 8;

/** The record of `err` with its bytes in place. `depth` is the number of causes above `err`. */
function plainWire(err: FokosError, depth: number): FokosErrorWire {
	const wire: FokosErrorWire = {
		name: err.name,
		message: err.message,
		code: err.code,
		error_id: err.error_id,
		origin: err.origin,
		httpStatusHint: err.httpStatusHint,
		attributes: err.attributes,
	};
	const fields = Object.entries(err).filter(([key]) => !BASE_FIELDS.has(key));
	if (fields.length > 0) {
		wire.fields = Object.fromEntries(fields);
	}
	const cause: unknown = err.cause;
	if (cause !== undefined) {
		wire.cause = causeWire(cause, depth);
	}
	return wire;
}

function causeWire(cause: unknown, depth: number): FokosCauseWire {
	const deeper = depth < CAUSE_DEPTH_MAX;
	if (FokosError.is(cause) && deeper) {
		return { fokos: plainWire(cause, depth + 1) };
	}
	if (!(cause instanceof Error)) {
		return { value: cause };
	}
	const fields = Object.fromEntries(Object.entries(cause).filter(([key]) => !ERROR_FIELDS.has(key)));
	const record: Extract<FokosCauseWire, { error: unknown }>["error"] = { name: cause.name, message: cause.message, fields };
	if (cause.cause !== undefined && deeper) {
		record.cause = causeWire(cause.cause, depth + 1);
	}
	return { error: record };
}

/** The own properties of every Error. Every other own enumerable property is a field of the Error. */
const ERROR_FIELDS: ReadonlySet<string> = new Set(["name", "message", "stack", "cause"]);

function buildCause(wire: FokosCauseWire): unknown {
	if ("fokos" in wire) {
		return buildError(wire.fokos);
	}
	if (!("error" in wire)) {
		return wire.value;
	}
	const { name, message, fields, cause } = wire.error;
	const err = new Error(message, cause === undefined ? undefined : { cause: buildCause(cause) });
	// `Error.prototype` holds `name`, so the own property stays out of the fields, as it does on an Error.
	Object.defineProperty(err, "name", { value: name, writable: true, enumerable: false, configurable: true });
	for (const [key, value] of Object.entries(fields)) {
		if (!ERROR_FIELDS.has(key)) {
			setOwn(err, key, value);
		}
	}
	return err;
}

/** The error of a record whose bytes are in place. */
function buildError(wire: FokosErrorWire): FokosError {
	const Category = FOKOS_ERROR_CATEGORIES.get(wire.name) ?? OtherCategoryError;
	const cause = wire.cause === undefined ? undefined : buildCause(wire.cause);
	// The error already has its identity, so the segment is never read.
	const err = new Category(
		{ tag: wire.name, code: wire.code, segment: "", origin: wire.origin, httpStatusHint: wire.httpStatusHint },
		{ message: "", attributes: wire.attributes, cause, error_id: wire.error_id },
	);
	// The message already carries its `fokos/<code>: ` prefix.
	err.message = wire.message;
	for (const [key, value] of Object.entries(wire.fields ?? {})) {
		if (!BASE_FIELDS.has(key)) {
			setOwn(err, key, value);
		}
	}
	return err;
}

/**
 * A copy of `value` with `null` in place of each Uint8Array, and the path and the bytes of each one
 * in `out`. `Object.fromEntries` makes own properties, so a key `__proto__` stays data.
 */
function encodeBytes(value: unknown, path: Array<string | number>, out: NonNullable<FokosErrorWire["bytes"]>): unknown {
	if (value instanceof Uint8Array) {
		out.push([path, value.toBase64({ alphabet: "base64url", omitPadding: true })]);
		return null;
	}
	if (Array.isArray(value)) {
		return value.map((item, i) => encodeBytes(item, [...path, i], out));
	}
	if (typeof value === "object" && value !== null) {
		return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, encodeBytes(item, [...path, key], out)]));
	}
	return value;
}

/** A copy of `wire` with each Uint8Array of `bytes` back at its path, and no `bytes`. */
function decodeBytes(wire: FokosErrorWire): FokosErrorWire {
	if (wire.bytes === undefined) {
		return wire;
	}
	const { bytes, ...rest } = wire;
	const copy = structuredClone(rest);
	for (const [path, data] of bytes) {
		invariant(path.length > 0, "bytes path must not be empty");
		let parent: unknown;
		let node: unknown = copy;
		for (const key of path) {
			parent = node;
			node = ownValue(node, key, path);
		}
		setOwn(parent as object, path.at(-1)!, Uint8Array.fromBase64(data, { alphabet: "base64url" }));
	}
	return copy;
}

/**
 * The own property `key` of `node`. A path that does not end at an own property is a record that
 * `toWire` did not make, and a path through an inherited property would write outside the record.
 */
function ownValue(node: unknown, key: string | number, path: Array<string | number>): unknown {
	if (typeof node !== "object" || node === null || !Object.hasOwn(node, key)) {
		throw new FokosInternalError(CORE_INTERNAL_CODES.invariant_failed, {
			message: "an error record has a bytes path with no value",
			attributes: { path },
		});
	}
	return (node as Record<string | number, unknown>)[key];
}

/** Writes an own data property, as an assignment does. An assignment to `__proto__` would change the prototype instead. */
function setOwn(target: object, key: string | number, value: unknown): void {
	Object.defineProperty(target, key, { value, writable: true, enumerable: true, configurable: true });
}

/**
 * True when the runtime marks `e` as a transient fault that a retry can clear: `retryable` and not
 * `overloaded`. It reads the markers on a raw runtime error, and in `attributes` after `wrap` moved them.
 */
export function isRuntimeRetryableError(e: unknown): boolean {
	const markers = (FokosError.is(e) ? e.attributes : e) as { retryable?: unknown; overloaded?: unknown } | null | undefined;
	return markers?.retryable === true && markers.overloaded !== true;
}

/** An error of a category that another package defines, as `fromWire` builds it. */
class OtherCategoryError extends FokosError {}

// ─── The categories ───────────────────────────────────────────────────────────
//
// `C` is open, so another package can raise its own codes in a category of this module.

export class FokosValidationError<C extends string = string> extends FokosError<"FokosValidationError", C> {
	static readonly tag = "FokosValidationError";
}

export class FokosExpressionError<C extends string = string> extends FokosError<"FokosExpressionError", C> {
	static readonly tag = "FokosExpressionError";
}

export class FokosConflictError<C extends string = string> extends FokosError<"FokosConflictError", C> {
	static readonly tag = "FokosConflictError";
}

export class FokosUnavailableError<C extends string = string> extends FokosError<"FokosUnavailableError", C> {
	static readonly tag = "FokosUnavailableError";
}

export class FokosTransactionPendingError<C extends string = string> extends FokosError<"FokosTransactionPendingError", C> {
	static readonly tag = "FokosTransactionPendingError";
}

export class FokosRoutingError<C extends string = string> extends FokosError<"FokosRoutingError", C> {
	static readonly tag = "FokosRoutingError";
}

export class FokosInternalError<C extends string = string> extends FokosError<"FokosInternalError", C> {
	static readonly tag = "FokosInternalError";
}

/** Each category class of this module by its tag. */
export const FOKOS_ERROR_CATEGORIES: ReadonlyMap<string, new (code: FokosCodeDef, options: FokosErrorOptions) => FokosError> = new Map(
	[
		FokosValidationError,
		FokosExpressionError,
		FokosConflictError,
		FokosUnavailableError,
		FokosTransactionPendingError,
		FokosRoutingError,
		FokosInternalError,
	].map((category) => [category.tag, category as unknown as new (code: FokosCodeDef, options: FokosErrorOptions) => FokosError]),
);

// ─── The codes ────────────────────────────────────────────────────────────────
//
// The origin and the hint are defaults. A constructor takes them unless the call site passes others,
// so a consumer must read the fields on the error and not these tables.

export const VALIDATION_CODES = defineCodes("FokosValidationError", "caller", 400, {
	hash_key_empty: "2fzzq9",
	sort_key_empty: "2gjvju",
	key_contains_nul: "42r8z7",
	hash_key_too_large: "4v2p4p",
	sort_key_too_large: "58daxm",
	item_data_too_large: "6z7eb3",
	item_data_wrong_type: "7vxpb8",
	item_data_not_json_serializable: "bfvvtt",
	ttl_at_invalid: "brcy77",
	return_values_option_invalid: "ed9wyr",
	client_request_token_invalid: "f9azze",
	idempotent_parameter_mismatch: "fn733z",
	transact_items_empty: "gmjfgw",
	transact_items_too_many: "h58dgv",
	transact_duplicate_key: "hgxg2r",
	transact_payload_too_large: "hsvepa",
	transact_operation_fields_invalid: "jr49a5",
	transact_version_after_write: "ja8vq2",
	compiled_plan_refused: "shbucy",
	transact_execution_mode_invalid: "k7nq3d",
	query_queries_empty: "k44ag9",
	query_limit_invalid: "k4g8z5",
	query_max_response_bytes_invalid: "xytewt",
	query_select_invalid: "k6wmvn",
	query_projection_with_count: "xyz9jz",
	cursor_malformed: "pndxkq",
	cursor_version_unknown: "s62ybe",
	cursor_query_index_out_of_range: "sevnxx",
	cursor_direction_mismatch: "sfcvks",
	cursor_fingerprint_mismatch: "t3kbec",
	num_tx_coordinators_invalid: "uc9fkn",
	fokosdb_options_invalid: "j5dk3c",
	item_too_large: "ynzx4p",
	update_not_applicable: "yysds3",
	update_value_is_bytes: "z9ar7e",
	batch_items_too_many: "wy5f9g",
	batch_duplicate_key: "eqs978",
	batch_operation_fields_invalid: "bfvz4s",
	batch_request_too_large: "nhncs4",
	batch_max_response_bytes_invalid: "7zkqbc",
});

export const EXPRESSION_CODES = defineCodes("FokosExpressionError", "caller", 400, {
	expression_invalid: "ucjjtz",
});

export const CONFLICT_CODES = {
	...defineCodes("FokosConflictError", "caller", 409, {
		item_locked_by_transaction: "vnfeg6",
		timestamp_conflict: "vw99ky",
		pending_conflict: "w65ens",
		read_conflict: "wx4mnz",
		pending_write: "xam35s",
	}),
	// The partition clock and the transaction clock disagree. A later attempt clears it, so it is a service condition.
	...defineCodes("FokosConflictError", "service", 503, {
		clock_skew: "xy3rrw",
	}),
};

export const TRANSACTION_PENDING_CODES = defineCodes("FokosTransactionPendingError", "service", 503, {
	transaction_undecided: "28ahbe",
	transaction_commit_pending: "3wbgez",
});

export const UNAVAILABLE_CODES = defineCodes("FokosUnavailableError", "service", 503, {
	partition_over_size: "49j6ez",
	coordinator_over_size: "tg8r62",
	prepare_unanswered: "mpncbz",
});

/**
 * The codes of the error system itself. `FokosError.wrap` raises `foreign_error`, and `invariant` raises
 * `invariant_failed`. They are here and not in `sharding/errors.ts`, because this module imports nothing.
 */
export const CORE_INTERNAL_CODES = defineCodes("FokosInternalError", "internal", 500, {
	invariant_failed: "85quf8",
	foreign_error: "jvufz5",
});

export const INTERNAL_CODES = defineCodes("FokosInternalError", "internal", 500, {
	item_data_parse_failed: "dx9mht",
	commit_keyset_mismatch: "e3kh5s",
	unexpected_transaction_state: "j6uhd6",
});

/**
 * Every code table of this module. `FokosRoutingError` has no table here: the codes that only
 * partitions exchange, its own among them, are in `sharding/errors.ts`.
 */
export const FOKOS_CODE_TABLES = [
	VALIDATION_CODES,
	EXPRESSION_CODES,
	CONFLICT_CODES,
	TRANSACTION_PENDING_CODES,
	UNAVAILABLE_CODES,
	CORE_INTERNAL_CODES,
	INTERNAL_CODES,
] as const;
