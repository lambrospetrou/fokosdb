/**
 * The structured errors of FokosDB.
 *
 * Every error the library raises extends `FokosError` and belongs to one of the nine categories below.
 * A category is the value of `name` and of `_tag`. The code is the fine-grained identifier of one
 * failure. The category and the code are contractual. The message is not.
 *
 * A Workers RPC hop carries the own properties of an error and drops its prototype. So the classes
 * hold data only: every field is an own data property that the constructor assigns, no class declares
 * an instance method or an accessor, and every helper is static and reads own properties. Nothing
 * classifies with `instanceof`, because it fails after a hop. Use `FokosError.is(e)` for any error of
 * this library, `FokosConflictError.is(e)` for one category, or compare `e.code`.
 *
 * The classes live under `shared/` because the client raises and matches on them too.
 */

export type FokosErrorOrigin = "caller" | "service" | "internal";

export type FokosErrorInit<C extends string = FokosErrorCode> = {
	code: C;
	/** A fixed phrase. The constructor puts `fokos/<code>: ` in front of it. The dynamic detail goes in `attributes`. */
	message: string;
	attributes?: Record<string, unknown>;
	cause?: unknown;
	/** Replaces the registry default of the code. */
	origin?: FokosErrorOrigin;
	/** Replaces the registry default of the code. */
	httpStatusHint?: number;
	/** Set only when the error already has an identity, so that it keeps it. The constructor mints one otherwise. */
	error_id?: string;
};

/** The plain record of an error, for storage. Every field is plain data, so it survives JSON and any RPC hop. */
export type FokosErrorWire = {
	name: string;
	message: string;
	code: string;
	error_id: string;
	origin: FokosErrorOrigin;
	httpStatusHint: number;
	attributes: Record<string, unknown>;
	cause?: { error: string; errorProps: Record<string, unknown> };
};

/**
 * `T` is the category and `C` is the union of its codes. A category passes both, so the base holds no
 * conditional type and every category is assignable to `FokosError`.
 */
export abstract class FokosError<T extends string = string, C extends string = FokosErrorCode> extends Error {
	readonly _tag: T;
	/** The category in snake case, for example `validation_error`. */
	readonly type: string;
	readonly code: C;
	/** `e_<segment>_<32 hex>`. The node that first detects the failure mints it, and every later hop keeps it. */
	readonly error_id: string;
	readonly origin: FokosErrorOrigin;
	readonly httpStatusHint: number;
	readonly attributes: Record<string, unknown>;

	constructor(init: FokosErrorInit<C>) {
		super(`fokos/${init.code}: ${init.message}`, init.cause === undefined ? undefined : { cause: init.cause });
		const tag = (new.target as unknown as { tag: T }).tag;
		// `fromWire` can pass a code that this version does not know, but it passes every field that the
		// entry supplies, so the `??` below never reads the missing entry.
		const entry: FokosErrorRegistryEntry = FOKOS_ERROR_REGISTRY[init.code as FokosErrorCode];
		this.name = tag;
		this._tag = tag;
		// "FokosConditionCheckError" becomes "condition_check_error".
		this.type = tag
			.slice("Fokos".length)
			.replace(/\B[A-Z]/g, "_$&")
			.toLowerCase();
		this.code = init.code;
		this.error_id = init.error_id ?? `e_${entry.segment}_${crypto.randomUUID().replaceAll("-", "")}`;
		this.origin = init.origin ?? entry.origin;
		this.httpStatusHint = init.httpStatusHint ?? entry.httpStatusHint;
		this.attributes = init.attributes ?? {};
	}

	/**
	 * True when `e` is an error of this library, after any number of hops. It reads own properties only.
	 * On `FokosError` it holds for every category. On a category class it holds for that category only,
	 * so `FokosConflictError.is(e)` narrows `e` to `FokosConflictError`.
	 */
	static is<K extends abstract new (...args: never) => FokosError>(this: K, e: unknown): e is Extract<FokosAnyError, InstanceType<K>> {
		const wanted = (this as unknown as { tag?: string }).tag;
		const tag = (e as { _tag?: unknown } | null)?._tag;
		return (
			typeof tag === "string" &&
			FOKOS_ERROR_CATEGORIES.has(tag) &&
			typeof (e as { code?: unknown }).code === "string" &&
			(wanted === undefined || tag === wanted)
		);
	}

	/**
	 * Returns `e` unchanged when it is a FokosError, with or without its prototype. Wraps any other value
	 * as `foreign_error` and keeps it as `cause`.
	 *
	 * The own enumerable properties of an object `e` go into `attributes`, so the runtime markers
	 * `retryable` and `overloaded` stay reachable. `message`, `stack` and `cause` of a native error are
	 * not enumerable, so they stay out. The runtime sets
	 * `retryable: true` on a fault that clears on its own, so that fault gets the origin `service` and
	 * the hint 503: it is a service condition, not a defect.
	 */
	static wrap(e: unknown): FokosAnyError {
		if (FokosError.is(e)) return e;
		const attributes: Record<string, unknown> = {};
		if (typeof e === "object" && e !== null) {
			for (const [key, value] of Object.entries(e)) {
				// Keep only values that can cross an RPC hop. One value that cannot makes the runtime drop
				// every field of the error, `code` and `error_id` included.
				// If we are creating so many errors and this becomes a performance bottleneck, we might need a more efficient cloning strategy.
				try {
					attributes[key] = structuredClone(value);
				} catch {}
			}
		}
		return new FokosInternalError({
			code: "foreign_error",
			message: "unexpected error occurred",
			cause: e,
			attributes,
			...(attributes.retryable === true ? { origin: "service", httpStatusHint: 503 } : {}),
		});
	}

	/** The plain record for storage. It accepts a class instance, an error that crossed a hop, or a foreign value, which it wraps first. */
	static toWire(e: unknown): FokosErrorWire {
		const err = FokosError.wrap(e);
		const wire: FokosErrorWire = {
			name: err.name,
			message: err.message,
			code: err.code,
			error_id: err.error_id,
			origin: err.origin,
			httpStatusHint: err.httpStatusHint,
			attributes: err.attributes,
		};
		const cause: unknown = err.cause;
		if (cause !== undefined) {
			// An Error object stores as `{}` in JSON, so the cause keeps its text and its own enumerable fields.
			wire.cause = { error: String(cause), errorProps: typeof cause === "object" && cause !== null ? { ...cause } : {} };
		}
		return wire;
	}

	/** Builds the class in the calling isolate from a wire record, or from an error that crossed a hop. */
	static fromWire(w: FokosErrorWire | FokosAnyError): FokosAnyError {
		const Category = FOKOS_ERROR_CATEGORIES.get(w.name);
		if (Category === undefined) return FokosError.wrap(w);
		const err = new Category({
			code: w.code,
			message: "",
			attributes: w.attributes,
			cause: w.cause,
			origin: w.origin,
			httpStatusHint: w.httpStatusHint,
			error_id: w.error_id,
		});
		// The message already carries its `fokos/<code>: ` prefix.
		err.message = w.message;
		return err;
	}
}

// ─── The categories ───────────────────────────────────────────────────────────

export class FokosValidationError extends FokosError<"FokosValidationError", FokosErrorCodeOf<"FokosValidationError">> {
	static readonly tag: FokosValidationError["_tag"] = "FokosValidationError";
}

export class FokosExpressionError extends FokosError<"FokosExpressionError", FokosErrorCodeOf<"FokosExpressionError">> {
	static readonly tag: FokosExpressionError["_tag"] = "FokosExpressionError";
}

export class FokosConditionCheckError extends FokosError<"FokosConditionCheckError", FokosErrorCodeOf<"FokosConditionCheckError">> {
	static readonly tag: FokosConditionCheckError["_tag"] = "FokosConditionCheckError";
}

export class FokosConflictError extends FokosError<"FokosConflictError", FokosErrorCodeOf<"FokosConflictError">> {
	static readonly tag: FokosConflictError["_tag"] = "FokosConflictError";
}

export class FokosTransactionCancelledError extends FokosError<
	"FokosTransactionCancelledError",
	FokosErrorCodeOf<"FokosTransactionCancelledError">
> {
	static readonly tag: FokosTransactionCancelledError["_tag"] = "FokosTransactionCancelledError";
}

export class FokosUnavailableError extends FokosError<"FokosUnavailableError", FokosErrorCodeOf<"FokosUnavailableError">> {
	static readonly tag: FokosUnavailableError["_tag"] = "FokosUnavailableError";
}

export class FokosTransactionPendingError extends FokosError<
	"FokosTransactionPendingError",
	FokosErrorCodeOf<"FokosTransactionPendingError">
> {
	static readonly tag: FokosTransactionPendingError["_tag"] = "FokosTransactionPendingError";
}

export class FokosRoutingError extends FokosError<"FokosRoutingError", FokosErrorCodeOf<"FokosRoutingError">> {
	static readonly tag: FokosRoutingError["_tag"] = "FokosRoutingError";
}

export class FokosInternalError extends FokosError<"FokosInternalError", FokosErrorCodeOf<"FokosInternalError">> {
	static readonly tag: FokosInternalError["_tag"] = "FokosInternalError";
}

/** The union of the nine categories. `_tag` is a literal here, so a switch on it narrows and stays exhaustive. */
export type FokosAnyError =
	| FokosValidationError
	| FokosExpressionError
	| FokosConditionCheckError
	| FokosConflictError
	| FokosTransactionCancelledError
	| FokosUnavailableError
	| FokosTransactionPendingError
	| FokosRoutingError
	| FokosInternalError;

export type FokosErrorTag = FokosAnyError["_tag"];

/** Each category class by its tag. */
export const FOKOS_ERROR_CATEGORIES: ReadonlyMap<string, new (init: FokosErrorInit<string>) => FokosAnyError> = new Map(
	[
		FokosValidationError,
		FokosExpressionError,
		FokosConditionCheckError,
		FokosConflictError,
		FokosTransactionCancelledError,
		FokosUnavailableError,
		FokosTransactionPendingError,
		FokosRoutingError,
		FokosInternalError,
	].map((category) => [category.tag, category as new (init: FokosErrorInit<string>) => FokosAnyError]),
);

// ─── The code registry ────────────────────────────────────────────────────────

type FokosErrorRegistryEntry = {
	tag: string;
	/** 6 characters from `a-hjkmnp-z2-9`, unique in the registry and fixed for the life of the code. */
	segment: string;
	origin: FokosErrorOrigin;
	httpStatusHint: number;
};

/**
 * Returns a maker of registry entries that carry the common origin and hint of one category.
 *
 * The tag is a literal here and not the static `tag` of the class: the class types derive from this
 * registry, so a reference back to a class is a type cycle. A misspelt tag leaves its category with no
 * code, so no call site of that category compiles.
 */
function codesOf<T extends string>(tag: T, origin: FokosErrorOrigin, httpStatusHint: number) {
	return (segment: string) => ({ tag, segment, origin, httpStatusHint });
}

const validation = codesOf("FokosValidationError", "caller", 400);
const expression = codesOf("FokosExpressionError", "caller", 400);
const conditionCheck = codesOf("FokosConditionCheckError", "caller", 409);
const conflict = codesOf("FokosConflictError", "caller", 409);
const transactionCancelled = codesOf("FokosTransactionCancelledError", "caller", 409);
const transactionPending = codesOf("FokosTransactionPendingError", "service", 503);
const unavailable = codesOf("FokosUnavailableError", "service", 503);
const routing = codesOf("FokosRoutingError", "internal", 500);
const internal = codesOf("FokosInternalError", "internal", 500);

/**
 * Every code, with its category, its 6-character `error_id` segment, and its default origin and hint.
 * A constructor takes the defaults unless the call site passes others, so a consumer must read the
 * fields on the error and not this registry.
 */
export const FOKOS_ERROR_REGISTRY = {
	hash_key_empty: validation("2fzzq9"),
	sort_key_empty: validation("2gjvju"),
	key_contains_nul: validation("42r8z7"),
	key_not_well_formed_utf16: validation("4767pp"),
	hash_key_too_large: validation("4v2p4p"),
	sort_key_too_large: validation("58daxm"),
	key_encode_empty: validation("58sjts"),
	item_data_too_large: validation("6z7eb3"),
	item_data_wrong_type: validation("7vxpb8"),
	item_data_not_json_serializable: validation("bfvvtt"),
	ttl_at_invalid: validation("brcy77"),
	return_values_option_invalid: validation("ed9wyr"),
	client_request_token_invalid: validation("f9azze"),
	idempotent_parameter_mismatch: validation("fn733z"),
	transact_items_empty: validation("gmjfgw"),
	transact_items_too_many: validation("h58dgv"),
	transact_duplicate_key: validation("hgxg2r"),
	transact_payload_too_large: validation("hsvepa"),
	transact_operation_fields_invalid: validation("jr49a5"),
	query_queries_empty: validation("k44ag9"),
	query_limit_invalid: validation("k4g8z5"),
	query_max_page_bytes_invalid: validation("k7zmpj"),
	cursor_malformed: validation("pndxkq"),
	cursor_version_unknown: validation("s62ybe"),
	cursor_query_index_out_of_range: validation("sevnxx"),
	cursor_direction_mismatch: validation("sfcvks"),
	cursor_fingerprint_mismatch: validation("t3kbec"),
	num_tx_coordinators_invalid: validation("uc9fkn"),
	expression_invalid: expression("ucjjtz"),
	condition_failed: conditionCheck("usbs9w"),
	item_locked_by_transaction: conflict("vnfeg6"),
	timestamp_conflict: conflict("vw99ky"),
	pending_conflict: conflict("w65ens"),
	read_conflict: conflict("wx4mnz"),
	pending_write: conflict("xam35s"),
	// The partition clock and the transaction clock disagree. A later attempt clears it, so it is a service condition.
	clock_skew: { ...conflict("xy3rrw"), origin: "service", httpStatusHint: 503 },
	item_too_large: validation("ynzx4p"),
	update_not_applicable: validation("yysds3"),
	update_value_is_bytes: validation("z9ar7e"),
	transaction_cancelled: transactionCancelled("zd7rzd"),
	transaction_undecided: transactionPending("28ahbe"),
	transaction_commit_pending: transactionPending("3wbgez"),
	partition_over_size: unavailable("49j6ez"),
	partition_migrating: unavailable("4rpgyu"),
	coordinator_over_size: unavailable("tg8r62"),
	prepare_unanswered: unavailable("mpncbz"),
	partition_misrouted: routing("6ddzyj"),
	range_partition_not_initialized: routing("6ue24c"),
	single_partition_fast_path_not_applicable: routing("7647dt"),
	invariant_failed: internal("85quf8"),
	partition_context_mismatch: internal("8hv63q"),
	stored_item_too_large: internal("cd8y95"),
	item_data_parse_failed: internal("dx9mht"),
	commit_keyset_mismatch: internal("e3kh5s"),
	item_not_found_for_update: internal("h5vq43"),
	unexpected_transaction_state: internal("j6uhd6"),
	partition_fanout_failed: internal("f3aqhc"),
	foreign_error: internal("jvufz5"),
} satisfies Record<string, FokosErrorRegistryEntry>;

type FokosErrorRegistry = typeof FOKOS_ERROR_REGISTRY;

export type FokosErrorCode = keyof FokosErrorRegistry;

/** The codes of one category. For `string` it is every code. */
export type FokosErrorCodeOf<T extends string> = {
	[C in FokosErrorCode]: FokosErrorRegistry[C]["tag"] extends T ? C : never;
}[FokosErrorCode];
