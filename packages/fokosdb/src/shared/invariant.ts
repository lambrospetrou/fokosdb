// Adapted from https://github.com/alexreardon/tiny-invariant/blob/master/src/tiny-invariant.ts.

import { FokosInternalError, INTERNAL_CODES } from "./errors.js";

/**
 * 💥 `invariant` will `throw` a `FokosInternalError` with the code `invariant_failed` if the `condition` is [falsey](https://github.com/getify/You-Dont-Know-JS/blob/bdbe570600d4e1107d0b131787903ca1c9ec8140/up%20%26%20going/ch2.md#truthy--falsy)
 *
 * ```ts
 * const value: Person | null = { name: 'Alex' };
 * invariant(value, 'Expected value to be a person');
 * // type of `value` has been narrowed to `Person`
 * ```
 */
export default function invariant(
	condition: any,
	/**
	 * Can provide a string, or a function that returns a string for cases where
	 * the message takes a fair amount of effort to compute
	 */
	message?: string | (() => string),
): asserts condition {
	if (condition) {
		return;
	}
	// Condition not passed

	// The text can hold dynamic detail and internal names, so it goes to the attributes and the message stays fixed.
	const detail: string | undefined = typeof message === "function" ? message() : message;
	throw new FokosInternalError(INTERNAL_CODES.invariant_failed, {
		message: "an internal invariant failed",
		attributes: detail === undefined ? {} : { detail },
	});
}
