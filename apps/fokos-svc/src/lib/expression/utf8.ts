const textEncoder = new TextEncoder();

/**
 * Returns true when the UTF-8 byte length of `text` is within `limit`.
 *
 * UTF-8 byte length is always >= `text.length` (each UTF-16 code unit produces at least one byte)
 * and always <= `3 * text.length` (a BMP code unit produces at most 3 bytes; a surrogate pair is
 * 2 units for 4 bytes). Both bounds decide most inputs without encoding, so the full encode runs
 * only for text in the narrow ambiguous band.
 */
export function utf8WithinLimit(text: string, limit: number): boolean {
	if (text.length > limit) return false;
	if (text.length * 3 <= limit) return true;
	return textEncoder.encode(text).byteLength <= limit;
}
