const textEncoder = new TextEncoder();

/**
 * Returns true when the UTF-8 byte length of `text` is within `limit`.
 *
 * UTF-8 byte length is always >= `text.length` (each UTF-16 code unit produces at least one byte)
 * and always <= `3 * text.length` (a BMP code unit produces at most 3 bytes; a surrogate pair is
 * 2 units for 4 bytes). Both bounds decide most inputs from the length. Only a text in the band
 * between them gets a byte count, and the count makes no copy of the text.
 */
export function utf8WithinLimit(text: string, limit: number): boolean {
	if (text.length > limit) {
		return false;
	}
	if (text.length * 3 <= limit) {
		return true;
	}
	return utf8ByteLength(text) <= limit;
}

/** Where `utf8ByteLength` encodes one part of a text at a time. It reads only the count of the bytes. */
const scratch = new Uint8Array(16 * 1024);

/**
 * The UTF-8 byte length of `text`, counted by the platform encoder.
 *
 * `encodeInto` writes into the fixed scratch buffer, so the function makes no copy of the text. One
 * call encodes the characters that fit in the buffer, and never one half of a character. It reports
 * the code units that it read and the bytes that it wrote, and the loop continues after the units
 * that it read. The buffer holds more than one character, so each call reads at least one code unit.
 */
export function utf8ByteLength(text: string): number {
	let bytes = 0;
	let offset = 0;
	while (offset < text.length) {
		const { read, written } = textEncoder.encodeInto(offset === 0 ? text : text.substring(offset), scratch);
		bytes += written;
		offset += read;
	}
	return bytes;
}
