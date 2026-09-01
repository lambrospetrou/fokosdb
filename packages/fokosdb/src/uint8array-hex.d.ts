// Type declarations for Uint8Array.toHex() / Uint8Array.fromHex() (ES2025 standard, not yet in TypeScript's es2024 lib).
interface Uint8Array {
	toHex(): string;
	toBase64(options?: { alphabet?: "base64" | "base64url"; omitPadding?: boolean } = { alphabet: "base64", omitPadding: false }): string;
}

interface Uint8ArrayConstructor {
	fromHex(hex: string): Uint8Array;
	fromBase64(
		b64: string,
		options?: { alphabet?: "base64" | "base64url"; lastChunkHandling?: "loose" | "strict" | "stop-before-partial" } = {
			alphabet: "base64",
			lastChunkHandling: "loose",
		},
	): Uint8Array;
}
