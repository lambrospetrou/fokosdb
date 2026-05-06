// Type declarations for Uint8Array.toHex() / Uint8Array.fromHex() (ES2025 standard, not yet in TypeScript's es2024 lib).
interface Uint8Array {
	toHex(): string;
}

interface Uint8ArrayConstructor {
	fromHex(hex: string): Uint8Array;
}
