/**
 * PROTOTYPE. The surface of `fokosdb/sharding` as the other files in this directory consume it. Each of
 * them is a host, a client, or a contract written against the real runtime, and `pnpm check` (tsc) is the
 * test: a change to the public surface that breaks a host shows up here before it reaches a package user.
 */
export * from "../../src/sharding/index.js";

/** Ordinary code that is not part of the sharding surface. Prototype helper for a body that is not written. */
export function todo<T = never>(what: string): T {
	throw new Error(`prototype: ${what}`);
}
