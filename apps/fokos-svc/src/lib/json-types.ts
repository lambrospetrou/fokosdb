export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
// Top-level accepted composites only (start restricted; top-level primitives excluded initially).
export type JsonComposite = JsonValue[] | { [key: string]: JsonValue };
