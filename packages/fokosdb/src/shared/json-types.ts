export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
// The write API accepts only a composite at the top level, never a top-level primitive.
export type JsonComposite = JsonValue[] | { [key: string]: JsonValue };
