import type { JsonValue } from "../json-types.js";
import invariant from "../invariant.js";

export type ProjectedWireCell = undefined | null | boolean | number | string | Uint8Array | { json: string };
export type ProjectedWireRow = ProjectedWireCell[];
export type ProjectedValue = JsonValue | Uint8Array;
export type ProjectedItem = Record<string, ProjectedValue>;

/** Converts one type column and one value column into a wire cell. */
export function decodeProjectionCell(typeName: unknown, value: unknown): ProjectedWireCell {
	switch (typeName) {
		case "missing":
			return undefined;
		case "null":
			return null;
		case "boolean":
			return value === 1;
		case "number":
			return value as number;
		case "text":
			return value as string;
		case "bytes":
			// Workers SQLite returns a BLOB as an ArrayBuffer.
			return value instanceof Uint8Array ? value : new Uint8Array(value as ArrayBuffer);
		case "array":
		case "object":
			return { json: value as string };
		default:
			invariant(false, `fokos/expression: unknown projection cell type ${String(typeName)}`);
	}
}

/** Reads the p{k} and t{k} columns of one SQL row into a positional wire row. */
export function decodeProjectedRow(row: Record<string, unknown>, entryCount: number): ProjectedWireRow {
	const cells: ProjectedWireRow = [];
	for (let k = 0; k < entryCount; k++) {
		cells.push(decodeProjectionCell(row[`t${k}`], row[`p${k}`]));
	}
	return cells;
}

/** Builds the flat record from a wire row and the resolved names; a missing cell is omitted and JSON text is parsed once. */
export function projectedItemFromWireRow(names: readonly string[], row: ProjectedWireRow): ProjectedItem {
	const entries: [string, ProjectedValue][] = [];
	for (let k = 0; k < names.length; k++) {
		const cell = row[k];
		if (cell === undefined) continue;
		entries.push([names[k], cell !== null && typeof cell === "object" && "json" in cell ? (JSON.parse(cell.json) as JsonValue) : cell]);
	}
	return Object.fromEntries(entries);
}
