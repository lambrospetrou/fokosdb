import { describe, expect, it } from "vitest";
import { KeyCodec } from "../../sharding/key-codec.js";
import { planRangeBoundaries } from "./split-planner.js";

const kb = (s: string) => KeyCodec.encode(s);
const rows = (...entries: Array<[string, number]>) => entries.map(([sk, bytes]) => ({ sk: kb(sk), bytes }));

describe("planRangeBoundaries", () => {
	it("gives the floor reason when the rows cannot make N children", () => {
		expect(planRangeBoundaries(0, 2, null, [])).toEqual({ floor: "fewer_items" });
		expect(planRangeBoundaries(10, 3, null, rows(["a", 5], ["b", 5]))).toEqual({ floor: "fewer_items" });
		// The large row crosses the first threshold, and the next threshold is above the bytes that remain.
		expect(planRangeBoundaries(5002, 3, null, rows(["a", 1], ["m", 5000], ["z", 1]))).toEqual({ floor: "skewed_bytes" });
	});

	it("stops reading the rows at the last boundary", () => {
		let read = 0;
		function* source() {
			for (const sk of ["a", "b", "c", "d", "e", "f", "g", "h"]) {
				read++;
				yield { sk: kb(sk), bytes: 10 };
			}
		}
		expect(planRangeBoundaries(80, 2, null, source())).toEqual({ boundaries: [kb("d")] });
		expect(read).toBe(4);
	});
});
