import { describe, expect, it } from "vitest";
import { joinBalanced } from "./sql-join.js";

describe("joinBalanced", () => {
	const terms = (count: number) => Array.from({ length: count }, (_, i) => `t${i}`);

	it("gives the flat text for a list of two terms or fewer", () => {
		expect(joinBalanced([], " AND ")).toBe("");
		expect(joinBalanced(terms(1), " AND ")).toBe("t0");
		expect(joinBalanced(terms(2), " OR ")).toBe("t0 OR t1");
	});

	it("puts the terms in balanced groups, in their order", () => {
		expect(joinBalanced(terms(3), " AND ")).toBe("t0 AND (t1 AND t2)");
		expect(joinBalanced(terms(4), " AND ")).toBe("(t0 AND t1) AND (t2 AND t3)");
		expect(joinBalanced(terms(5), " OR ")).toBe("(t0 OR t1) OR (t2 OR (t3 OR t4))");
	});

	it("keeps the depth of a chain at the base 2 logarithm of its count of terms", () => {
		for (const count of [6, 67, 94, 299]) {
			const sql = joinBalanced(terms(count), " AND ");
			expect(sql.replace(/[()]/g, "")).toBe(terms(count).join(" AND "));
			let depth = 0;
			let deepest = 0;
			for (const character of sql) {
				depth += character === "(" ? 1 : character === ")" ? -1 : 0;
				deepest = Math.max(deepest, depth);
			}
			// The outer operator is one level, and each level of parentheses is one more.
			expect(deepest + 1, `${count} terms`).toBe(Math.ceil(Math.log2(count)));
		}
	});
});
