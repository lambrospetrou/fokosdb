/**
 * Joins SQL terms with one operator, `" AND "` or `" OR "`, as a balanced tree of groups in parentheses.
 *
 * SQLite parses `a AND b AND c AND d` as `((a AND b) AND c) AND d`, so each term of a flat chain adds
 * one level to the expression depth, and SQLite refuses a statement whose expression is too deep.
 * `(a AND b) AND (c AND d)` has a depth of `ceil(log2(n))` for n terms.
 *
 * The result is the same as the flat chain: `AND` and `OR` are associative, also for a NULL term, and
 * the terms keep their order, so SQLite evaluates them from left to right. A list of one term or two
 * terms gives the text of the flat chain.
 */
export function joinBalanced(terms: readonly string[], operator: " AND " | " OR "): string {
	return terms.length <= 2 ? terms.join(operator) : joinRange(terms, 0, terms.length, operator);
}

/** Joins the terms from `start` to before `end`. A half with more than one term gets parentheses. */
function joinRange(terms: readonly string[], start: number, end: number, operator: string): string {
	if (end - start === 1) {
		return terms[start];
	}
	const middle = start + ((end - start) >> 1);
	const left = joinRange(terms, start, middle, operator);
	const right = joinRange(terms, middle, end, operator);
	return `${middle - start > 1 ? `(${left})` : left}${operator}${end - middle > 1 ? `(${right})` : right}`;
}
