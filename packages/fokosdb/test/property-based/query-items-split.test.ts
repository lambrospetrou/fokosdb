// Property-based tests for queryItems over a key whose partition has split into a range tree.
//
// The single-partition suite (query-items.test.ts) never leaves one leaf, so it never runs the range
// walk of the client. This suite promotes one hash key into a range tree of several leaves and then
// queries it. Every page therefore crosses child boundaries: the walk clips the interval to each
// child, carries one page budget across the children, and turns a stopped child into a cursor that
// the next page must resume from exactly. A gap, a duplicate, a lost boundary item, or a cursor that
// does not resume shows up as a mismatch with the model.
//
// The tree is settled here: nothing writes to it, and no split runs while a property reads it.
// query-items-active-split.test.ts asks the same questions of a tree that is still splitting.
//
// The fixture is built once. Queries change nothing, so every run and every property shares it, and
// one run costs only the pages it drains. query-harness.ts holds the fixture, the request arbitrary,
// the model and the key oracle.
import fc from "fast-check";
import { beforeAll, describe, expect, it } from "vitest";
import type { QueryItemsOptions, QueryItemsResult } from "../../src/shared/types.js";
import { propertyRuns } from "./harness.js";
import {
	budgetFor,
	buildFixture,
	expectDrainedAnswer,
	expectedAnswer,
	makeArbRequest,
	publicItem,
	type Fixture,
	type Query,
} from "./query-harness.js";

const SUITE_TIMEOUT_MS = 600_000;

/**
 * A cursor must not promise a page that cannot exist. The walk emits one when a later child of the
 * tree can still hold rows. Once it has reached the last child of its direction AND the request has
 * matched everything the model holds, no later child is left and the answer is complete, so a cursor
 * there buys a round trip that returns nothing.
 *
 * The rule applies to a request of ONE sub-query. A request that names several carries its cursor
 * from one to the next, and no page knows whether the next sub-query matches anything.
 */
function expectCursorHasSomewhereToGo(
	fixture: Fixture,
	queries: readonly Query[],
	page: QueryItemsResult,
	matchedSoFar: number,
	expectedCount: number,
): void {
	if (queries.length !== 1 || page.cursor === undefined || matchedSoFar < expectedCount) {
		return;
	}
	const lastChildOfTheWalk = queries[0].scanIndexForward === false ? fixture.leaves[0] : fixture.leaves[fixture.leaves.length - 1];
	expect(
		page.partitionMetas.map((meta) => meta.servedByActorName),
		"a cursor was emitted at the last child of the walk, with the whole answer already returned",
	).not.toContain(lastChildOfTheWalk);
}

describe("FokosDB queryItems over a range tree — model-based properties", () => {
	let fixture: Fixture;

	beforeAll(async () => {
		fixture = await buildFixture();
		// Without this the suite falls back to the single-partition case, which query-items.test.ts
		// already covers, and it reports a silent success.
		expect(new Set(fixture.leaves).size, "the hot key must be spread over several range leaves").toBeGreaterThanOrEqual(4);
		expect(fixture.boundaries).toHaveLength(new Set(fixture.leaves).size - 1);
	}, SUITE_TIMEOUT_MS);

	/** Drains one request and checks every page of it against the cursor rule above. */
	async function expectDrainAndCursors(queries: Query[], budget: number, select?: "count"): Promise<void> {
		const expected = expectedAnswer(fixture.model, queries);
		const opts: QueryItemsOptions = { queries, ...budgetFor(budget, expected, fixture), ...(select ? { select } : {}) };
		let matched = 0;
		await expectDrainedAnswer(fixture.db, expected, opts, (page) => {
			matched += page.count;
			expectCursorHasSomewhereToGo(fixture, queries, page, matched, expected.length);
		});
	}

	it("a drained request returns exactly the model's items, in key order", { timeout: SUITE_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(makeArbRequest(fixture), ({ queries, budget }) => expectDrainAndCursors(queries, budget)),
			{ numRuns: propertyRuns(40) },
		);
	});

	it("count mode counts the same items and materializes none", { timeout: SUITE_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(makeArbRequest(fixture), ({ queries, budget }) => expectDrainAndCursors(queries, budget, "count")),
			{ numRuns: propertyRuns(25) },
		);
	});

	it("a cursor answers the same page every time it is sent", { timeout: SUITE_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(makeArbRequest(fixture), async ({ queries, budget }) => {
				const expected = expectedAnswer(fixture.model, queries);
				const opts: QueryItemsOptions = { queries, ...budgetFor(budget, expected, fixture) };

				// Nothing writes to the fixture, so a cursor names one position for as long as it exists.
				// A cursor that carries a child boundary must resolve to that same position on every send,
				// whichever child the walk enters it from.
				const first = await fixture.db.queryItems(opts);
				if (first.cursor === undefined) {
					return;
				}
				const again: QueryItemsResult = await fixture.db.queryItems({ ...opts, cursor: first.cursor });
				const onceMore: QueryItemsResult = await fixture.db.queryItems({ ...opts, cursor: first.cursor });

				expect(again.items).toEqual(onceMore.items);
				expect(again.count).toBe(onceMore.count);
				expect(again.scannedCount).toBe(onceMore.scannedCount);
				expect(again.cursor).toBe(onceMore.cursor);
				// The two pages must also be the right two pages. A request can name one hash key twice,
				// so the same item can appear more than once in the answer. The model compares its
				// sequence by position, and the resumed page must continue exactly where the first one
				// stopped.
				const expectedItemsOfPage = expected.map(publicItem);
				expect(first.items).toEqual(expectedItemsOfPage.slice(0, first.items.length));
				expect(again.items).toEqual(expectedItemsOfPage.slice(first.items.length, first.items.length + again.items.length));
			}),
			{ numRuns: propertyRuns(25) },
		);
	});
});
