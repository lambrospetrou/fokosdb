// Property-based tests for queryItems while the range tree under it is still splitting.
//
// query-items-split.test.ts settles the tree first, so its walk always reads a topology that stands
// still. This suite keeps writing to the same fixture: before each run it fills one leaf past its
// cap, which queues a split, and the run then reads the tree while that leaf turns into a router and
// its two children import their share. Two paths exist only inside that window.
//
//   - An importing child answers from its SOURCE, and the source keeps the rows of the WHOLE parent
//     range. The interval the walk clips to each child is therefore the only thing that stops a
//     child from returning rows its sibling owns, and a clip that is too wide shows up as a
//     duplicate or an out-of-order item.
//   - A cursor is minted under one topology and redeemed under another: the child it stopped in has
//     become two children by the time the next page resumes from it.
//
// The answer itself must not move. A split relocates rows between partitions and changes nothing
// about what the table holds, and the churn stops before each drain, so the model stays exact while
// a migration runs in the background and the pages arrive.
//
// query-harness.ts holds the fixture, the request arbitrary, the model and the key oracle.
//
// A query of a promoted hash key must enter its range tree at the root. An operation that spans the
// sort-key axis carries no single sort key, so it can never select a node from a point-keyed cache:
// such a node covers part of the interval, and the page it answers stops short with no cursor.
// `apiQueryItems` therefore declares `shape: "range"`, which has no entry-point key at all, and the
// interval planner picks nodes that fully contain each segment.
// docs/ideas/2026-09-20-query-entry-point-into-a-range-tree.md records the defect this prevents.
import fc from "fast-check";
import { beforeAll, describe, expect, it } from "vitest";
import { FokosError, FokosUnavailableError, UNAVAILABLE_CODES } from "../../src/shared/errors.js";
import type { PutItemResult, QueryItemsOptions } from "../../src/shared/types.js";
import { propertyRuns, sleep, untilAvailable } from "./harness.js";
import {
	budgetFor,
	buildFixture,
	drainFromCursor,
	expectDrainedAnswer,
	expectedAnswer,
	expectedItems,
	hotSortKey,
	HOT_HASH_KEY,
	HOT_ITEMS,
	itemId,
	leavesOf,
	makeArbRequest,
	pollLeaves,
	publicItem,
	RANGE_SPLIT_MAX_SIZE_MB,
	recordItem,
	type Fixture,
	type QueryKey,
} from "./query-harness.js";

const SUITE_TIMEOUT_MS = 600_000;

// ─── The churn ────────────────────────────────────────────────────────────────

// A churn item is large, so few writes carry one leaf from half full to past its cap: a split costs
// about a third of a megabyte of writes, and a run that paid for a whole megabyte would spend more
// time on writes than on reads.
const CHURN_ITEM_BYTES = 32 * 1024;
// The live churn items a query has to read. SQLite keeps the space of a deleted row, so the leaf
// still grows towards its cap and still splits, while the answer a property compares stays small.
const CHURN_LIVE_MAX = 24;
const MAX_PUTS_PER_PUSH = 20;
// The whole suite writes no more than this, which is about a dozen splits. Every split adds two
// partitions and every partition adds a hop to a scan, so an unbounded churn would spend the whole
// suite on a walk of the tree instead of a check of it — and a deep search even more.
const CHURN_PUT_BUDGET = 200;
const RANGE_CAP_BYTES = RANGE_SPLIT_MAX_SIZE_MB * 1024 * 1024;
// One second. A write meets `partition_migrating` while a child imports, and that clears in a moment.
const CHURN_RETRY_LIMIT = 40;

/** A churn sort key sits between two stable ones, so the writes land inside the tree and not above it. */
const churnSortKey = (region: number, seq: number) => `${hotSortKey(region)}.w${String(seq).padStart(4, "0")}`;
const churnData = (sortKey: string) => `${sortKey}:`.padEnd(CHURN_ITEM_BYTES, "x");

/** The outcome of one push. Only `queued` promises that a split is on its way. */
type PushResult = "queued" | "refused" | "budget" | "under_cap";

/**
 * The writer that keeps the tree splitting. One push fills a single region of the hot key until the
 * leaf that owns it reports a database past the range cap: that write queued the split, so the walk
 * the caller runs next crosses a router whose children still import.
 *
 * The churn writes each key once and never again, so the model knows every version without a read.
 */
class Churn {
	#seq = 0;
	#puts = 0;
	#pushes = 0;
	readonly #live: string[] = [];

	constructor(
		private readonly fixture: Fixture,
		private readonly regions: readonly number[],
	) {}

	/** The bounds a request can draw. Some of these keys exist by then and some never will. */
	bounds(): QueryKey[] {
		return this.regions.flatMap((region) => [
			hotSortKey(region),
			`${hotSortKey(region)}.`,
			churnSortKey(region, 1),
			churnSortKey(region, 9999),
		]);
	}

	async push(): Promise<PushResult> {
		// The regions rotate, so the splits happen over the whole tree and not in one stretch of it.
		const region = this.regions[this.#pushes++ % this.regions.length];
		for (let i = 0; i < MAX_PUTS_PER_PUSH; i++) {
			if (this.#puts >= CHURN_PUT_BUDGET) return "budget";
			const result = await this.#put(churnSortKey(region, this.#seq++));
			if (result === "refused") return "refused";
			this.#puts++;
			await this.#trim();
			if (result.meta.databaseSize > RANGE_CAP_BYTES) return "queued";
		}
		return "under_cap";
	}

	/** Writes one churn item and records it in the model. */
	async #put(sortKey: string): Promise<PutItemResult | "refused"> {
		const data = churnData(sortKey);
		const res = await this.#write(() => this.fixture.db.putItem({ hashKey: HOT_HASH_KEY, sortKey, data }));
		if (res === "refused") return res;
		recordItem(this.fixture.model, HOT_HASH_KEY, sortKey, data, res.version);
		this.#live.push(sortKey);
		return res;
	}

	/**
	 * Deletes the oldest churn items once too many are live. A refused delete leaves its key live and
	 * a later trim removes it: the model only ever holds what the table answered for.
	 */
	async #trim(): Promise<void> {
		while (this.#live.length > CHURN_LIVE_MAX) {
			const sortKey = this.#live[0];
			const res = await this.#write(() => this.fixture.db.deleteItem({ hashKey: HOT_HASH_KEY, sortKey }));
			if (res === "refused") return;
			this.#live.shift();
			this.fixture.model.delete(itemId(HOT_HASH_KEY, sortKey));
		}
	}

	/**
	 * Runs one write and reports a refusal instead of a wait for it to clear. A partition above 1.1
	 * times its cap refuses every write, a delete included, until its split makes room, and a wait
	 * for that would close the very window this suite reads in. The churn never writes a refused key
	 * again and never puts it in the model, so the model still knows every version it holds.
	 */
	async #write<T>(op: () => Promise<T>): Promise<T | "refused"> {
		for (let attempt = 0; ; attempt++) {
			try {
				return await op();
			} catch (e) {
				if (FokosError.isCode(e, UNAVAILABLE_CODES.partition_over_size)) return "refused";
				if (!FokosUnavailableError.is(e) || attempt >= CHURN_RETRY_LIMIT) throw e;
				await sleep(25);
			}
		}
	}
}

/**
 * The stable sort keys the churn writes around: spread over the tree, and never inside the stretch
 * the fixture emptied, which has to stay empty to keep covering the child that carries no rows.
 */
function churnRegions(fixture: Fixture): number[] {
	const emptied = new Set(fixture.emptiedKeys.filter((key): key is string => typeof key === "string"));
	return [0.1, 0.35, 0.6, 0.85].map((fraction) => {
		let index = Math.floor(fraction * HOT_ITEMS);
		while (index < HOT_ITEMS - 1 && emptied.has(hotSortKey(index))) index++;
		return index;
	});
}

/**
 * Whether one page shows a child that answers from its source. A leaf is listed once per sub-query
 * that reaches it, so this is read off a request of ONE sub-query only: there, a name that appears
 * twice is one source that answers for two importing children of the same split.
 */
const readsThroughToSource = (names: readonly string[]) => new Set(names).size < names.length;

// ─── The suite ────────────────────────────────────────────────────────────────

describe("FokosDB queryItems while a range tree splits — model-based properties", () => {
	let fixture: Fixture;
	let churn: Churn;

	/** Counts the whole hot key and says whether a page came from a source instead of its child. */
	async function probeHotKey(): Promise<{ count: number; readThrough: boolean }> {
		let count = 0;
		let readThrough = false;
		let cursor: string | undefined;
		do {
			const page = await untilAvailable(() =>
				fixture.db.queryItems({ queries: [{ hashKey: HOT_HASH_KEY }], select: "count", limit: 100_000, cursor }),
			);
			count += page.count;
			readThrough ||= readsThroughToSource(page.partitionMetas.map((meta) => meta.servedByActorName));
			cursor = page.cursor;
		} while (cursor !== undefined);
		return { count, readThrough };
	}

	const hotCount = () => expectedItems(fixture.model, { hashKey: HOT_HASH_KEY }).length;

	beforeAll(async () => {
		fixture = await buildFixture();
		expect(new Set(fixture.leaves).size, "the hot key must be spread over several range leaves").toBeGreaterThanOrEqual(4);
		churn = new Churn(fixture, churnRegions(fixture));
	}, SUITE_TIMEOUT_MS);

	// This runs first, and it is the one test that proves the window exists. If a query never meets
	// an importing child, the properties below still pass and cover nothing new, so the suite says so
	// here instead of a report of a silent success.
	it("a query answers from the source while the children of a split still import", { timeout: SUITE_TIMEOUT_MS }, async () => {
		for (let split = 0; split < 6; split++) {
			const pushed = await churn.push();
			expect(pushed, "the churn must be able to fill a leaf past its cap").not.toBe("budget");
			// A tight loop of probes covers the window between the creation of the children and the last
			// import that completes. Every probe must also agree with the model: an importing child
			// answers from its source, and that answer is the same answer.
			for (let probe = 0; probe < 80; probe++) {
				const { count, readThrough } = await probeHotKey();
				expect(count, "the count of the hot key must not change while its leaves split").toBe(hotCount());
				if (readThrough) return;
			}
		}
		throw new Error("no page ever showed a child reading through its source; the suite covers nothing new");
	});

	it("a drained request returns exactly the model's items, in key order", { timeout: SUITE_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(makeArbRequest(fixture, churn.bounds()), async ({ queries, budget }) => {
				await churn.push();
				const expected = expectedAnswer(fixture.model, queries);
				await expectDrainedAnswer(fixture.db, expected, { queries, ...budgetFor(budget, expected, fixture) });
			}),
			{ numRuns: propertyRuns(20) },
		);
	});

	it("count mode counts the same items and materializes none", { timeout: SUITE_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(makeArbRequest(fixture, churn.bounds()), async ({ queries, budget }) => {
				await churn.push();
				const expected = expectedAnswer(fixture.model, queries);
				await expectDrainedAnswer(fixture.db, expected, { queries, ...budgetFor(budget, expected, fixture), select: "count" });
			}),
			{ numRuns: propertyRuns(10) },
		);
	});

	it("a cursor minted before a split resumes exactly after it", { timeout: SUITE_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(makeArbRequest(fixture, churn.bounds()), async ({ queries, budget }) => {
				await churn.push();
				const expected = expectedAnswer(fixture.model, queries);
				const opts: QueryItemsOptions = { queries, ...budgetFor(budget, expected, fixture) };

				const before = await leavesOf(fixture.db, { queries: [{ hashKey: HOT_HASH_KEY }] });
				const first = await untilAvailable(() => fixture.db.queryItems(opts));
				if (first.cursor === undefined) return;
				// The cursor names a position in the topology that the first page walked. A wait for the
				// leaves to change redeems it in another topology, where the child it stopped in is two
				// children and the position it carries has to resolve to the same place. A window that
				// closes before the leaves move leaves the rest of the property just as valid, so nothing
				// is asserted here.
				await pollLeaves(fixture.db, (now) => now.join() !== before.join(), 20);

				const rest = await drainFromCursor(fixture.db, opts, first.cursor, expected.length);
				const expectedPublic = expected.map(publicItem);
				expect(first.items).toEqual(expectedPublic.slice(0, first.items.length));
				expect([...first.items, ...rest]).toEqual(expectedPublic);
			}),
			{ numRuns: propertyRuns(10) },
		);
	});
});
