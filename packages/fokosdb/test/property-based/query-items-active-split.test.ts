// Property-based tests for queryItems while the range tree under it is still splitting.
//
// query-items-split.test.ts settles the tree first, so its walk always reads a topology that stands
// still. This suite keeps writing to its fixture: before each run it fills one leaf past its
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
// query-harness.ts holds the fixture, the request arbitrary, the model and the key oracle. The
// fixture stops at the promotion of the hot key, and the churn makes every split after it.
//
// A query of a promoted hash key must enter its range tree at the root. An operation that spans the
// sort-key axis carries no single sort key, so it can never select a node from a point-keyed cache:
// such a node covers part of the interval, and the page it answers stops short with no cursor.
// `apiQueryItems` therefore declares `shape: "range"`, which has no entry-point key at all, and the
// interval planner picks nodes that fully contain each segment.
// docs/ideas/2026-09-20-query-entry-point-into-a-range-tree.md records the defect this prevents.
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { FokosError, FokosUnavailableError, UNAVAILABLE_CODES } from "../../src/shared/errors.js";
import type { PutItemResult, QueryItemsOptions } from "../../src/shared/types.js";
import { propertyRuns, sleep, untilAvailable } from "./harness.js";
import {
	budgetFor,
	buildSplittingFixture,
	drainFromCursor,
	expectDrainedAnswer,
	expectedAnswer,
	expectedItems,
	hotSortKey,
	HOT_HASH_KEY,
	itemId,
	makeArbRequest,
	publicItem,
	RANGE_SPLIT_MAX_SIZE_MB,
	recordItem,
	type Fixture,
	type Query,
	type QueryKey,
} from "./query-harness.js";

const SUITE_TIMEOUT_MS = 600_000;
// A run with a split costs about 0.6 s alone, a tree build included. This allows for the load of the
// full suite, so a high run count gets more time than the fixed suite limit.
const timeoutFor = (runs: number) => Math.max(SUITE_TIMEOUT_MS, runs * 5_000);

// ─── The churn ────────────────────────────────────────────────────────────────

// A churn item is large. Thus few writes fill one leaf from half full to more than its cap. A split
// costs approximately one third of a megabyte of writes. A larger cost gives more time to the writes
// than to the reads.
// The item must also stay below one tenth of the range cap. The write that goes past the cap then
// keeps the leaf in the 1.1x band, and the next push can write to the leaf while its split operates.
// A larger item puts the leaf above the band. The partition then refuses each write until the split
// is complete, and a push that gets a refusal queues no split. The properties then read a tree that
// does not change, and the settled suite already does that test.
const CHURN_ITEM_BYTES = 32 * 1024;
// The live churn items a query has to read. SQLite keeps the space of a deleted row, so the leaf
// still grows towards its cap and still splits, while the answer a property compares stays small.
const CHURN_LIVE_MAX = 12;
const MAX_PUTS_PER_PUSH = 20;
// One tree gets no more than this. A split needs about 11 puts, so a tree ends at about 20 leaves.
// Every split adds two partitions and every partition adds a hop to a scan, so an unbounded churn
// would spend the suite on a walk of the tree instead of a check of it — and a deep search even more.
const CHURN_PUT_BUDGET = 200;
const RANGE_CAP_BYTES = RANGE_SPLIT_MAX_SIZE_MB * 1024 * 1024;
// The runs of one property that share a tree. A push that queues a split uses about 10 puts. A run
// after the budget reads a tree that does not change, and query-items-split.test.ts already does
// that test. Thus a property builds a new tree and a new churn for each batch of runs, and does not
// run on after the budget.
const RUNS_PER_TREE = CHURN_PUT_BUDGET / 10;
// The time the churn tries a write again after another unavailable error. This limit finds a table
// that stopped. It must not decide a table that is only slow, thus it is much more than the time a
// retry needs under the load of the full suite.
const CHURN_RETRY_BUDGET_MS = 10_000;

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
	 * Runs one write and reports a refusal instead of a wait for it to clear. Two refusals apply:
	 *
	 * - A partition above 1.1 times its cap refuses every write, a delete included, until its split
	 *   makes room. A wait for that would close the window that this suite reads in.
	 * - A child that still imports refuses every write with `partition_migrating`. The length of the
	 *   import depends on the load of the machine, thus a wait for it measures the clock.
	 *
	 * Both refusals occur before the write applies. The churn never writes a refused key again and
	 * never puts it in the model, so the model still knows every version it holds.
	 */
	async #write<T>(op: () => Promise<T>): Promise<T | "refused"> {
		const deadline = Date.now() + CHURN_RETRY_BUDGET_MS;
		for (;;) {
			try {
				return await op();
			} catch (e) {
				if (FokosError.isCode(e, UNAVAILABLE_CODES.partition_over_size)) return "refused";
				if (FokosError.isCode(e, UNAVAILABLE_CODES.partition_migrating)) return "refused";
				if (!FokosUnavailableError.is(e) || Date.now() >= deadline) throw e;
				await sleep(25);
			}
		}
	}
}

/** The stable sort keys the churn writes around, spread over the hot items of the fixture. */
const churnRegions = (fixture: Fixture): number[] =>
	[0.1, 0.35, 0.6, 0.85].map((fraction) => Math.floor(fraction * fixture.hotSortKeys.length));

/**
 * Whether one page shows a child that answers from its source. A leaf is listed once per sub-query
 * that reaches it, so this is read off a request of ONE sub-query only: there, a name that appears
 * twice is one source that answers for two importing children of the same split.
 */
const readsThroughToSource = (names: readonly string[]) => new Set(names).size < names.length;

/**
 * A text that changes with every split of the hot key. A response names only the leaves that fit in
 * its route data, so the names alone can miss a split late in the walk. `partitionsVisited` counts
 * every leaf that answered, so a split changes it immediately.
 */
async function topologyOf(fixture: Fixture): Promise<string> {
	const page = await untilAvailable(() => fixture.db.queryItems({ queries: [{ hashKey: HOT_HASH_KEY }], select: "count", limit: 100_000 }));
	return `${page.meta.partitionsVisited}:${page.partitionMetas.map((meta) => meta.servedByActorName).join()}`;
}

/** Polls the topology of the hot key until it is not `before`, or until the last attempt. */
async function awaitTopologyChange(fixture: Fixture, before: string, attempts: number): Promise<void> {
	for (let attempt = 0; attempt < attempts; attempt++) {
		if ((await topologyOf(fixture)) !== before) return;
		await sleep(25);
	}
}

/** A range tree and the churn that keeps it splitting. */
type Tree = { fixture: Fixture; churn: Churn };

async function buildTree(): Promise<Tree> {
	const fixture = await buildSplittingFixture();
	return { fixture, churn: new Churn(fixture, churnRegions(fixture)) };
}

/** Runs a property `numRuns` times, on a new tree for each batch of `RUNS_PER_TREE` runs. */
async function assertOnSplittingTrees(
	numRuns: number,
	run: (tree: Tree, request: { queries: Query[]; budget: number }) => Promise<void>,
): Promise<void> {
	for (let done = 0; done < numRuns; done += RUNS_PER_TREE) {
		const tree = await buildTree();
		await fc.assert(
			fc.asyncProperty(makeArbRequest(tree.fixture, tree.churn.bounds()), (request) => run(tree, request)),
			{ numRuns: Math.min(RUNS_PER_TREE, numRuns - done) },
		);
	}
}

// ─── The suite ────────────────────────────────────────────────────────────────

describe("FokosDB queryItems while a range tree splits — model-based properties", () => {
	/** Counts the whole hot key and says whether a page came from a source instead of its child. */
	async function probeHotKey(fixture: Fixture): Promise<{ count: number; readThrough: boolean }> {
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

	// This runs first, and it is the one test that proves the window exists. If a query never meets
	// an importing child, the properties below still pass and cover nothing new, so the suite says so
	// here instead of a report of a silent success.
	it("a query answers from the source while the children of a split still import", { timeout: SUITE_TIMEOUT_MS }, async () => {
		const { fixture, churn } = await buildTree();
		const hotCount = () => expectedItems(fixture.model, { hashKey: HOT_HASH_KEY }).length;
		for (let split = 0; split < 6; split++) {
			const pushed = await churn.push();
			expect(pushed, "the churn must be able to fill a leaf past its cap").not.toBe("budget");
			// A tight loop of probes covers the window between the creation of the children and the last
			// import that completes. Every probe must also agree with the model: an importing child
			// answers from its source, and that answer is the same answer.
			for (let probe = 0; probe < 80; probe++) {
				const { count, readThrough } = await probeHotKey(fixture);
				expect(count, "the count of the hot key must not change while its leaves split").toBe(hotCount());
				if (readThrough) return;
				await sleep(5);
			}
		}
		throw new Error("no page ever showed a child reading through its source; the suite covers nothing new");
	});

	it("a drained request returns exactly the model's items, in key order", { timeout: timeoutFor(propertyRuns(20)) }, async () => {
		await assertOnSplittingTrees(propertyRuns(20), async ({ fixture, churn }, { queries, budget }) => {
			await churn.push();
			const expected = expectedAnswer(fixture.model, queries);
			await expectDrainedAnswer(fixture.db, expected, { queries, ...budgetFor(budget, expected, fixture) });
		});
	});

	it("count mode counts the same items and materializes none", { timeout: timeoutFor(propertyRuns(10)) }, async () => {
		await assertOnSplittingTrees(propertyRuns(10), async ({ fixture, churn }, { queries, budget }) => {
			await churn.push();
			const expected = expectedAnswer(fixture.model, queries);
			await expectDrainedAnswer(fixture.db, expected, { queries, ...budgetFor(budget, expected, fixture), select: "count" });
		});
	});

	it("a cursor minted before a split resumes exactly after it", { timeout: timeoutFor(propertyRuns(10)) }, async () => {
		await assertOnSplittingTrees(propertyRuns(10), async ({ fixture, churn }, { queries, budget }) => {
			const pushed = await churn.push();
			const expected = expectedAnswer(fixture.model, queries);
			const opts: QueryItemsOptions = { queries, ...budgetFor(budget, expected, fixture) };

			const before = await topologyOf(fixture);
			const first = await untilAvailable(() => fixture.db.queryItems(opts));
			if (first.cursor === undefined) return;
			// The cursor names a position in the topology that the first page walked. A wait for the
			// leaves to change redeems it in another topology, where the child it stopped in is two
			// children and the position it carries has to resolve to the same place. A window that
			// closes before the leaves move leaves the rest of the property just as valid, so nothing
			// is asserted here. A push that queued no split cannot move the leaves, so the wait
			// operates only after a queued split.
			if (pushed === "queued") await awaitTopologyChange(fixture, before, 20);

			const rest = await drainFromCursor(fixture.db, opts, first.cursor, expected.length);
			const expectedPublic = expected.map(publicItem);
			expect(first.items).toEqual(expectedPublic.slice(0, first.items.length));
			expect([...first.items, ...rest]).toEqual(expectedPublic);
		});
	});
});
