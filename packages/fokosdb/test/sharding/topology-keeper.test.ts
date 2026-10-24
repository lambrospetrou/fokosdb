import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
	FokosTopologyKeeper,
	KeyCodec,
	TOPOLOGY_BODY_CHUNK_BYTES,
	fokosTopologyEntrypointFetch,
	type FokosTopologyKeeperOptions,
	type FokosTopologyRead,
	type KeyBytes,
} from "../../src/sharding/index-server.js";
import {
	maybePromoted,
	parseHashBody,
	parseRangeBody,
	rangeBoundaryAt,
	rangeCutKey,
	rangeIntervalLookup,
	rangePointLookup,
	type RangeTarget,
	type RangeTopologyView,
} from "../../src/sharding/topology-codec.js";
import { hashSplitReport, promotionReport, rangeSplitReport } from "./topology-events.js";

const LATER = Number.MAX_SAFE_INTEGER;

/** Runs `fn` with a new keeper in a new admin object. The keeper records each build it asks for. */
async function withKeeper<T>(
	fn: (
		keeper: FokosTopologyKeeper,
		ctx: { storage: DurableObjectStorage; scheduled: number[]; sg: string; newKeeper: () => FokosTopologyKeeper },
	) => T | Promise<T>,
	opts: Omit<FokosTopologyKeeperOptions, "scheduleBuild"> = {},
): Promise<T> {
	const stub = env.TOPOLOGY_ADMIN_DO.getByName(`keeper-${crypto.randomUUID()}`);
	return await runInDurableObject(stub, async (_instance, state) => {
		const scheduled: number[] = [];
		const newKeeper = () => {
			const keeper = new FokosTopologyKeeper(state.storage, {
				...opts,
				buildDelayMs: 1_000,
				scheduleBuild: (at) => void scheduled.push(at),
			});
			keeper.runMigrations();
			return keeper;
		};
		return await fn(newKeeper(), { storage: state.storage, scheduled, sg: `sg_${crypto.randomUUID()}`, newKeeper });
	});
}

function body(read: FokosTopologyRead): Uint8Array {
	expect(read.status).toBe(200);
	return (read as { body: Uint8Array }).body;
}

const T = { rootTreesN: 2, hashSplitN: 2 };

describe("topology keeper — reports", () => {
	it("ignores a repeated report, and derives the ancestors of a child that arrives before its parent", async () => {
		await withKeeper(async (keeper, { sg, scheduled }) => {
			const child = hashSplitReport(sg, T, 0, [1], 1_000);
			await keeper.registerRepartition(child, 5_000);
			await keeper.registerRepartition(child, 5_100);
			// One build for the delay: the second report does not move it.
			await keeper.registerRepartition(hashSplitReport(sg, T, 1, [], 1_000), 5_200);
			expect(scheduled).toEqual([6_000, 6_000]);
			keeper.runDueBuilds(5_999);
			expect(parseHashBody(body(keeper.getTopology(sg))).bitCount).toBe(0);
			keeper.runDueBuilds(6_000);
			const view = parseHashBody(body(keeper.getTopology(sg)));
			expect(view.version).toBe(2n);
			// Level order: roots 0 and 1 split; children 0.0, 0.1, 1.0, 1.1; 0.1 split; its children.
			expect(view.bitCount).toBe(8);
			expect(view.words[0] & 0xff).toBe(0b0000_1011);
			expect(keeper.nextBuildAt()).toBeNull();
		});
	});

	it("refuses a report that changes hashSplitN", async () => {
		await withKeeper(async (keeper, { sg }) => {
			await keeper.registerRepartition(hashSplitReport(sg, T, 0, []));
			await expect(keeper.registerRepartition(hashSplitReport(sg, { rootTreesN: 2, hashSplitN: 3 }, 1, []))).rejects.toThrow(
				/never change/,
			);
		});
	});

	it("starts a new epoch at a reset, and ignores a report that completed before it", async () => {
		await withKeeper(async (keeper, { sg }) => {
			await keeper.registerRepartition(hashSplitReport(sg, T, 0, [], 1_000), 1_000);
			keeper.runDueBuilds(LATER);
			keeper.resetShardGroup(sg, 2_000);
			let view = parseHashBody(body(keeper.getTopology(sg)));
			expect([view.epoch, view.version, view.bitCount]).toEqual([1, 0n, 0]);
			await keeper.registerRepartition(hashSplitReport(sg, T, 0, [], 1_999), 2_100);
			expect(keeper.nextBuildAt()).toBeNull();
			// The new table can also use other topology values.
			await keeper.registerRepartition(hashSplitReport(sg, { rootTreesN: 1, hashSplitN: 3 }, 0, [], 2_000), 2_100);
			keeper.runDueBuilds(LATER);
			view = parseHashBody(body(keeper.getTopology(sg)));
			expect([view.epoch, view.version, view.bitCount]).toEqual([1, 1n, 4]);
		});
	});

	it("answers the older body until the next build, and 304 for the current ETag", async () => {
		await withKeeper(async (keeper, { sg }) => {
			await keeper.registerRepartition(hashSplitReport(sg, T, 0, []), 0);
			keeper.runDueBuilds(LATER);
			const first = keeper.getTopology(sg);
			expect(first.etag).toBe('"0.1"');
			expect(keeper.getTopology(sg, '"0.1"').status).toBe(304);
			await keeper.registerRepartition(hashSplitReport(sg, T, 1, []), 0);
			expect(keeper.getTopology(sg).etag).toBe('"0.1"');
			expect(keeper.nextBuildAt()).not.toBeNull();
			keeper.runDueBuilds(LATER);
			expect(keeper.getTopology(sg, '"0.1"').etag).toBe('"0.2"');
		});
	});

	it("builds the same hash body after each batch of reports as one full build of all of them", async () => {
		const T3 = { rootTreesN: 3, hashSplitN: 2 };
		const arb = fc.record({
			picks: fc.array(fc.tuple(fc.nat(), fc.boolean(), fc.nat()), { maxLength: 40 }),
			promoted: fc.array(fc.tuple(fc.nat({ max: 30 }), fc.nat()), { maxLength: 8 }),
			builds: fc.array(fc.nat(), { maxLength: 5 }),
		});
		await withKeeper(async (_keeper, { newKeeper }) => {
			await fc.assert(
				fc.asyncProperty(arb, async ({ picks, promoted, builds }) => {
					// Grow a forest, and keep the report of each split with `keep`, in the order of `rank`.
					const leaves: Array<{ root: number; path: number[] }> = [0, 1, 2].map((root) => ({ root, path: [] }));
					const reports: Array<{ rank: number; report: (sg: string) => unknown }> = [];
					for (const [pick, keep, rank] of picks) {
						const [leaf] = leaves.splice(pick % leaves.length, 1);
						leaves.push(...[0, 1].map((c) => ({ root: leaf.root, path: [...leaf.path, c] })));
						if (keep) {
							reports.push({ rank, report: (sg) => hashSplitReport(sg, T3, leaf.root, leaf.path, 0) });
						}
					}
					for (const [key, rank] of promoted) {
						reports.push({ rank, report: (sg) => promotionReport(sg, k(`p${key}`), 0, 3, 2) });
					}
					reports.sort((a, b) => a.rank - b.rank);
					const keeper = newKeeper();
					const stepwise = `sg_${crypto.randomUUID()}`;
					const once = `sg_${crypto.randomUUID()}`;
					const buildAt = new Set(builds.map((b) => b % (reports.length + 1)));
					for (const [i, { report }] of reports.entries()) {
						if (buildAt.has(i)) {
							keeper.runDueBuilds(LATER);
						}
						await keeper.registerRepartition(report(stepwise), 0);
					}
					// The other shard group gets the same reports in the same order, and one build.
					for (const { report } of reports) {
						await keeper.registerRepartition(report(once), 0);
					}
					keeper.runDueBuilds(LATER);
					expect(keeper.getTopology(stepwise)).toEqual(keeper.getTopology(once));
				}),
				{ numRuns: 40 },
			);
		});
	});

	it("puts the promoted keys in the Bloom filter of the hash body", async () => {
		await withKeeper(async (keeper, { sg }) => {
			await keeper.registerRepartition(promotionReport(sg, KeyCodec.encode("u42"), 0, 2, 2));
			keeper.runDueBuilds(LATER);
			const view = parseHashBody(body(keeper.getTopology(sg)));
			expect(maybePromoted(view, KeyCodec.encode("u42"))).toBe(true);
			expect(maybePromoted(view, KeyCodec.encode("u43"))).toBe(false);
		});
	});
});

const k = (s: string) => KeyCodec.encode(s);
const HK = k("u42");

function rangeOf(view: RangeTopologyView, target: RangeTarget | null) {
	if (!target) {
		return null;
	}
	const edge = (p: number) => {
		const b = rangeBoundaryAt(view, p);
		return b === null ? null : (KeyCodec.decode(b) as string);
	};
	return [edge(target.start), edge(target.end), target.rank];
}

function interval(lo: string, hi: string) {
	return { lower: { value: k(lo), inclusive: true }, upper: { value: k(hi), inclusive: false } };
}

describe("topology keeper — range build", () => {
	it("builds the example: levels, depths, and the lookups", async () => {
		await withKeeper(async (keeper, { sg, storage }) => {
			await keeper.registerRepartition(promotionReport(sg, HK));
			await keeper.registerRepartition(rangeSplitReport(sg, HK, null, null, [k("g"), k("p")]));
			await keeper.registerRepartition(rangeSplitReport(sg, HK, k("g"), k("p"), [k("k")]));
			keeper.runDueBuilds(LATER);
			// B is a target of the first split and the source of the second, and is stored once.
			expect(storage.sql.exec("SELECT COUNT(*) AS n FROM fokos_topo_range_intervals").one().n).toBe(6);
			const view = parseRangeBody(body(keeper.getRangeTopology(sg, HK)));
			expect([...view.levels]).toEqual([0, 1, 0]);
			expect([...view.depths]).toEqual([1, 2, 2, 1]);
			const none = new Set<number>();
			expect(rangeOf(view, rangePointLookup(view, k("h"), none))).toEqual(["g", "k", 2]);
			expect(rangeOf(view, rangePointLookup(view, k("k"), none))).toEqual(["k", "p", 2]);
			expect(rangeOf(view, rangeIntervalLookup(view, interval("h", "m"), none))).toEqual(["g", "p", 1]);
			expect(rangeOf(view, rangeIntervalLookup(view, interval("a", "h"), none))).toEqual([null, null, 0]);
			// The fallback of a miss at [g, k) is its parent B.
			const target = rangePointLookup(view, k("h"), none)!;
			expect(rangeOf(view, rangePointLookup(view, k("h"), new Set([rangeCutKey(view, target)])))).toEqual(["g", "p", 1]);
		});
	});

	it("keeps the range root when only an inner split is known, and when the promotion report is missing", async () => {
		await withKeeper(async (keeper, { sg }) => {
			await keeper.registerRepartition(rangeSplitReport(sg, HK, k("g"), k("p"), [k("k")]));
			keeper.runDueBuilds(LATER);
			const view = parseRangeBody(body(keeper.getRangeTopology(sg, HK)));
			expect([...view.levels]).toEqual([0, 1, 0]);
			expect([...view.depths]).toEqual([0, 2, 2, 0]);
			expect(rangeOf(view, rangePointLookup(view, k("a"), new Set()))).toEqual([null, null, 0]);
			expect(rangeOf(view, rangePointLookup(view, k("h"), new Set()))).toEqual(["g", "k", 2]);
		});
	});

	it("stores the range root only when two intervals cross, and an empty body for a key that is not promoted", async () => {
		await withKeeper(async (keeper, { sg }) => {
			await keeper.registerRepartition(rangeSplitReport(sg, HK, null, null, [k("g")]));
			await keeper.registerRepartition(rangeSplitReport(sg, HK, null, k("m"), [k("h")]));
			const error = console.error;
			console.error = () => {};
			try {
				keeper.runDueBuilds(LATER);
			} finally {
				console.error = error;
			}
			const view = parseRangeBody(body(keeper.getRangeTopology(sg, HK)));
			expect([view.n, [...view.depths]]).toEqual([0, [0]]);
			const empty = parseRangeBody(body(keeper.getRangeTopology(sg, k("other"))));
			expect(empty.depths.length).toBe(0);
		});
	});

	it("builds in bounded steps, the hash body only for its facts, and a range body only for a changed key", async () => {
		await withKeeper(
			async (keeper, { sg, storage }) => {
				const dirty = () => storage.sql.exec("SELECT COUNT(*) AS n FROM fokos_topo_range_dirty").one().n;
				const rangeEtag = (key: KeyBytes) => keeper.getRangeTopology(sg, key).etag;
				const keys = [k("u1"), k("u2"), k("u3")];
				for (const key of keys) {
					await keeper.registerRepartition(promotionReport(sg, key));
				}
				expect(rangeEtag(keys[0])).toBe('"0.0"');
				// The first step builds the hash body before any range body, and then stops at the budget.
				keeper.runDueBuilds(LATER);
				expect(dirty()).toBe(3);
				const hashEtag = keeper.getTopology(sg).etag;
				expect(hashEtag).toBe('"0.3"');
				expect(keeper.nextBuildAt()).toBe(LATER);
				// A range split of a promoted key: a new version, the key is dirty, and the hash body stays.
				await keeper.registerRepartition(rangeSplitReport(sg, keys[0], null, null, [k("m")]));
				// Another report of a known fact writes nothing new, so the version stays at 4.
				await keeper.registerRepartition(promotionReport(sg, keys[1]));
				for (let step = 0; step < 3; step++) {
					keeper.runDueBuilds(LATER);
				}
				expect(dirty()).toBe(0);
				expect(keeper.nextBuildAt()).toBeNull();
				expect(keeper.getTopology(sg).etag).toBe(hashEtag);
				expect(keys.map(rangeEtag)).toEqual(['"0.4"', '"0.4"', '"0.4"']);
				const range = parseRangeBody(body(keeper.getRangeTopology(sg, keys[0])));
				expect([range.version, range.n]).toEqual([4n, 1]);
				// A hash split builds the hash body only.
				await keeper.registerRepartition(hashSplitReport(sg, { rootTreesN: 1, hashSplitN: 4 }, 0, []));
				expect(dirty()).toBe(0);
				keeper.runDueBuilds(LATER);
				expect(keeper.getTopology(sg).etag).toBe('"0.5"');
				expect(keys.map(rangeEtag)).toEqual(['"0.4"', '"0.4"', '"0.4"']);
				expect(keeper.nextBuildAt()).toBeNull();
				// A repeated hash split is a known fact: no version and no build.
				await keeper.registerRepartition(hashSplitReport(sg, { rootTreesN: 1, hashSplitN: 4 }, 0, []));
				expect(keeper.nextBuildAt()).toBeNull();
			},
			{ buildStepRows: 1 },
		);
	});

	it("stores a body above one chunk in chunks, and a new keeper answers it with no build", async () => {
		await withKeeper(
			async (keeper, { sg, storage, newKeeper }) => {
				const big = (i: number) => k(`b${String(i).padStart(3, "0")}${"x".repeat(25_000)}`);
				let start: KeyBytes | null = null;
				for (let i = 1; i <= 48; i++) {
					await keeper.registerRepartition(rangeSplitReport(sg, HK, start, null, [big(i)]));
					start = big(i);
				}
				keeper.runDueBuilds(LATER);
				const stored = body(keeper.getRangeTopology(sg, HK));
				expect(stored.length).toBeGreaterThan(TOPOLOGY_BODY_CHUNK_BYTES);
				expect(storage.sql.exec("SELECT COUNT(*) AS n FROM fokos_topo_bodies WHERE kind = 'range'").one().n).toBe(2);
				expect(parseRangeBody(stored).truncatedAtRank).toBeNull();
				const again = newKeeper();
				expect(again.nextBuildAt()).toBeNull();
				expect(body(again.getRangeTopology(sg, HK))).toEqual(stored);
				expect(body(again.getTopology(sg))).toEqual(body(keeper.getTopology(sg)));
			},
			{ maxRangeBodyBytes: 8 * 1024 * 1024 },
		);
	});
});

// ─── the range lookups against a search of the known intervals ──────────────

/** An interval of integer edges. 0 is ~min and 1000 is ~max. */
type Iv = { lo: number; hi: number };
const MIN = 0;
const MAX = 1000;
const ik = (n: number) => k(`k${String(n).padStart(4, "0")}`);
const edgeKey = (n: number) => (n === MIN || n === MAX ? null : ik(n));
const edgeOf = (view: RangeTopologyView, p: number) => {
	const b = rangeBoundaryAt(view, p);
	return b === null ? (p === 0 ? MIN : MAX) : Number((KeyCodec.decode(b) as string).slice(1));
};

/** The deepest known interval that holds [x, y), or the point x when y is null, with a rank of at most `maxRank`. */
function deepest(known: Array<Iv & { rank: number }>, x: number, y: number | null, maxRank: number) {
	const hits = known.filter((iv) => iv.lo <= x && (y === null ? x < iv.hi : y <= iv.hi) && iv.rank <= maxRank);
	return hits.reduce<(Iv & { rank: number }) | null>((best, iv) => (best === null || iv.rank > best.rank ? iv : best), null);
}

describe("topology keeper — range lookups (property)", () => {
	it("each point lookup, range lookup, and fallback gives the deepest known interval, with and without the size cap", async () => {
		const split = fc.tuple(fc.nat(), fc.integer({ min: 2, max: 4 }), fc.array(fc.nat(), { minLength: 3, maxLength: 3 }));
		const arb = fc.record({
			splits: fc.array(split, { maxLength: 12 }),
			increasing: fc.boolean(),
			keep: fc.array(fc.boolean(), { minLength: 12, maxLength: 12 }),
			keepPromotion: fc.boolean(),
			queries: fc.array(fc.tuple(fc.integer({ min: 1, max: 999 }), fc.integer({ min: 1, max: 999 })), { minLength: 1, maxLength: 10 }),
		});
		for (const cap of [undefined, 1_150]) {
			await withKeeper(
				async (_keeper, { newKeeper }) => {
					await fc.assert(
						fc.asyncProperty(arb, async ({ splits, increasing, keep, keepPromotion, queries }) => {
							const keeper = newKeeper();
							const sg = `sg_${crypto.randomUUID()}`;
							let leaves: Iv[] = [{ lo: MIN, hi: MAX }];
							const known: Iv[] = [];
							const reports: Promise<void>[] = [];
							splits.forEach(([pick, n, seeds], i) => {
								const leaf = increasing ? leaves.find((l) => l.hi === MAX)! : leaves[pick % leaves.length];
								const room = leaf.hi - leaf.lo - 1;
								const inner = [...new Set(seeds.slice(0, n - 1).map((s) => leaf.lo + 1 + (s % Math.max(room, 1))))]
									.filter((b) => room > 0 && b > leaf.lo && b < leaf.hi)
									.sort((a, b) => a - b);
								if (inner.length === 0) {
									return;
								}
								const edges = [leaf.lo, ...inner, leaf.hi];
								const children = edges.slice(1).map((hi, c) => ({ lo: edges[c], hi }));
								leaves = leaves.filter((l) => l !== leaf).concat(children);
								if (keep[i]) {
									known.push(leaf, ...children);
									reports.push(keeper.registerRepartition(rangeSplitReport(sg, HK, edgeKey(leaf.lo), edgeKey(leaf.hi), inner.map(ik))));
								}
							});
							await Promise.all(reports);
							if (keepPromotion || known.length > 0) {
								known.push({ lo: MIN, hi: MAX });
							}
							if (keepPromotion) {
								await keeper.registerRepartition(promotionReport(sg, HK));
							}
							keeper.runDueBuilds(LATER);
							const unique = [...new Map(known.map((iv) => [`${iv.lo}-${iv.hi}`, iv])).values()];
							const ranked = unique.map((iv) => ({
								...iv,
								rank: unique.filter((o) => o !== iv && o.lo <= iv.lo && iv.hi <= o.hi).length,
							}));
							const view = parseRangeBody(body(keeper.getRangeTopology(sg, HK)));
							const maxRank = view.truncatedAtRank ?? Number.POSITIVE_INFINITY;
							const asIv = (t: RangeTarget | null) => t && { lo: edgeOf(view, t.start), hi: edgeOf(view, t.end), rank: t.rank };
							const pick = (iv: (Iv & { rank: number }) | null) => iv && { lo: iv.lo, hi: iv.hi, rank: iv.rank };
							for (const [a, b] of queries) {
								const point = rangePointLookup(view, ik(a), new Set());
								expect(asIv(point)).toEqual(pick(deepest(ranked, a, null, maxRank)));
								if (point) {
									const parent = rangePointLookup(view, ik(a), new Set([rangeCutKey(view, point)]));
									expect(asIv(parent)).toEqual(pick(deepest(ranked, a, null, point.rank - 1)));
								}
								if (a !== b) {
									const [x, y] = a < b ? [a, b] : [b, a];
									expect(
										asIv(
											rangeIntervalLookup(view, interval(`k${String(x).padStart(4, "0")}`, `k${String(y).padStart(4, "0")}`), new Set()),
										),
									).toEqual(pick(deepest(ranked, x, y, maxRank)));
								}
							}
						}),
						{ numRuns: 40 },
					);
				},
				cap === undefined ? {} : { maxRangeBodyBytes: cap },
			);
		}
	});
});

describe("topology entrypoint", () => {
	const admin = (read: () => Promise<FokosTopologyRead>) => () => ({ getTopology: read, getRangeTopology: read });
	const ok = async (): Promise<FokosTopologyRead> => ({ status: 200, etag: '"1.4"', body: new Uint8Array([1, 2, 3]) });
	const opts = { admin: admin(ok), maxAgeS: 10, staleWhileRevalidateS: 600 };
	const sg = new TextEncoder().encode("fokos.p.t").toBase64({ alphabet: "base64url", omitPadding: true });

	it("answers the body with the cache headers, and 304 for a matching If-None-Match", async () => {
		const res = await fokosTopologyEntrypointFetch(new Request(`http://topology/internal/v1/topology.Get?sg=${sg}`), opts);
		expect(res.status).toBe(200);
		expect(res.headers.get("Cache-Control")).toBe("public, max-age=10, stale-while-revalidate=600");
		expect(res.headers.get("ETag")).toBe('"1.4"');
		expect(new Uint8Array(await res.arrayBuffer())).toEqual(new Uint8Array([1, 2, 3]));
		let seen: string | undefined;
		const conditional = await fokosTopologyEntrypointFetch(
			new Request(`http://topology/internal/v1/topology.GetForRange?sg=${sg}&hk=dTQy`, { headers: { "If-None-Match": '"1.4"' } }),
			{
				...opts,
				admin: () => ({
					getTopology: ok,
					getRangeTopology: async (_sg, hk, ifNoneMatch) => {
						seen = `${new TextDecoder().decode(hk)} ${ifNoneMatch}`;
						return { status: 304, etag: '"1.4"' };
					},
				}),
			},
		);
		expect(conditional.status).toBe(304);
		expect(seen).toBe('u42 "1.4"');
	});

	it("answers no-store for another path, another method, a bad query, and an admin that fails", async () => {
		const error = console.error;
		console.error = () => {};
		try {
			const cases: Array<[Request, typeof opts, number]> = [
				[new Request("http://topology/other"), opts, 404],
				[new Request(`http://topology/internal/v1/topology.Get?sg=${sg}`, { method: "POST" }), opts, 405],
				[new Request("http://topology/internal/v1/topology.Get?sg=%%%"), opts, 400],
				[
					new Request(`http://topology/internal/v1/topology.Get?sg=${sg}`),
					{
						...opts,
						admin: admin(async () => {
							throw new Error("down");
						}),
					},
					503,
				],
			];
			for (const [request, o, status] of cases) {
				const res = await fokosTopologyEntrypointFetch(request, o);
				expect(res.status).toBe(status);
				expect(res.headers.get("Cache-Control")).toBe("no-store");
			}
		} finally {
			console.error = error;
		}
	});
});
