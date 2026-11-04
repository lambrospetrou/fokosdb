import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import type { FokosDBAutoAdminDO } from "../../src/server/do-auto-admin.js";
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
	isPromotedRoutingHash,
	parseHashBody,
	parseRangeBody,
	rangeBoundaryAt,
	rangeCutKey,
	rangeIntervalLookup,
	rangePointLookup,
	type RangeTarget,
	type RangeTopologyView,
} from "../../src/sharding/topology-codec.js";
import { routingHashPromotionReport, hashSplitReport, promotionReport, rangeSplitReport } from "./topology-events.js";
import { asRoutingHash, compareRoutingHashes, routingHashOf } from "../../src/sharding/hash-primitives.js";
import type { FokosCompositeKey } from "../../src/sharding/key-codec.js";
import { INIT_RETRY_ALARM_MS } from "../../src/sharding/scheduler.js";
import { fokosErrorWith, invariantFailure } from "../errors-matchers.js";
import { promotedSectionRoutingHashes } from "../../src/sharding/topology-encode.js";

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
			expect(scheduled).toEqual([6_000, 6_000, 6_000]);
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

	it.each(["same report", "same fact"])("restores a pending build after a schedule failure and a retry of the %s", async (retry) => {
		await withKeeper(async (_keeper, { storage, sg }) => {
			const scheduled: number[] = [];
			const newKeeper = () =>
				new FokosTopologyKeeper(storage, {
					buildDelayMs: 1_000,
					scheduleBuild: async (at) => {
						scheduled.push(at);
						if (scheduled.length === 1) {
							throw new Error("schedule failed");
						}
					},
				});
			const first = newKeeper();
			const event = hashSplitReport(sg, T, 0, [], 1_000);
			await expect(first.registerRepartition(event, 5_000)).rejects.toThrow("schedule failed");
			expect(first.nextBuildAt()).toBe(6_000);
			const restarted = newKeeper();
			const repeated = retry === "same report" ? event : hashSplitReport(sg, T, 0, [], 1_000);
			await restarted.registerRepartition(repeated, 5_500);
			expect(scheduled).toEqual([6_000, 6_000]);
			expect(restarted.nextBuildAt()).toBe(6_000);
			restarted.runDueBuilds(6_000);
			expect(parseHashBody(body(restarted.getTopology(sg))).version).toBe(1n);
			expect(restarted.nextBuildAt()).toBeNull();
			await restarted.registerRepartition(repeated, 6_500);
			expect(scheduled).toEqual([6_000, 6_000]);
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

	it("ignores a report from before the first of two resets, as a destroy that resets again after a lost answer does", async () => {
		await withKeeper(async (keeper, { sg }) => {
			await keeper.registerRepartition(rangeSplitReport(sg, HK, null, null, [c("g")], 1_000), 1_000);
			keeper.runDueBuilds(LATER);
			keeper.resetShardGroup(sg, 2_000);
			keeper.resetShardGroup(sg, 3_000);
			for (const completedAt of [1_000, 1_500, 2_500]) {
				await keeper.registerRepartition(rangeSplitReport(sg, HK, null, null, [c("g")], completedAt), 3_100);
			}
			expect(keeper.nextBuildAt()).toBeNull();
			expect(parseHashBody(body(keeper.getTopology(sg))).epoch).toBe(2);
			expect(parseRangeBody(rangeBody(keeper, sg)).depths.length).toBe(0);
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
						// Each shard group has its own seed, so both groups get the routingHash of one fixed group.
						const routingHash = routingHashOf(k(`p${key}`), "fixed");
						reports.push({ rank, report: (sg) => routingHashPromotionReport(sg, routingHash, 0, 3, 2) });
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

	it("puts the promoted routingHashes in the promoted list of the hash body", async () => {
		await withKeeper(async (keeper, { sg }) => {
			await keeper.registerRepartition(promotionReport(sg, KeyCodec.encode("u42"), 0, 2, 2));
			keeper.runDueBuilds(LATER);
			const view = parseHashBody(body(keeper.getTopology(sg)));
			expect(isPromotedRoutingHash(view, routingHashOf(KeyCodec.encode("u42"), sg))).toBe("exact");
			expect(isPromotedRoutingHash(view, routingHashOf(KeyCodec.encode("u43"), sg))).toBe(false);
		});
	});

	it("merges the reports of later builds into the promoted list, equal to a full build", async () => {
		await withKeeper(async (keeper, { sg }) => {
			const routingHashes = Array.from({ length: 30 }, () => asRoutingHash(crypto.getRandomValues(new Uint8Array(8))));
			// Register the routingHashes in three unordered batches, with a build after each one.
			for (const batch of [routingHashes.slice(20), routingHashes.slice(0, 10), routingHashes.slice(10, 20)]) {
				for (const routingHash of batch) {
					await keeper.registerRepartition(routingHashPromotionReport(sg, routingHash, 0, 2, 2));
				}
				keeper.runDueBuilds(LATER);
			}
			// A repeated report adds nothing.
			await keeper.registerRepartition(routingHashPromotionReport(sg, routingHashes[0], 0, 2, 2));
			keeper.runDueBuilds(LATER);
			const view = parseHashBody(body(keeper.getTopology(sg)));
			expect(view.bloom).toBeNull();
			expect([...promotedSectionRoutingHashes(view.promoted!)]).toEqual([...routingHashes].sort(compareRoutingHashes));
		});
	});

	it("moves the promoted set into a Bloom filter once it is above promotedListMax", async () => {
		await withKeeper(
			async (keeper, { sg, storage }) => {
				const routingHashes = Array.from({ length: 5 }, () => asRoutingHash(crypto.getRandomValues(new Uint8Array(8))));
				for (const routingHash of routingHashes.slice(0, 4)) {
					await keeper.registerRepartition(routingHashPromotionReport(sg, routingHash, 0, 2, 2));
				}
				keeper.runDueBuilds(LATER);
				let view = parseHashBody(body(keeper.getTopology(sg)));
				expect([view.promoted?.count, view.bloom === null]).toEqual([4, true]);
				// The build after the fifth report takes the first four roots from the stored list, and
				// reads only the new root. The test deletes the stored root rows to show that the build
				// does not read them.
				storage.sql.exec(`DELETE FROM fokos_topo_range_intervals WHERE shard_group = ?1`, sg);
				await keeper.registerRepartition(routingHashPromotionReport(sg, routingHashes[4], 0, 2, 2));
				keeper.runDueBuilds(LATER);
				view = parseHashBody(body(keeper.getTopology(sg)));
				expect([view.promoted, view.bloom === null]).toEqual([null, false]);
				for (const routingHash of routingHashes) {
					expect(isPromotedRoutingHash(view, routingHash)).toBe("maybe");
				}
			},
			{ promotedListMax: 4 },
		);
	});

	it.each([-1, Number.NaN, 1.5])("refuses promotedListMax %s", async (promotedListMax) => {
		const stub = env.TOPOLOGY_ADMIN_DO.getByName(`keeper-${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: FokosDBAutoAdminDO, state) => {
			expect(() => new FokosTopologyKeeper(state.storage, { promotedListMax, scheduleBuild: () => {} })).toThrow(
				invariantFailure(/promotedListMax must be an integer of at least 0/),
			);
		});
	});
});

describe("topology keeper — build failure", () => {
	it("defers only the group whose build failed, at the error retry", async () => {
		await withKeeper(
			async (keeper, { sg, storage, newKeeper }) => {
				const badSg = `bad_${sg}`;
				await keeper.registerRepartition(hashSplitReport(badSg, T, 0, [], 1_000), 5_000);
				keeper.runDueBuilds(LATER);
				// The stored hash body is gone while `built_version` stays: the next build fails when it
				// reads the body it must merge. `newKeeper` has no cached body, so it takes that path.
				storage.sql.exec(`DELETE FROM fokos_topo_bodies WHERE shard_group = ?1`, badSg);
				const again = newKeeper();
				await again.registerRepartition(hashSplitReport(badSg, T, 1, [], 1_000), 5_100);
				await again.registerRepartition(hashSplitReport(sg, T, 0, [], 1_000), 5_200);
				const errors: unknown[] = [];
				const error = console.error;
				console.error = (e: unknown) => void errors.push(e);
				try {
					again.runDueBuilds(10_000);
				} finally {
					console.error = error;
				}
				expect(errors).toHaveLength(1);
				expect(errors[0]).toMatchObject({ shardGroup: badSg });
				const dueAt = (group: string) =>
					storage.sql.exec(`SELECT build_due_at AS at FROM fokos_topo_groups WHERE shard_group = ?1`, group).one().at;
				expect(dueAt(badSg)).toBe(17_000);
				expect(dueAt(sg)).toBeNull();
				expect(body(again.getTopology(sg)).length).toBeGreaterThan(0);
				expect(again.nextBuildAt()).toBe(17_000);
			},
			{ buildErrorRetryMs: 7_000 },
		);
	});

	it("leaves the alarm armed at the error retry when a build fails in the alarm", async () => {
		const stub = env.TOPOLOGY_ADMIN_DO.getByName(`keeper-${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (instance: FokosDBAutoAdminDO, state) => {
			const keeper = new FokosTopologyKeeper(state.storage, { buildDelayMs: 0, scheduleBuild: () => {} });
			keeper.runMigrations();
			const badSg = `sg_${crypto.randomUUID()}`;
			await keeper.registerRepartition(hashSplitReport(badSg, T, 0, [], 1_000), 5_000);
			keeper.runDueBuilds(LATER);
			state.storage.sql.exec(`DELETE FROM fokos_topo_bodies WHERE shard_group = ?1`, badSg);
			const again = new FokosTopologyKeeper(state.storage, { scheduleBuild: () => {} });
			await again.registerRepartition(hashSplitReport(badSg, T, 1, [], 1_000), 5_100);
			await instance.alarm();
			const at = await state.storage.getAlarm();
			expect(at).toBeGreaterThan(Date.now());
			expect(at).toBeLessThanOrEqual(Date.now() + 60_000);
		});
	});

	it("arms the error retry when the build pass itself fails", async () => {
		const stub = env.TOPOLOGY_ADMIN_DO.getByName(`keeper-${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (instance: FokosDBAutoAdminDO, state) => {
			state.storage.sql.exec(`DROP TABLE fokos_topo_groups`);
			await instance.alarm();
			expect(await state.storage.getAlarm()).toBeGreaterThan(Date.now());
		});
	});
});

const k = (s: string) => KeyCodec.encode(s);
const HK = k("u42");
/** The composite key of the sort key `s` of `HK`. */
const c = (s: string): FokosCompositeKey => ({ hk: HK, sk: k(s) });
/** The range body of the routingHash of `hashKey`. */
const rangeBody = (keeper: FokosTopologyKeeper, sg: string, hashKey: KeyBytes = HK) =>
	body(keeper.getRangeTopology(sg, routingHashOf(hashKey, sg)));

function rangeOf(view: RangeTopologyView, target: RangeTarget | null) {
	if (!target) {
		return null;
	}
	const edge = (p: number) => {
		const b = rangeBoundaryAt(view, p);
		return b === null ? null : (KeyCodec.decode(b.sk) as string);
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
			await keeper.registerRepartition(rangeSplitReport(sg, HK, null, null, [c("g"), c("p")]));
			await keeper.registerRepartition(rangeSplitReport(sg, HK, c("g"), c("p"), [c("k")]));
			keeper.runDueBuilds(LATER);
			// B is a target of the first split and the source of the second, and is stored once.
			expect(storage.sql.exec("SELECT COUNT(*) AS n FROM fokos_topo_range_intervals").one().n).toBe(6);
			const view = parseRangeBody(rangeBody(keeper, sg));
			expect([...view.levels]).toEqual([0, 1, 0]);
			expect([...view.depths]).toEqual([1, 2, 2, 1]);
			const none = new Set<number>();
			expect(rangeOf(view, rangePointLookup(view, c("h"), none))).toEqual(["g", "k", 2]);
			expect(rangeOf(view, rangePointLookup(view, c("k"), none))).toEqual(["k", "p", 2]);
			expect(rangeOf(view, rangeIntervalLookup(view, HK, interval("h", "m"), none))).toEqual(["g", "p", 1]);
			expect(rangeOf(view, rangeIntervalLookup(view, HK, interval("a", "h"), none))).toEqual([null, null, 0]);
			// The fallback of a miss at [g, k) is its parent B.
			const target = rangePointLookup(view, c("h"), none)!;
			expect(rangeOf(view, rangePointLookup(view, c("h"), new Set([rangeCutKey(view, target)])))).toEqual(["g", "p", 1]);
		});
	});

	it("serves the range body from range reports only, and a hash body with no hash node and only that routingHash", async () => {
		// The admin of one promoted routingHash gets only its promotion and its range splits.
		await withKeeper(async (keeper, { sg, storage }) => {
			await keeper.registerRepartition(rangeSplitReport(sg, HK, null, null, [c("g")]));
			keeper.runDueBuilds(LATER);
			const view = parseRangeBody(rangeBody(keeper, sg));
			expect(rangeOf(view, rangePointLookup(view, c("h"), new Set()))).toEqual(["g", null, 1]);
			expect(storage.sql.exec("SELECT COUNT(*) AS n FROM fokos_topo_hash_nodes").one().n).toBe(0);
			const hash = parseHashBody(body(keeper.getTopology(sg)));
			expect([...promotedSectionRoutingHashes(hash.promoted!)]).toEqual([routingHashOf(HK, sg)]);
		});
	});

	it("keeps the range root when only an inner split is known, and when the promotion report is missing", async () => {
		await withKeeper(async (keeper, { sg }) => {
			await keeper.registerRepartition(rangeSplitReport(sg, HK, c("g"), c("p"), [c("k")]));
			keeper.runDueBuilds(LATER);
			const view = parseRangeBody(rangeBody(keeper, sg));
			expect([...view.levels]).toEqual([0, 1, 0]);
			expect([...view.depths]).toEqual([0, 2, 2, 0]);
			expect(rangeOf(view, rangePointLookup(view, c("a"), new Set()))).toEqual([null, null, 0]);
			expect(rangeOf(view, rangePointLookup(view, c("h"), new Set()))).toEqual(["g", "k", 2]);
		});
	});

	it("stores the range root only when two intervals cross, and an empty body for a key that is not promoted", async () => {
		await withKeeper(async (keeper, { sg }) => {
			await keeper.registerRepartition(rangeSplitReport(sg, HK, null, null, [c("g")]));
			await keeper.registerRepartition(rangeSplitReport(sg, HK, null, c("m"), [c("h")]));
			const error = console.error;
			console.error = () => {};
			try {
				keeper.runDueBuilds(LATER);
			} finally {
				console.error = error;
			}
			const view = parseRangeBody(rangeBody(keeper, sg));
			expect([view.n, [...view.depths]]).toEqual([0, [0]]);
			const empty = parseRangeBody(rangeBody(keeper, sg, k("other")));
			expect(empty.depths.length).toBe(0);
		});
	});

	it("builds in bounded steps, the hash body only for its facts, and a range body only for a changed key", async () => {
		await withKeeper(
			async (keeper, { sg, storage }) => {
				const dirty = () => storage.sql.exec("SELECT COUNT(*) AS n FROM fokos_topo_range_dirty").one().n;
				const rangeEtag = (key: KeyBytes) => keeper.getRangeTopology(sg, routingHashOf(key, sg)).etag;
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
				await keeper.registerRepartition(rangeSplitReport(sg, keys[0], null, null, [{ hk: keys[0], sk: k("m") }]));
				// Another report of a known fact writes nothing new, so the version stays at 4.
				await keeper.registerRepartition(promotionReport(sg, keys[1]));
				for (let step = 0; step < 3; step++) {
					keeper.runDueBuilds(LATER);
				}
				expect(dirty()).toBe(0);
				expect(keeper.nextBuildAt()).toBeNull();
				expect(keeper.getTopology(sg).etag).toBe(hashEtag);
				expect(keys.map(rangeEtag)).toEqual(['"0.4"', '"0.4"', '"0.4"']);
				const range = parseRangeBody(rangeBody(keeper, sg, keys[0]));
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
				const big = (i: number) => c(`b${String(i).padStart(3, "0")}${"x".repeat(25_000)}`);
				let start: FokosCompositeKey | null = null;
				for (let i = 1; i <= 48; i++) {
					await keeper.registerRepartition(rangeSplitReport(sg, HK, start, null, [big(i)]));
					start = big(i);
				}
				keeper.runDueBuilds(LATER);
				const stored = rangeBody(keeper, sg);
				expect(stored.length).toBeGreaterThan(TOPOLOGY_BODY_CHUNK_BYTES);
				expect(storage.sql.exec("SELECT COUNT(*) AS n FROM fokos_topo_bodies WHERE kind = 'range'").one().n).toBe(2);
				expect(parseRangeBody(stored).truncatedAtRank).toBeNull();
				const again = newKeeper();
				expect(again.nextBuildAt()).toBeNull();
				expect(rangeBody(again, sg)).toEqual(stored);
				expect(body(again.getTopology(sg))).toEqual(body(keeper.getTopology(sg)));
			},
			{ maxRangeBodyBytes: 8 * 1024 * 1024 },
		);
	});
});

// ─── the range lookups against a search of the known intervals ──────────────

/**
 * An interval of integer edges. 0 is ~min and 1000 is ~max. The integers map to composite keys of two
 * hash keys of one routingHash, in the same order: below `SPLIT` a sort key of `HA`, at `SPLIT` the
 * empty sort key of `HB`, and above `SPLIT` a sort key of `HB`. Thus a tree has boundaries inside each
 * hash key and between the two hash keys.
 */
type Iv = { lo: number; hi: number };
const MIN = 0;
const MAX = 1000;
const SPLIT = 500;
const HA = k("a");
const HB = k("b");
const sk = (n: number) => (n === SPLIT ? KeyCodec.encodeOptional(undefined) : k(`k${String(n).padStart(4, "0")}`));
const ik = (n: number): FokosCompositeKey => ({ hk: n < SPLIT ? HA : HB, sk: sk(n) });
const edgeKey = (n: number) => (n === MIN || n === MAX ? null : ik(n));
const edgeOf = (view: RangeTopologyView, p: number) => {
	const b = rangeBoundaryAt(view, p);
	if (b === null) {
		return p === 0 ? MIN : MAX;
	}
	return b.sk.length === 0 ? SPLIT : Number((KeyCodec.decode(b.sk) as string).slice(1));
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
							const view = parseRangeBody(rangeBody(keeper, sg));
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
								// A query with no upper bound covers the sort keys of one hash key up to its end.
								const hashKey = ik(a).hk;
								const lower = { value: sk(a), inclusive: true };
								expect(asIv(rangeIntervalLookup(view, hashKey, { lower }, new Set()))).toEqual(
									pick(deepest(ranked, a, a < SPLIT ? SPLIT : MAX, maxRank)),
								);
								const [x, y] = a < b ? [a, b] : [b, a];
								if (x !== y && (y < SPLIT || x >= SPLIT)) {
									const upper = { value: sk(y), inclusive: false };
									expect(asIv(rangeIntervalLookup(view, ik(x).hk, { lower: { value: sk(x), inclusive: true }, upper }, new Set()))).toEqual(
										pick(deepest(ranked, x, y, maxRank)),
									);
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
			new Request(`http://topology/internal/v1/topology.GetForRange?sg=${sg}&rh=0123456789abcdef`, {
				headers: { "If-None-Match": '"1.4"' },
			}),
			{
				...opts,
				admin: () => ({
					getTopology: ok,
					getRangeTopology: async (_sg, routingHash, ifNoneMatch) => {
						seen = `${routingHash.toHex()} ${ifNoneMatch}`;
						return { status: 304, etag: '"1.4"' };
					},
				}),
			},
		);
		expect(conditional.status).toBe(304);
		expect(seen).toBe('0123456789abcdef "1.4"');
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

describe("topology keeper — failed initialization", () => {
	it("answers partition_init_failed when the schema is broken, and the alarm re-arms at the init interval", async () => {
		const name = `keeper-${crypto.randomUUID()}`;
		// The next construction re-runs the migrations, and one needs the column that is gone. The
		// constructor catches the failure instead of rejecting the object. The writes and the abort run
		// in separate calls: an abort rolls back what its own call wrote, so the poison must land first.
		await runInDurableObject(env.TOPOLOGY_ADMIN_DO.getByName(name), (_i: FokosDBAutoAdminDO, state) => {
			state.storage.sql.exec(`DROP INDEX idx_fokos_topo_hash_nodes_seq`);
			state.storage.sql.exec(`ALTER TABLE fokos_topo_hash_nodes RENAME COLUMN seq TO seq_broken`);
			state.storage.kv.delete("__fokos/topo_sql_schema_version");
		});
		await runInDurableObject(env.TOPOLOGY_ADMIN_DO.getByName(name), (_i: FokosDBAutoAdminDO, state) => {
			state.abort();
		}).catch(() => {});

		await runInDurableObject(env.TOPOLOGY_ADMIN_DO.getByName(name), async (instance: FokosDBAutoAdminDO, state) => {
			await expect(instance.getTopology("sg_any")).rejects.toThrow(fokosErrorWith("partition_init_failed"));
			await instance.alarm();
			const alarmAt = await state.storage.getAlarm();
			expect(alarmAt).toBeGreaterThan(Date.now() + INIT_RETRY_ALARM_MS - 60_000);
			expect(alarmAt).toBeLessThanOrEqual(Date.now() + INIT_RETRY_ALARM_MS);
		});
	});
});
