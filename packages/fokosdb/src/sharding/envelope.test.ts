import { describe, expect, it } from "vitest";
import { FokosInternalError, FokosError } from "../shared/errors.js";
import { SHARDING_INTERNAL_CODES } from "./errors.js";
import { attachRouting, ROUTE_EVIDENCE_MAX_BYTES, RouteCollector, routedError, routeNodeBytes } from "./envelope.js";
import { KeyCodec } from "./key-codec.js";
import type { FokosRouteNode, FokosRouting, FokosServedRole } from "./runtime-types.js";

function node(name: string, role: FokosServedRole = "executed", hashDepth = 0): FokosRouteNode {
	return { ref: { partitionId: `00${name}`, doName: `t.h.${name}` }, actorId: `actor-${name}`, hashDepth, rangeDepth: 0, role };
}

function routingOf(servedBy: FokosRouteNode[], forwardCount = 0): FokosRouting {
	return { servedBy, forwardCount, servedByTruncated: false };
}

describe("RouteCollector", () => {
	it("lists a partition once, under the most informative role it took, whichever came first", () => {
		const collector = new RouteCollector();
		collector.add(node("self", "executed"));
		collector.add(node("self", "merged"));
		const routing = collector.build();
		expect(routing.forwardCount).toBe(0);
		expect(routing.servedByTruncated).toBe(false);
		expect(routing.servedBy).toEqual([node("self", "executed")]);

		// A range router that forwards first and then scans its own rows is an executor: the order the
		// two roles were taken in must not decide which one the caller reads.
		const upgraded = new RouteCollector();
		upgraded.add(node("self", "merged"));
		upgraded.add(node("self", "executed"));
		expect(upgraded.build().servedBy).toEqual([node("self", "executed")]);

		// `read_through` outranks `merged` and yields to `executed`.
		const readThrough = new RouteCollector();
		readThrough.add(node("self", "merged"));
		readThrough.add(node("self", "read_through"));
		expect(readThrough.build().servedBy).toEqual([node("self", "read_through")]);
	});

	it("puts the partition that raised an error at the head of the list, without losing a stronger role", () => {
		// A fan-out that fails merges the groups that answered before the router adds itself, so the
		// raiser must lead the list explicitly or the caller would read a group that succeeded.
		const collector = new RouteCollector();
		collector.mergeForwarded(routingOf([node("groupA"), node("groupB")], 0));
		collector.addRaiser(node("self", "merged"));
		expect(collector.build().servedBy.map((n) => [n.ref.doName, n.role])).toEqual([
			["t.h.self", "merged"],
			["t.h.groupA", "executed"],
			["t.h.groupB", "executed"],
		]);

		// The raiser already ran the handler, so its stronger role stands and it still leads.
		const executed = new RouteCollector();
		executed.add(node("self", "executed"));
		executed.mergeForwarded(routingOf([node("groupA")], 0));
		executed.addRaiser(node("self", "merged"));
		expect(executed.build().servedBy.map((n) => [n.ref.doName, n.role])).toEqual([
			["t.h.self", "executed"],
			["t.h.groupA", "executed"],
		]);
	});

	it("reports an empty list until a partition is named, so an error can name its raiser", () => {
		const collector = new RouteCollector();
		expect(collector.isEmpty).toBe(true);
		collector.add(node("self", "executed"));
		expect(collector.isEmpty).toBe(false);
	});

	it("merges a forwarded envelope by partition, and counts the RPC plus the child's own forwards", () => {
		const leaf = node("leaf", "executed", 2);
		const other = node("other", "executed", 2);
		const collector = new RouteCollector();
		collector.add(node("self", "merged"));
		collector.mergeForwarded(routingOf([leaf, other], 1));
		collector.mergeForwarded(routingOf([leaf], 0));
		const routing = collector.build();
		// Two RPCs, and the first child had forwarded once itself.
		expect(routing.forwardCount).toBe(3);
		expect(routing.servedBy.map((n) => [n.ref.doName, n.role])).toEqual([
			["t.h.self", "merged"],
			["t.h.leaf", "executed"],
			["t.h.other", "executed"],
		]);
	});

	it("applies a stamp to every merged node and keeps the rest of the node", () => {
		const collector = new RouteCollector();
		const range: FokosRouteNode = {
			ref: { partitionId: "01aa", doName: "t.r.k.~min.~max" },
			actorId: "range",
			hashDepth: 0,
			rangeDepth: 0,
			role: "executed",
			_rangeAncestors: [],
		};
		collector.mergeForwarded(routingOf([range]), (n) => ({ ...n, hashDepth: 1 }));
		const [entry] = collector.build().servedBy;
		expect(entry).toEqual({ ...range, hashDepth: 1 });
	});

	it("drops the nodes that cross the byte cap, keeps the earlier ones, and never drops the count", () => {
		const collector = new RouteCollector();
		const boundary = KeyCodec.encode("k".repeat(2000));
		let nodes = 0;
		for (let i = 0; i < 20; i++) {
			const leaf = { ...node(`leaf${i}`, "executed", 1), _rangeAncestors: [{ depth: 1, startBoundary: boundary, endBoundary: boundary }] };
			collector.mergeForwarded(routingOf([leaf]));
			nodes++;
		}
		const routing = collector.build();
		expect(routing.servedByTruncated).toBe(true);
		expect(routing.servedBy.length).toBeLessThan(nodes);
		expect(routing.servedBy.length).toBeGreaterThan(0);
		expect(routing.servedBy.reduce((sum, n) => sum + routeNodeBytes(n), 0)).toBeLessThanOrEqual(ROUTE_EVIDENCE_MAX_BYTES);
		expect(routing.forwardCount).toBe(nodes);
	});

	it("keeps the truncation flag of a child envelope", () => {
		const collector = new RouteCollector();
		collector.mergeForwarded({ ...routingOf([node("leaf")]), servedByTruncated: true });
		expect(collector.build().servedByTruncated).toBe(true);
	});
});

describe("routing on an error", () => {
	it("is an own data property that the guards read back", () => {
		const err = new FokosInternalError(SHARDING_INTERNAL_CODES.partition_fanout_failed, { message: "x" });
		expect(routedError(err)).toBeUndefined();
		const collector = new RouteCollector();
		collector.add(node("self"));
		const routing = collector.build();
		attachRouting(err, routing);
		expect(routedError(err)?.routing).toBe(routing);
		expect(Object.hasOwn(err, "routing")).toBe(true);
		// A hop keeps own properties, so the same shape survives a wire round trip.
		expect(routedError(FokosError.fromWire(err))?.routing).toEqual(routing);
	});
});
