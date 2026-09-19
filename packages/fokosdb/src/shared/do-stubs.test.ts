/**
 * The namespace accessors and the stub helpers over recorded namespace doubles. A double counts
 * every call, so a test proves which binding a helper resolved and whether `jurisdiction()` ran.
 */
import { describe, expect, it, vi } from "vitest";
import { PartitionContextCreator, type PartitionContext } from "./partition-topology/partition-context.js";
import { partitionNamespace, partitionStub, partitionStubByName, txCoordinatorNamespace, txCoordinatorStub } from "./do-stubs.js";

function fakeNamespace() {
	return {
		jurisdiction: vi.fn(),
		getByName: vi.fn((name: string) => `stub-by-name:${name}`),
		get: vi.fn((id: unknown) => `stub-by-id:${String(id)}`),
		idFromString: vi.fn((id: string) => `id-from-string:${id}`),
	};
}

function makeEnv(jurisdiction?: DurableObjectJurisdiction, locationHint?: DurableObjectLocationHint) {
	const partitionNs = fakeNamespace();
	const coordinatorNs = fakeNamespace();
	const partitionSub = fakeNamespace();
	const coordinatorSub = fakeNamespace();
	partitionNs.jurisdiction.mockReturnValue(partitionSub);
	coordinatorNs.jurisdiction.mockReturnValue(coordinatorSub);
	const env = { PARTITION_DO: partitionNs, TRANSACTION_COORDINATOR_DO: coordinatorNs } as unknown as Env;
	const ctx = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: "do-stubs-test",
		rootTreesN: 1,
		hashSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
		...(jurisdiction === undefined ? {} : { jurisdiction }),
		...(locationHint === undefined ? {} : { locationHint }),
	});
	return { env, ctx, partitionNs, coordinatorNs, partitionSub, coordinatorSub };
}

// An object, not a string: a string means the stringified form of an id, which idFromString maps first.
const doId = { toString: () => "a-do-id" } as unknown as DurableObjectId;

describe("partitionNamespace", () => {
	it("returns the plain binding and never calls jurisdiction() for a context that selects none", () => {
		const { env, ctx, partitionNs } = makeEnv();

		expect(partitionNamespace(env, ctx)).toBe(partitionNs);
		expect(partitionNs.jurisdiction).not.toHaveBeenCalled();
	});

	it("calls jurisdiction() once with the context value and returns the subnamespace", () => {
		const { env, ctx, partitionNs, partitionSub } = makeEnv("eu");

		expect(partitionNamespace(env, ctx)).toBe(partitionSub);
		expect(partitionNs.jurisdiction).toHaveBeenCalledTimes(1);
		expect(partitionNs.jurisdiction).toHaveBeenCalledWith("eu");
	});
});

describe("txCoordinatorNamespace", () => {
	it("resolves the nsTx binding and not the ns binding, for a context that selects none", () => {
		const { env, ctx, partitionNs, coordinatorNs } = makeEnv();

		expect(txCoordinatorNamespace(env, ctx)).toBe(coordinatorNs);
		expect(coordinatorNs.jurisdiction).not.toHaveBeenCalled();
		expect(partitionNs.jurisdiction).not.toHaveBeenCalled();
	});

	it("applies the jurisdiction to the nsTx binding and not to the ns binding", () => {
		const { env, ctx, partitionNs, coordinatorNs, coordinatorSub } = makeEnv("fedramp");

		expect(txCoordinatorNamespace(env, ctx)).toBe(coordinatorSub);
		expect(coordinatorNs.jurisdiction).toHaveBeenCalledTimes(1);
		expect(coordinatorNs.jurisdiction).toHaveBeenCalledWith("fedramp");
		expect(partitionNs.jurisdiction).not.toHaveBeenCalled();
	});
});

describe("stub helpers", () => {
	it.each([undefined, "eu" as const])(
		"partitionStubByName gets the stub on the resolved namespace only (jurisdiction: %s)",
		(jurisdiction) => {
			const { env, ctx, partitionNs, coordinatorNs, partitionSub, coordinatorSub } = makeEnv(jurisdiction);
			const target = jurisdiction === undefined ? partitionNs : partitionSub;

			expect(partitionStubByName(env, ctx, "p0")).toBe("stub-by-name:p0");
			expect(target.getByName).toHaveBeenCalledTimes(1);
			expect(target.getByName).toHaveBeenCalledWith("p0");
			for (const other of [partitionNs, coordinatorNs, partitionSub, coordinatorSub]) {
				if (other !== target) expect(other.getByName).not.toHaveBeenCalled();
			}
		},
	);

	it.each([undefined, "eu" as const])("partitionStub gets the stub on the resolved namespace only (jurisdiction: %s)", (jurisdiction) => {
		const { env, ctx, partitionNs, coordinatorNs, partitionSub, coordinatorSub } = makeEnv(jurisdiction);
		const target = jurisdiction === undefined ? partitionNs : partitionSub;

		expect(partitionStub(env, ctx, doId)).toBe(`stub-by-id:${String(doId)}`);
		expect(target.get).toHaveBeenCalledTimes(1);
		expect(target.get).toHaveBeenCalledWith(doId);
		for (const other of [partitionNs, coordinatorNs, partitionSub, coordinatorSub]) {
			if (other !== target) expect(other.get).not.toHaveBeenCalled();
		}
	});

	it.each([undefined, "eu" as const])(
		"txCoordinatorStub gets the stub on the resolved nsTx namespace only (jurisdiction: %s)",
		(jurisdiction) => {
			const { env, ctx, partitionNs, coordinatorNs, partitionSub, coordinatorSub } = makeEnv(jurisdiction);
			const target = jurisdiction === undefined ? coordinatorNs : coordinatorSub;

			expect(txCoordinatorStub(env, ctx, doId)).toBe(`stub-by-id:${String(doId)}`);
			expect(target.get).toHaveBeenCalledTimes(1);
			expect(target.get).toHaveBeenCalledWith(doId);
			expect(target.idFromString).not.toHaveBeenCalled();
			for (const other of [partitionNs, coordinatorNs, partitionSub, coordinatorSub]) {
				if (other !== target) expect(other.get).not.toHaveBeenCalled();
			}
		},
	);

	it("txCoordinatorStub maps a string through idFromString on the resolved namespace and then calls get", () => {
		const { env, ctx, coordinatorNs } = makeEnv();

		expect(txCoordinatorStub(env, ctx, "abc")).toBe("stub-by-id:id-from-string:abc");
		expect(coordinatorNs.idFromString).toHaveBeenCalledTimes(1);
		expect(coordinatorNs.idFromString).toHaveBeenCalledWith("abc");
		expect(coordinatorNs.get).toHaveBeenCalledTimes(1);
		expect(coordinatorNs.get).toHaveBeenCalledWith("id-from-string:abc");
	});

	it("stub helpers pass locationHint to get and getByName when configured", () => {
		const { env, ctx, partitionNs, coordinatorNs } = makeEnv(undefined, "weur");

		expect(partitionStubByName(env, ctx, "p0")).toBe("stub-by-name:p0");
		expect(partitionNs.getByName).toHaveBeenCalledWith("p0", { locationHint: "weur" });

		expect(partitionStub(env, ctx, doId)).toBe(`stub-by-id:${String(doId)}`);
		expect(partitionNs.get).toHaveBeenCalledWith(doId, { locationHint: "weur" });

		expect(txCoordinatorStub(env, ctx, doId)).toBe(`stub-by-id:${String(doId)}`);
		expect(coordinatorNs.get).toHaveBeenCalledWith(doId, { locationHint: "weur" });
	});
});
