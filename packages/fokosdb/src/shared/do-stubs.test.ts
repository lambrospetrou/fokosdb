/**
 * The namespace accessors and the stub helpers over recorded namespace doubles. A double counts
 * every call, so a test proves which binding a helper resolved and whether `jurisdiction()` ran.
 */
import { describe, expect, it, vi } from "vitest";
import { PartitionContextCreator } from "./partition-context.js";
import {
	partitionNamespace,
	partitionStub,
	partitionStubByName,
	txCoordinatorNamespace,
	txCoordinatorStubByName,
	txCoordinatorStubForParticipant,
} from "./do-stubs.js";

function fakeNamespace() {
	return {
		jurisdiction: vi.fn(),
		getByName: vi.fn((name: string) => `stub-by-name:${name}`),
		get: vi.fn((id: unknown) => `stub-by-id:${String(id)}`),
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
			// No location hint is configured, so the options are undefined.
			expect(target.getByName).toHaveBeenCalledWith("p0", undefined);
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
		expect(target.get).toHaveBeenCalledWith(doId, undefined);
		for (const other of [partitionNs, coordinatorNs, partitionSub, coordinatorSub]) {
			if (other !== target) expect(other.get).not.toHaveBeenCalled();
		}
	});

	it.each([undefined, "eu" as const])(
		"txCoordinatorStubByName gets the stub on the resolved nsTx namespace only (jurisdiction: %s)",
		(jurisdiction) => {
			const { env, ctx, partitionNs, coordinatorNs, partitionSub, coordinatorSub } = makeEnv(jurisdiction);
			const target = jurisdiction === undefined ? coordinatorNs : coordinatorSub;

			expect(txCoordinatorStubByName(env, ctx, "tc0")).toBe("stub-by-name:tc0");
			expect(target.getByName).toHaveBeenCalledTimes(1);
			expect(target.getByName).toHaveBeenCalledWith("tc0", undefined);
			for (const other of [partitionNs, coordinatorNs, partitionSub, coordinatorSub]) {
				if (other !== target) expect(other.getByName).not.toHaveBeenCalled();
			}
		},
	);

	it.each([undefined, "eu" as const])(
		"txCoordinatorStubForParticipant gets the stub on the nsTx namespace of the address only, with no options (jurisdiction: %s)",
		(jurisdiction) => {
			const { env, partitionNs, coordinatorNs, partitionSub, coordinatorSub } = makeEnv();
			const target = jurisdiction === undefined ? coordinatorNs : coordinatorSub;

			expect(txCoordinatorStubForParticipant(env, { nsTx: "TRANSACTION_COORDINATOR_DO", jurisdiction }, "tc0")).toBe("stub-by-name:tc0");
			expect(target.getByName).toHaveBeenCalledTimes(1);
			expect(target.getByName).toHaveBeenCalledWith("tc0");
			if (jurisdiction !== undefined) expect(coordinatorNs.jurisdiction).toHaveBeenCalledWith(jurisdiction);
			for (const other of [partitionNs, coordinatorNs, partitionSub, coordinatorSub]) {
				if (other !== target) expect(other.getByName).not.toHaveBeenCalled();
			}
		},
	);

	it("stub helpers pass locationHint to get and getByName when configured", () => {
		const { env, ctx, partitionNs, coordinatorNs } = makeEnv(undefined, "weur");

		expect(partitionStubByName(env, ctx, "p0")).toBe("stub-by-name:p0");
		expect(partitionNs.getByName).toHaveBeenCalledWith("p0", { locationHint: "weur" });

		expect(partitionStub(env, ctx, doId)).toBe(`stub-by-id:${String(doId)}`);
		expect(partitionNs.get).toHaveBeenCalledWith(doId, { locationHint: "weur" });

		expect(txCoordinatorStubByName(env, ctx, "tc0")).toBe("stub-by-name:tc0");
		expect(coordinatorNs.getByName).toHaveBeenCalledWith("tc0", { locationHint: "weur" });
	});
});
