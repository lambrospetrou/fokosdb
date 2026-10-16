import { describe, expect, it } from "vitest";
import { isFokosDBError, type FokosDB } from "../../src/client/db.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import { FokosError, TRANSACTION_PENDING_CODES } from "../../src/shared/errors.js";
import type { ConditionExpression } from "../../src/shared/types.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { TestPartition } from "../partition-do/partition-harness.js";
import {
	controlledCoordinator,
	controlledPartition,
	coordinatorRouter,
	keysAcrossPartitions,
	keysInOnePartition,
	makeDB,
	owningPartition,
	partitionEntryOf,
	type Key,
} from "./tx-helpers.js";

/** A condition that fails on an absent item. */
const EXISTS: ConditionExpression = { op: "exists", args: [{ ref: "hashKey" }] };

/** The entry of the root coordinator of `token`. The coordinator group never splits in these tests. */
function coordinatorEntryOf(db: FokosDB, token: string) {
	return {
		servedByActorId: expect.stringMatching(/^[0-9a-f]{64}$/),
		servedByActorName: coordinatorRouter(db).rootContext(KeyCodec.encode(token)).doName,
		hashDepth: 0,
	};
}

async function cancelOf(write: Promise<unknown>): Promise<FokosTransactionCancelledError> {
	const err = await write.then(
		() => null,
		(e: unknown) => e,
	);
	if (!FokosTransactionCancelledError.is(err)) {
		throw new Error(`expected a cancelled transaction, got ${String(err)}`);
	}
	return err;
}

describe("transactions - meta on the coordinator path", () => {
	it("names each participant and the coordinator on a commit, and a replay names no participant", async () => {
		const db = makeDB({ controlled: true });
		const keys = keysAcrossPartitions(db, 2, "meta-commit");
		const items = keys.map((key) => ({ ...key, operation: "put" as const, data: "v" }));
		const token = `meta-commit-${crypto.randomUUID()}`;

		const first = await db.transactWriteItems({ items, clientRequestToken: token });

		expect(first.meta.partitions).toHaveLength(2);
		expect(first.meta.partitions).toEqual(expect.arrayContaining(keys.map((key) => partitionEntryOf(db, key))));
		expect(first.meta.txCoordinator).toEqual(coordinatorEntryOf(db, token));
		expect(first.meta.forwardCount).toBe(0);
		// A replay reads the stored outcome and sends no participant RPC.
		const replay = await db.transactWriteItems({ items, clientRequestToken: token });
		expect(replay.meta).toEqual({ forwardCount: 0, partitions: [], txCoordinator: coordinatorEntryOf(db, token) });
	});

	it("names the partition of each rejected operation of a cancelled transaction", async () => {
		const db = makeDB({ controlled: true });
		const [missing, other] = keysAcrossPartitions(db, 2, "meta-cancel");
		const token = `meta-cancel-${crypto.randomUUID()}`;

		const err = await cancelOf(
			db.transactWriteItems({
				clientRequestToken: token,
				items: [
					{ ...missing, operation: "check", condition: EXISTS },
					{ ...other, operation: "put", data: "v" },
				],
			}),
		);

		const [rejected, passed] = err.results;
		expect(passed).toEqual({ outcome: "passed" });
		expect(rejected.outcome === "rejected" && err.meta.partitions[rejected.partitionIndex!]).toEqual(partitionEntryOf(db, missing));
		expect(err.meta.partitions).toEqual(expect.arrayContaining([partitionEntryOf(db, other)]));
		expect(err.meta.txCoordinator).toEqual(coordinatorEntryOf(db, token));
		// A replay answers from the stored results. It got no answer that rejected an operation.
		const replay = await cancelOf(
			db.transactWriteItems({
				clientRequestToken: token,
				items: [
					{ ...missing, operation: "check", condition: EXISTS },
					{ ...other, operation: "put", data: "v" },
				],
			}),
		);
		expect(replay.results).toEqual(err.results.map((r) => ({ ...r, partitionIndex: undefined })));
		expect(replay.results[0]).not.toHaveProperty("partitionIndex");
		expect(replay.meta).toEqual({ forwardCount: 0, partitions: [], txCoordinator: coordinatorEntryOf(db, token) });
	});

	it("names the coordinator and the participants that answered on transaction_commit_pending", async () => {
		const db = makeDB({ controlled: true });
		const keys = keysAcrossPartitions(db, 2, "meta-pending");
		const items = keys.map((key) => ({ ...key, operation: "put" as const, data: "v" }));
		const token = `meta-pending-${crypto.randomUUID()}`;
		const coordinator = controlledCoordinator(db);
		await coordinator.testConfig({ fanoutRequestBudgetMs: 250 });
		const partition = controlledPartition(db, keys[1]);
		await partition.testTxResponse("txCommit", { error: "simulated participant outage" });

		const err = await db.transactWriteItems({ items, clientRequestToken: token }).then(
			() => null,
			(e: unknown) => e,
		);

		expect(FokosError.isCode(err, TRANSACTION_PENDING_CODES.transaction_commit_pending)).toBe(true);
		if (!isFokosDBError(err)) {
			throw new Error("expected a FokosDBError");
		}
		// Both participants answered the prepare. One did not answer the commit.
		expect(err.meta?.txCoordinator).toEqual(coordinatorEntryOf(db, token));
		expect(err.meta?.partitions).toHaveLength(2);
		expect(err.meta?.partitions).toEqual(expect.arrayContaining(keys.map((key) => partitionEntryOf(db, key))));
		expect(err).not.toHaveProperty("routing");

		await partition.testClearTxResponse("txCommit");
		await coordinator.testConfig({});
		// The retry sends only the commit that is missing.
		const committed = await db.transactWriteItems({ items, clientRequestToken: token });
		expect(committed.meta.partitions).toEqual([partitionEntryOf(db, keys[1])]);
	});
});

describe("transactions - meta on the fast path", () => {
	it("names the one partition and no coordinator", async () => {
		const db = makeDB({ controlled: true });
		const [missing, other] = keysInOnePartition(db, 2, "meta-fast");

		const committed = await db.transactWriteItems({ items: [{ ...other, operation: "put", data: "v" }] });
		expect(committed.meta).toEqual({ forwardCount: 0, partitions: [partitionEntryOf(db, other)] });

		const err = await cancelOf(
			db.transactWriteItems({
				items: [
					{ ...missing, operation: "check", condition: EXISTS },
					{ ...other, operation: "put", data: "w" },
				],
			}),
		);
		expect(err.meta).toEqual({ forwardCount: 0, partitions: [partitionEntryOf(db, missing)] });
		expect(err.results[0]).toMatchObject({ outcome: "rejected", partitionIndex: 0 });
		expect(err.results[1]).not.toHaveProperty("partitionIndex");
	});
});

describe("transactions - meta of a participant that fans out", () => {
	it("lists each owner below a split participant, with the index of each rejected operation and item", async () => {
		const db = makeDB({ rootTreesN: 1, maxSizeMb: 1 });
		const { pCtx, stub } = owningPartition(db, { hashKey: "any", sortKey: "sk" });
		const root = TestPartition.at(pCtx, stub);
		await root.splitHash();
		// Two keys that the two children own. The coordinator sends both to the root, which fans out.
		const keys: Key[] = [];
		const owners: string[] = [];
		for (let i = 0; keys.length < 2; i++) {
			const key = { hashKey: `meta-fanout-${i}`, sortKey: "sk" };
			const owner = (await root.childOwning(key.hashKey)).doName;
			if (!owners.includes(owner)) {
				keys.push(key);
				owners.push(owner);
			}
		}
		const token = `meta-fanout-${crypto.randomUUID()}`;
		const childEntry = (i: number) => expect.objectContaining({ servedByActorName: owners[i], hashDepth: 1 });

		const committed = await db.transactWriteItems({
			clientRequestToken: token,
			items: keys.map((key) => ({ ...key, operation: "put" as const, data: "v" })),
		});
		expect(committed.meta.partitions).toHaveLength(2);
		expect(committed.meta.partitions).toEqual(expect.arrayContaining([childEntry(0), childEntry(1)]));
		// The root forwards each phase to both children.
		expect(committed.meta.forwardCount).toBeGreaterThanOrEqual(2);

		const cancelled = await cancelOf(
			db.transactWriteItems({
				clientRequestToken: `${token}-cancel`,
				items: [
					{ hashKey: keys[0].hashKey, sortKey: "absent", operation: "check", condition: EXISTS },
					{ ...keys[1], operation: "put", data: "w" },
				],
			}),
		);
		const [rejected, passed] = cancelled.results;
		expect(passed).toEqual({ outcome: "passed" });
		expect(rejected.outcome === "rejected" && cancelled.meta.partitions[rejected.partitionIndex!]).toEqual(childEntry(0));

		const read = await db.transactGetItems({ items: keys });
		expect(read.items.map((item) => read.meta.partitions[item.partitionIndex])).toEqual([childEntry(0), childEntry(1)]);
		expect(read.meta).not.toHaveProperty("coordinator");
	});
});
