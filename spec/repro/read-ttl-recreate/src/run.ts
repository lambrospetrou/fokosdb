import {
	FokosConflictError,
	FokosDB,
	FokosTransactionCancelledError,
	type MaybeReadItem,
	type TransactGetItemsResult,
} from "fokosdb/client";

type Key = { hashKey: string; sortKey: string };

export type ReproOutcome =
	| { status: "read_conflict"; table: string; keys: [Key, Key] }
	| { status: "counterexample"; table: string; keys: [Key, Key]; items: [MaybeReadItem, MaybeReadItem] };

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function makeDb(): FokosDB {
	return new FokosDB({
		table: {
			name: `ttl-repro.${crypto.randomUUID()}`,
			// The generated subclass namespace is invariant, but ReproPartitionDO extends PartitionDO.
			ns: "REPRO_PARTITION_DO" as never,
			nsTx: "TRANSACTION_COORDINATOR_DO",
			rootTreesN: 4,
			hashSplitN: 2,
			coordinatorRootsN: 1,
		},
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
		singlePartitionFastPath: false,
	});
}

async function keysAcrossPartitions(db: FokosDB): Promise<[[Key, string], [Key, string]]> {
	const names = new Map<string, Key>();
	for (let i = 0; i < 20 && names.size < 2; i++) {
		const key = { hashKey: `ttl-repro-${i}`, sortKey: "sk" };
		const result = await db.getItem(key);
		names.set(result.meta.servedByActorName, key);
	}
	if (names.size !== 2) throw new Error("the keys did not reach two partitions");
	return [...names].map(([name, key]) => [key, name]) as [[Key, string], [Key, string]];
}

async function waitFor(label: string, ready: () => Promise<boolean> | boolean, timeoutMs: number): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!(await ready())) {
		if (Date.now() >= deadline) throw new Error(`timed out: ${label}`);
		await sleep(100);
	}
}

async function expectData(db: FokosDB, key: Key, value: string): Promise<void> {
	const result = await db.getItem(key);
	if (!result.found || result.item.data !== value) {
		throw new Error(`expected ${key.hashKey} to contain ${value}`);
	}
}

async function commitRecreation(db: FokosDB, a: Key, b: Key): Promise<void> {
	for (let attempt = 0; attempt < 10; attempt++) {
		try {
			await db.transactWriteItems({
				items: [
					{ ...a, operation: "put", data: "new-a" },
					{ ...b, operation: "put", data: "new-b" },
				],
			});
			return;
		} catch (error) {
			if (
				!FokosTransactionCancelledError.is(error) ||
				!error.results.some((item) => item.outcome === "rejected" && item.reason.code === "timestamp_conflict")
			) {
				throw error;
			}
			await sleep(Math.min(25 * 2 ** attempt, 250));
		}
	}
	throw new Error("the two-partition write did not commit");
}

async function runSchedule(db: FokosDB, env: Env): Promise<ReproOutcome> {
	const [[a, nameA], [b, nameB]] = await keysAcrossPartitions(db);
	const keys: [Key, Key] = [a, b];
	const partitionA = env.REPRO_PARTITION_DO.getByName(nameA);
	const partitionB = env.REPRO_PARTITION_DO.getByName(nameB);

	await db.putItem({ ...b, data: "old-b" });
	const ttlAt = Math.floor(Date.now() / 1000) + 10;
	await db.putItem({ ...a, data: "old-a", ttlAt });
	await partitionA.holdRead("after");
	await partitionB.holdRead("before");

	try {
		const read = db.transactGetItems({ items: keys });
		void read.catch(() => {});

		await waitFor("A sampled phase 1", () => partitionA.readParked("after"), 5_000);
		await waitFor("B arrived at phase 1", () => partitionB.readParked("before"), 5_000);
		if (Date.now() >= ttlAt * 1000) throw new Error("A sampled its first read after the TTL expired");
		await expectData(db, a, "old-a");
		await partitionA.releaseRead("after");

		await waitFor("TTL expired", () => Math.floor(Date.now() / 1000) > ttlAt, 20_000);
		await waitFor("the TTL timer deleted A", async () => !(await db.getItem(a)).found, 15_000);
		await commitRecreation(db, a, b);
		await expectData(db, a, "new-a");
		await expectData(db, b, "new-b");
		await partitionB.releaseRead("before");

		let result: TransactGetItemsResult;
		try {
			result = await read;
		} catch (error) {
			if (FokosConflictError.is(error) && error.code === "read_conflict" && error.attributes.hashKey === a.hashKey) {
				return { status: "read_conflict", table: db.options().table.name, keys };
			}
			throw error;
		}

		const [readA, readB] = result.items;
		if (!readA.found || readA.data !== "old-a" || !readB.found || readB.data !== "new-b") {
			throw new Error(`unexpected read result: ${JSON.stringify(result.items)}`);
		}
		return { status: "counterexample", table: db.options().table.name, keys, items: [readA, readB] };
	} finally {
		await Promise.allSettled([partitionA.releaseRead("after"), partitionB.releaseRead("before")]);
	}
}

export async function runRepro(env: Env): Promise<ReproOutcome> {
	const db = makeDb();
	try {
		return await runSchedule(db, env);
	} finally {
		try {
			await db.destroy();
		} catch (error) {
			console.error("the repro table was not destroyed", error);
		}
	}
}
