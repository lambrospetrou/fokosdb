import * as v from "valibot";
import { FokosDB, PartitionContextCreator, FokosRouter, type SplitConditions } from "fokosdb/client";

/** The Hono environment of the `/api` routes. */
export type AppEnv = { Bindings: Env; Variables: { dbItemMeta?: object } };

const SplitConditionsSchema = v.object({
	maxSizeMb: v.optional(v.number()),
	// maxItems: v.optional(v.number()),
});

export const PartitionOptionsSchema = v.optional(
	v.object({
		rootTreesN: v.optional(v.number()),
		hashSplitN: v.optional(v.number()),
		rangeSplitN: v.optional(v.number()),
		hashSplitConditions: v.optional(SplitConditionsSchema),
		rangeSplitConditions: v.optional(SplitConditionsSchema),
	}),
);

const DEFAULT_PARTITION_OPTIONS = {
	rootTreesN: 10,
	hashSplitN: 4,
	rangeSplitN: 4,
	hashSplitConditions: { maxSizeMb: 500 } as SplitConditions,
	rangeSplitConditions: { maxSizeMb: 500 } as SplitConditions,
};

export type PartitionOptionsInput = v.InferOutput<typeof PartitionOptionsSchema>;

export function makeFokosDB(env: Env, tableName: string, partitionOptions?: PartitionOptionsInput): FokosDB {
	const table = PartitionContextCreator.create({
		ns: "CUSTOM_PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName,
		rootTreesN: partitionOptions?.rootTreesN ?? DEFAULT_PARTITION_OPTIONS.rootTreesN,
		hashSplitN: partitionOptions?.hashSplitN ?? DEFAULT_PARTITION_OPTIONS.hashSplitN,
		rangeSplitN: partitionOptions?.rangeSplitN ?? DEFAULT_PARTITION_OPTIONS.rangeSplitN,
		hashSplitConditions: partitionOptions?.hashSplitConditions ?? DEFAULT_PARTITION_OPTIONS.hashSplitConditions,
		rangeSplitConditions: partitionOptions?.rangeSplitConditions ?? DEFAULT_PARTITION_OPTIONS.rangeSplitConditions,
	});
	return new FokosDB({
		topology: new FokosRouter(table.topology, table.rangeConfig, table.policy),
	});
}
