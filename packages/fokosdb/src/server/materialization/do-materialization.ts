import { DurableObject } from "cloudflare:workers";
import type { FokosDBPolicy, FokosDBRouteContext } from "../../shared/partition-context.js";
import { FokosShardingRuntime } from "../../sharding/runtime.js";
import type { FokosRuntimeConfigOverrides } from "../../sharding/runtime-config.js";
import type { FokosEnvelope, FokosLocalCall, FokosOperations, FokosShardingHooks } from "../../sharding/runtime-types.js";
import type {
	FokosExecuteLocalRequest,
	FokosInitRequest,
	FokosMigrationAckRequest,
	FokosMigrationPage,
	FokosMigrationPullRequest,
	FokosPrepareDestroyRequest,
	FokosPromotionsRequest,
	FokosRequestPromotionRequest,
	FokosShardingRpc,
	FokosStartImportRequest,
	FokosStatusRequest,
	MigrationHost,
} from "../../sharding/repartition-types.js";
import { identityDepth } from "../../sharding/partition-id.js";
import { materializationStubByName } from "../../shared/do-stubs.js";
import { CONFLICT_CODES, FokosConflictError, FokosUnavailableError, UNAVAILABLE_CODES } from "../../shared/errors.js";
import invariant from "../../shared/invariant.js";
import { classifyBatchFailure } from "../../shared/batch-failure.js";
import { mergeBatchParts, withOffset, type BatchFailure } from "../../shared/batch-merge.js";
import { ADMISSION_MARGIN } from "../../shared/transaction-limits.js";
import type { WirePartitionMeta } from "../../shared/types.js";
import { MaterializationStore, type LatestImage } from "./materialization-store.js";

export type { LatestImage } from "./materialization-store.js";

/** An ancestor that a split child names, so that each receiver rejects the late calls of that ancestor. */
export type AncestorFence = {
	/** The `DurableObjectId` of the ancestor, as `DurableObjectId.toString()` gives it. */
	ancestorId: string;
	minEpoch: number;
};

/** One export call of a base partition. Its items are the latest images of the changed base items. */
export type ApplyLatestImagesRequest = {
	/** The `DurableObjectId` of the sending base partition, as `DurableObjectId.toString()` gives it. */
	producerId: string;
	/** The export epoch of the producer. A receiver that stored a higher epoch applies nothing. */
	epoch: number;
	ancestorFences: readonly AncestorFence[];
	items: LatestImage[];
};

/**
 * The answer for each item, by its `opIndex`. An acknowledged item needs no new export at the revision
 * that the call carried: the receiver applied it now, applied it before, or holds a newer revision. A
 * failed item applied nothing, except an item that is `unknown`.
 */
export type ApplyLatestImagesResponse = {
	acknowledged: Array<{ opIndex: number; partitionIndex: number }>;
	failed: BatchFailure[];
	partitions: WirePartitionMeta[];
};

export type MaterializationOps = {
	applyLatestImages: { req: ApplyLatestImagesRequest; res: ApplyLatestImagesResponse };
};

type MaterializationRpc = FokosShardingRpc & {
	[K in keyof MaterializationOps]: (
		ctx: FokosDBRouteContext,
		req: MaterializationOps[K]["req"],
	) => Promise<FokosEnvelope<MaterializationOps[K]["res"]>>;
};

/**
 * This class does not split: `evaluateSplit` never starts a repartition and nothing asks it for a
 * promotion, so the runtime never calls its migration host.
 */
const NO_MIGRATION: MigrationHost = {
	buildPage: () => noSplit(),
	applyPage: () => noSplit(),
	validatePage: () => noSplit(),
};

function noSplit(): never {
	invariant(false, "fokos/mat: MaterializationDO does not split");
}

/**
 * The latest image of each changed base item of a table, in the shard group `fokos.m.<tableName>`. It
 * routes by the base key `(hashKey, sortKey)`, so a split of the base table does not change the owner
 * of an item here. Each base partition exports its changes with `applyLatestImages`.
 */
export class MaterializationDO extends DurableObject<Env> implements MaterializationRpc {
	/** The sharding runtime: identity, routing, and the alarm. */
	readonly fokos: FokosShardingRuntime<FokosDBPolicy, MaterializationOps>;
	#store: MaterializationStore;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#store = new MaterializationStore(ctx.storage);
		// The runtime runs the sharding migrations in its own blockConcurrencyWhile, before the host's.
		this.fokos = new FokosShardingRuntime<FokosDBPolicy, MaterializationOps>({
			ctx,
			stub: (routeCtx, doName) => materializationStubByName(env, routeCtx, doName),
			hooks: this.hooks(),
			operations: this.operations(),
			config: () => this.fokosRuntimeConfig(),
		});
		void ctx.blockConcurrencyWhile(async () => this.#store.runMigrations());
	}

	// ═══ the RPC surface: one dispatch per method ════════════════════════════

	applyLatestImages(ctx: FokosDBRouteContext, req: ApplyLatestImagesRequest): Promise<FokosEnvelope<ApplyLatestImagesResponse>> {
		return this.fokos.dispatch("applyLatestImages", ctx, req);
	}

	fokosInit(req: FokosInitRequest): Promise<void> {
		return this.fokos.fokosInit(req);
	}
	fokosStartImport(req: FokosStartImportRequest): Promise<void> {
		return this.fokos.fokosStartImport(req);
	}
	fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage> {
		return this.fokos.fokosMigrationPull(req);
	}
	fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void> {
		return this.fokos.fokosMigrationAck(req);
	}
	fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<FokosEnvelope<unknown>> {
		return this.fokos.fokosExecuteLocal(req);
	}
	fokosRequestPromotion(req: FokosRequestPromotionRequest) {
		return this.fokos.fokosRequestPromotion(req);
	}
	fokosStatus(req: FokosStatusRequest) {
		return this.fokos.fokosStatus(req);
	}
	fokosPromotions(req: FokosPromotionsRequest) {
		return this.fokos.fokosPromotions(req);
	}
	fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		return this.fokos.fokosPrepareDestroy(req);
	}
	fokosDestroy(): Promise<void> {
		return this.fokos.fokosDestroy();
	}

	async alarm(info: AlarmInvocationInfo): Promise<void> {
		await this.fokos.alarm(info);
	}

	// ═══ operations and hooks ════════════════════════════════════════════════

	private operations(): FokosOperations<MaterializationOps> {
		return {
			applyLatestImages: {
				shape: "group",
				whileMigrating: "throw",
				admissionTag: "batch_write",
				failurePolicy: "collect",
				items: (req) => req.items.map((item) => ({ key: { hashKey: item.hashKey, sortKey: item.sortKey }, item })),
				subRequest: (req, items) => ({ ...req, items: items as LatestImage[] }),
				local: (req, call) => this.applyLatestImagesLocal(req, call),
				merge: (parts) =>
					mergeBatchParts(
						parts,
						{ write: true, self: () => this.selfPartition() },
						{ acknowledged: [], failed: [], partitions: [] },
						(out, result, offset) => {
							out.acknowledged.push(...result.acknowledged.map((entry) => withOffset(entry, offset)));
						},
					),
			},
		};
	}

	private hooks(): FokosShardingHooks<FokosDBPolicy> {
		return {
			evaluateSplit: () => false,
			migration: NO_MIGRATION,
		};
	}

	/**
	 * Applies each image of the local part in its own transaction, so one failed item does not stop
	 * the others. For each item, the receiver rules run first:
	 *
	 * 1. Each ancestor fence raises the stored epoch of its ancestor to at least `minEpoch`.
	 * 2. An epoch below the stored epoch of the producer applies nothing, and the item is `unprocessed`
	 *    with `producer_epoch_fenced`. The answer is definite.
	 * 3. A higher epoch is stored.
	 *
	 * Then an image applies only when its revision is above the stored revision. An equal or lower
	 * revision is acknowledged with no write: this partition holds that revision or a newer one.
	 *
	 * Above its size limit the partition refuses each newer live image with `partition_over_size`, and
	 * asks for a split. The receiver rules still apply first, so the fences and the epochs of the call
	 * are stored. It still applies each delete, because a delete makes the partition smaller.
	 */
	private applyLatestImagesLocal(req: ApplyLatestImagesRequest, call: FokosLocalCall): ApplyLatestImagesResponse {
		const out: ApplyLatestImagesResponse = { acknowledged: [], failed: [], partitions: [this.selfPartition()] };
		const overSize = req.items.some((item) => !item.deleted) ? this.sizeRefusal() : undefined;
		if (overSize) {
			call.signal({ evaluateSplit: true });
		}
		const fail = (error: unknown, opIndex: number) =>
			out.failed.push({ ...classifyBatchFailure(error, { write: true, local: true }), opIndexes: [opIndex], partitionIndex: 0 });
		for (const item of req.items) {
			let result: "applied" | "fenced" | "over_size";
			try {
				result = this.#store.transactionSync(() => {
					for (const fence of req.ancestorFences) {
						this.#store.raiseProducerEpoch(fence.ancestorId, fence.minEpoch);
					}
					const stored = this.#store.producerEpoch(req.producerId);
					if (req.epoch < stored) {
						return "fenced";
					}
					if (req.epoch > stored) {
						this.#store.raiseProducerEpoch(req.producerId, req.epoch);
					}
					const current = this.#store.storedImage(item.hashKey, item.sortKey);
					if (current !== undefined && item.revision <= current.revision) {
						return "applied";
					}
					if (overSize && !item.deleted) {
						return "over_size";
					}
					this.#store.putLatestImage(item, this.fokosNow(), current?.estRowBytes ?? 0);
					return "applied";
				});
			} catch (e) {
				fail(e, item.opIndex);
				continue;
			}
			if (result === "fenced") {
				fail(producerEpochFenced(req.epoch), item.opIndex);
			} else if (result === "over_size") {
				fail(overSize, item.opIndex);
			} else {
				out.acknowledged.push({ opIndex: item.opIndex, partitionIndex: 0 });
			}
		}
		out.partitions = [{ ...this.selfPartition(), databaseSize: this.#store.databaseSize }];
		return out;
	}

	/** The refusal of a live image above the admission limit of this partition, or undefined below it. */
	private sizeRefusal(): FokosUnavailableError | undefined {
		const policy = this.fokos.policy();
		const maxSizeMb = (this.fokos.identity().kind === "hash" ? policy.hashSplitConditions : policy.rangeSplitConditions)?.maxSizeMb;
		if (maxSizeMb && this.#store.databaseSize > maxSizeMb * ADMISSION_MARGIN * 1024 * 1024) {
			return new FokosUnavailableError(UNAVAILABLE_CODES.partition_over_size, {
				message: "the materialization partition exceeded its limits, please retry later",
				attributes: { operation: "applyLatestImages" },
			});
		}
		return undefined;
	}

	/** The entry of this partition in the `partitions` of an answer, with the depth that it has. */
	private selfPartition(): WirePartitionMeta {
		const identity = this.fokos.identity();
		const depth = identityDepth(identity);
		return { actorId: this.ctx.id.toString(), ...(identity.kind === "hash" ? { hashDepth: depth } : { rangeDepth: depth }) };
	}

	//////////////////////////////
	// User overridable methods.
	//////////////////////////////

	/**
	 * Override to change the settings of the sharding runtime of this class. The rules of
	 * `PartitionDO.fokosRuntimeConfig` apply.
	 */
	protected fokosRuntimeConfig(): FokosRuntimeConfigOverrides {
		return {};
	}

	/**
	 * The clock of this partition, in milliseconds. Read at each use, so a test can replace it on one
	 * instance and avoid global mocks.
	 */
	fokosNow(): number {
		return Date.now();
	}
}

function producerEpochFenced(epoch: number): FokosConflictError {
	return new FokosConflictError(CONFLICT_CODES.producer_epoch_fenced, {
		message: "the producer sent this call with an epoch below the epoch that the receiver stored",
		attributes: { epoch },
	});
}
