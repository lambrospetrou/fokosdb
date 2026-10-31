import { DurableObject } from "cloudflare:workers";
import { baseTableName, type FokosDBPolicy, type FokosDBRouteContext } from "../../shared/partition-context.js";
import { FokosShardingRuntime } from "../../sharding/runtime.js";
import type { FokosRuntimeConfigOverrides } from "../../sharding/runtime-config.js";
import type { FokosEnvelope, FokosLifecycle, FokosLocalCall, FokosOperations, FokosShardingHooks } from "../../sharding/runtime-types.js";
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
} from "../../sharding/repartition-types.js";
import { promotedKeyOf } from "../../sharding/repartition-slice.js";
import { identityDepth } from "../../sharding/partition-id.js";
import { materializationStubByName, partitionStubByName, pushTargetStub } from "../../shared/do-stubs.js";
import { resolveLimits } from "../../shared/transaction-limits.js";
import { GsiWriter } from "./gsi-writer.js";
import { checkGsiPlans, desiredGsiEntry, type GsiPlans } from "./gsi-target.js";
import { CONFLICT_CODES, FokosConflictError, FokosError, FokosUnavailableError, UNAVAILABLE_CODES } from "../../shared/errors.js";
import { classifyBatchFailure } from "../../shared/batch-failure.js";
import { mergeBatchParts, withOffset, type BatchFailure } from "../../shared/batch-merge.js";
import { ADMISSION_MARGIN } from "../../shared/transaction-limits.js";
import type { WirePartitionMeta } from "../../shared/types.js";
import type { KeyBytes } from "../../sharding/key-codec.js";
import { resolveMaterializationConfig, type MaterializationDOConfig, type MaterializationDOConfigOverrides } from "../host-config.js";
import { MaterializationStore, type LatestImage, type TargetPositions } from "./materialization-store.js";
import { MaterializationCleanup } from "./materialization-cleanup.js";
import { MaterializationMigrationHost } from "./materialization-migration-host.js";
import { maxSplitBytes, SplitPlanner } from "../../shared/partition/split-planner.js";
import { TargetRunner } from "./target-runner.js";
import { validateTargets, type FokosTargetDefinition } from "./targets.js";

export type { LatestImage } from "./materialization-store.js";
export type {
	FokosTargetDefinition,
	GsiTargetDefinition,
	PushSinkTargetDefinition,
	PushTargetBatch,
	PushTargetItem,
	PushTargetResult,
} from "./targets.js";

/** The host job that starts the target runner again after a restart, an eviction, a backoff, or a probe time. */
const JOB_TARGET_PUSH = "target_push";

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
 * The latest image of each changed base item of a table, in the shard group `fokos.m.<tableName>.<fp(ns)>`. It
 * routes by the base key `(hashKey, sortKey)`, so a split of the base table does not change the owner
 * of an item here. Each base partition exports its changes with `applyLatestImages`.
 *
 * Each accepted image gets a change position, and the target runner sends the latest image of each
 * changed item to each target of `fokosTargets()`.
 *
 * It splits with the rules of `FokosDBPartitionDO`: a hash split, a promotion of a large base hash key, and a
 * range split of a promoted key. The migration moves the items with their positions and their
 * `target_retry` rows, and the split fences of each target.
 */
export class FokosDBMaterializationDO extends DurableObject<Env> implements MaterializationRpc {
	/** The sharding runtime: identity, routing, and the alarm. */
	readonly fokos: FokosShardingRuntime<FokosDBPolicy, MaterializationOps>;
	#store: MaterializationStore;
	#runner: TargetRunner;
	#cleanup: MaterializationCleanup;
	#splitPlanner: SplitPlanner;
	#gsiWriter: GsiWriter;
	/**
	 * The checked plans of each GSI target, by `targetId`. A target whose check failed has no entry, so
	 * the next step checks it again.
	 */
	readonly #gsiPlans = new Map<string, GsiPlans>();
	/** The stored target list, read one time for each instance. */
	#targetList: readonly FokosTargetDefinition[] | undefined;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#store = new MaterializationStore(ctx.storage);
		this.#splitPlanner = new SplitPlanner({
			keySizes: this.#store.keySizes,
			source: {
				probeHashKeys: () => this.#store.probeHashKeys(),
				sortKeySizes: (hashKey, start, end) => this.#store.sortKeySizes(hashKey, start, end),
			},
			databaseSize: () => this.#store.databaseSize,
			promotionFraction: () => this.config().promotionFraction,
			rangeSplitN: () => this.fokos.routeContext().rangeConfig.rangeSplitN,
			logName: "fokos/mat",
			logParams: () => this.logParams(),
		});
		this.#cleanup = new MaterializationCleanup({
			store: this.#store,
			config: () => this.config().cleanup,
		});
		this.#gsiWriter = new GsiWriter({
			table: () => {
				const { topology, rangeConfig, policy, policyVersion } = this.fokos.routeContext();
				return { topology, rangeConfig, policy, policyVersion };
			},
			stub: (routeCtx, doName) => partitionStubByName(env, routeCtx, doName),
		});
		this.#runner = new TargetRunner({
			store: this.#store,
			cleanup: this.#cleanup,
			targets: () => this.#targets(),
			canRun: () => this.#canRunLocally(this.fokos.lifecycle()),
			ownerCheck: () => {
				const owns = this.fokos.ownerCheck();
				return (row) => owns({ hashKey: row.hk, sortKey: row.sk });
			},
			tableName: () => baseTableName(this.fokos.identity().topology),
			producerId: () => this.ctx.id.toString(),
			checkPushTarget: (target) => {
				pushTargetStub(this.env, target);
			},
			push: async (target, batch) => await pushTargetStub(this.env, target)[target.method](batch),
			config: () => this.config().targetRunner,
			scheduleRetry: (runAt) => {
				this.fokos.scheduleJob(JOB_TARGET_PUSH, runAt).catch((error: unknown) => {
					console.error({ ...this.logParams(), message: "fokos/mat: failed to schedule the target runner", error: String(error) });
				});
			},
			writeGsi: (target, request) => this.#gsiWriter.write(target, request),
			gsiEvaluator: (target) => {
				const plans = this.#gsiPlans.get(target.targetId) ?? checkGsiPlans(this.#store, target);
				this.#gsiPlans.set(target.targetId, plans);
				const limits = resolveLimits(this.fokos.policy().limits);
				return (image) => desiredGsiEntry(this.#store, plans, image, limits);
			},
			now: () => this.fokosNow(),
			logParams: () => this.logParams(),
		});
		// The runtime runs the sharding migrations in its own blockConcurrencyWhile, before the host's.
		this.fokos = new FokosShardingRuntime<FokosDBPolicy, MaterializationOps>({
			ctx,
			stub: (routeCtx, doName) => materializationStubByName(env, routeCtx, doName),
			hooks: this.hooks(),
			operations: this.operations(),
			config: () => this.fokosRuntimeConfig(),
		});
		void ctx.blockConcurrencyWhile(async () => {
			this.#store.runMigrations();
			this.#checkStoredTargets();
		});
	}

	/**
	 * Validates the stored target list one time for each instance, checks the plans of each GSI
	 * target, and checks that the Worker env has the binding of each push target. A failure logs one
	 * error for each target, and changes nothing: the partition keeps the stored list, and the target
	 * runner stops only a GSI target whose plans fail. It never throws, because a throw in
	 * `blockConcurrencyWhile` resets the Durable Object.
	 */
	#checkStoredTargets(): void {
		try {
			const stored = this.#store.storedTargets() ?? [];
			let valid = true;
			for (const target of stored) {
				try {
					validateTargets([target]);
					if (target.kind === "gsi") {
						this.#gsiPlans.set(target.targetId, checkGsiPlans(this.#store, target));
					} else {
						pushTargetStub(this.env, target);
					}
				} catch (error) {
					valid = false;
					console.error({
						...this.logParams(),
						message: "fokos/mat: a stored target is not valid",
						targetId: target.targetId,
						kind: target.kind,
						...errorFields(error),
					});
				}
			}
			if (valid) {
				validateTargets(stored);
			}
		} catch (error) {
			console.error({ ...this.logParams(), message: "fokos/mat: the stored target list is not valid", ...errorFields(error) });
		}
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
	async fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		await this.fokos.fokosPrepareDestroy(req);
		// The fence stops the target runner. No call of this instance to a target stays in flight after it.
		await this.#runner.settled();
	}
	fokosDestroy(): Promise<void> {
		return this.fokos.fokosDestroy();
	}
	/** The `targetId` of each GSI target of the stored target list, or null when this partition has no list. */
	fokosGsiTargetIds(): string[] | null {
		return this.#store.storedTargets()?.flatMap((target) => (target.kind === "gsi" ? [target.targetId] : [])) ?? null;
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
			evaluateSplit: (input) => this.#splitPlanner.evaluateSplit(input),
			computeRangeBoundaries: ({ hashKey, start, end, childCount }) => this.#splitPlanner.rangeBoundaries(hashKey, start, end, childCount),
			migration: new MaterializationMigrationHost({ store: this.#store, selfId: () => this.ctx.id.toString() }),
			// Each call of a target from before the cutover then has an epoch below the epoch that the
			// child gets with its inherited work, so each receiver rejects a late call. A promotion
			// source keeps its other keys and its runner continues, so its cursors move past the moved
			// items. The snapshot keeps the cursors of the cutover for the child.
			onRepartitionCutover: (plan) => {
				this.#store.raiseAllTargetEpochs();
				if (plan.kind === "key_promotion") {
					this.#store.storeCursorSnapshot(promotedKeyOf(plan));
				}
			},
			// The child has each row of the promoted key now. Each new write of the key goes to the
			// child, so the estimate is not necessary, and the cleanup can delete the rows above the
			// snapshot.
			beforeComplete: (plan) => {
				if (plan.kind === "key_promotion") {
					const hashKey = promotedKeyOf(plan);
					this.#store.keySizes.remove(hashKey);
					this.#store.deleteCursorSnapshot(hashKey);
				}
			},
			// A split keeps its rows, as `FokosDBPartitionDO` does. A promotion deletes the items of its key in
			// steps of a limited size, because one large delete holds each request of this partition
			// until its write is durable.
			cleanupSourceStep: (plan) => {
				if (plan.kind !== "key_promotion") {
					return true;
				}
				const hashKey = promotedKeyOf(plan);
				if (!this.#store.transactionSync(() => this.#store.deleteItemsOfHashKey(hashKey, this.config().cleanup.deleteRows))) {
					return false;
				}
				this.#store.keySizes.remove(hashKey);
				return true;
			},
			jobs: [
				{
					name: JOB_TARGET_PUSH,
					canRun: (lifecycle) => this.#canRunLocally(lifecycle),
					deadline: () => this.#runner.deadline(),
					runStep: () => {
						this.#runner.wake();
						return { nextRunAt: null };
					},
				},
			],
		};
	}

	/**
	 * The target runner needs complete local state. An uninitialized partition has none. An importing
	 * target does not hold it yet. A split router no longer holds it. A fenced partition is on its way
	 * out and must make no transition.
	 */
	#canRunLocally(lifecycle: FokosLifecycle): boolean {
		if (!this.fokos.initialized()) {
			return false;
		}
		if (lifecycle.destroying || lifecycle.role === "router") {
			return false;
		}
		return lifecycle.import === null || (lifecycle.import.state !== "awaiting_data" && lifecycle.import.state !== "importing");
	}

	/**
	 * The stored target list. The first call of a partition with no stored list validates
	 * `this.fokosTargets()`, checks the plans of each GSI target, and stores the list with a state row for
	 * each target. A failed check stores nothing, so the next call checks the list again. Each later call
	 * uses only the stored list, and never refuses it. When `this.fokosTargets()` is different from it,
	 * the partition logs one error for each instance and continues with the stored list.
	 */
	#targets(): readonly FokosTargetDefinition[] {
		if (this.#targetList !== undefined) {
			return this.#targetList;
		}
		const declared = this.fokosTargets();
		const stored = this.#store.storedTargets();
		if (stored === undefined) {
			validateTargets(declared);
			for (const target of declared) {
				if (target.kind === "gsi") {
					checkGsiPlans(this.#store, target);
				}
			}
			const list = structuredClone(declared);
			this.#store.storeTargets(list);
			this.#targetList = list;
			return list;
		}
		if (JSON.stringify(stored) !== JSON.stringify(declared)) {
			console.error({
				...this.logParams(),
				message: "fokos/mat: fokosTargets() is different from the stored target list, and the partition uses the stored list",
				storedTargetIds: stored.map((t) => t.targetId),
				declaredTargetIds: declared.map((t) => t.targetId),
			});
		}
		this.#targetList = stored;
		return stored;
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
		this.#targets();
		// The handler is synchronous, so no cursor and no batch in flight changes while it runs.
		const targets: TargetPositions[] = this.#store.targetRows().map((row) => ({
			cursor: row.cursorSeq,
			read: Math.max(row.cursorSeq, this.#runner.readPosition(row.targetId) ?? row.cursorSeq),
		}));
		let accepted = false;
		const grown: Array<{ hashKey: KeyBytes; keyEstBytes: number }> = [];
		const overSize = req.items.some((item) => !item.deleted) ? this.sizeRefusal() : undefined;
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
					const keyEstBytes = this.#store.putLatestImage(item, this.fokosNow(), current, targets);
					if (keyEstBytes !== undefined && !item.deleted) {
						grown.push({ hashKey: item.hashKey, keyEstBytes });
					}
					if (item.deleted && targets.length === 0) {
						// No target reads the tombstone, so the cleanup can delete it now.
						this.#cleanup.tombstoneUnblocked();
					}
					accepted = true;
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
		if (accepted) {
			this.#runner.wake();
		}
		// A key that grew past its share of the size limit gets its own range tree. Promotions are
		// signaled first, because an unfinished promotion blocks the split behind it.
		const promotionCandidates = this.#splitPlanner.promotionCandidates(this.fokos.identity(), this.fokos.policy(), grown);
		if (promotionCandidates.length > 0) {
			call.signal({ promotionCandidates });
		}
		if (accepted || overSize) {
			call.signal({ evaluateSplit: true });
		}
		return out;
	}

	/** The refusal of a live image above the admission limit of this partition, or undefined below it. */
	private sizeRefusal(): FokosUnavailableError | undefined {
		const maxSizeBytes = maxSplitBytes(this.fokos.identity(), this.fokos.policy());
		if (maxSizeBytes > 0 && this.#store.databaseSize > maxSizeBytes * ADMISSION_MARGIN) {
			return new FokosUnavailableError(UNAVAILABLE_CODES.partition_over_size, {
				message: "the materialization partition exceeded its limits, please retry later",
				attributes: { operation: "applyLatestImages" },
			});
		}
		return undefined;
	}

	/** The active settings: the overrides of `fokosConfig()` over the defaults, validated. */
	private __cachedConfig?: MaterializationDOConfig;
	private config(): MaterializationDOConfig {
		if (!this.__cachedConfig) {
			this.__cachedConfig = resolveMaterializationConfig(this.fokosConfig());
		}
		return this.__cachedConfig;
	}

	private logParams(): Record<string, unknown> {
		// Cloudflare Workers can truncate this to 1024 bytes. The runtime logs the full doName with its own lines.
		return { actorId: this.ctx.id.toString(), actorName: this.ctx.id.name };
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
	 * Override to declare the targets of this table. A partition stores the first valid result, and then
	 * uses only the stored list, so the list cannot change after the first request. Each `targetId` is
	 * not empty, has no `~`, and is unique.
	 */
	protected fokosTargets(): readonly FokosTargetDefinition[] {
		return [];
	}

	/**
	 * Override to change the settings of this class. The rules of `FokosDBPartitionDO.fokosConfig` apply.
	 */
	protected fokosConfig(): MaterializationDOConfigOverrides {
		return {};
	}

	/**
	 * Override to change the settings of the sharding runtime of this class. The rules of
	 * `FokosDBPartitionDO.fokosRuntimeConfig` apply.
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

/** The log fields of an error. The detail of an `invariant` failure is in the attributes, not in the message. */
function errorFields(error: unknown): { error: string; attributes?: Record<string, unknown> } {
	return { error: String(error), ...(FokosError.is(error) ? { attributes: error.attributes } : {}) };
}

function producerEpochFenced(epoch: number): FokosConflictError {
	return new FokosConflictError(CONFLICT_CODES.producer_epoch_fenced, {
		message: "the producer sent this call with an epoch below the epoch that the receiver stored",
		attributes: { epoch },
	});
}
