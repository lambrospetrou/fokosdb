/**
 * The merge of the parts of a batch `group` operation with the failure policy `collect`. A host that
 * answers in the batch shape (`failed` and `partitions` lists, items by `opIndex`) joins its parts here.
 */
import type { FokosGroupFailure, FokosGroupPart } from "../sharding/runtime-types.js";
import { routedError } from "../sharding/envelope.js";
import { isRangePartition } from "../sharding/partition-id.js";
import type { BatchUnprocessedCode } from "./batch-api-types.js";
import { classifyBatchFailure } from "./batch-failure.js";
import type { RejectionReasonEncoded } from "./transaction-wire-types.js";
import type { WirePartitionMeta } from "./types.js";

/** One error that stopped one or more items. */
export type BatchFailure = {
	outcome: "rejected" | "unprocessed" | "unknown";
	code: BatchUnprocessedCode;
	/** Absent only for `response_too_large`, which is no error. */
	error_id?: string;
	/** Absent when no partition answered. */
	partitionIndex?: number;
	opIndexes: number[];
	/** Only for `condition_failed`, which stops one operation. `reason.item` is the old image. */
	reason?: RejectionReasonEncoded;
	/** The byte length of the image in `reason.item`, which `applyBatchImageCap` adds up. */
	imageBytes?: number;
	itemOmitted?: "response_too_large";
};

/**
 * Joins the parts of a batch operation. A part that answered adds its lists, with each
 * `partitionIndex` moved by the entries of the parts before it. A failed part gives all its items one
 * failure, and adds the entry of the partition that raised the error with no counts: `self` for the
 * local part, else the head of the routing of the error. A remote error with no routing adds no
 * entry. It never throws.
 */
export function mergeBatchParts<
	Req extends { items: Array<{ opIndex: number }> },
	Res extends { failed: BatchFailure[]; partitions: WirePartitionMeta[] },
>(
	parts: Array<FokosGroupPart<Req, Res> | FokosGroupFailure<Req>>,
	opts: { write: boolean; self: () => WirePartitionMeta },
	out: Res,
	addLists: (out: Res, result: Res, offset: number) => void,
): Res {
	for (const part of parts) {
		const offset = out.partitions.length;
		if ("result" in part) {
			addLists(out, part.result, offset);
			out.failed.push(...part.result.failed.map((entry) => withOffset(entry, offset)));
			out.partitions.push(...part.result.partitions);
			continue;
		}
		const local = part.target === "local";
		const entry = local ? opts.self() : raiserPartition(part.error);
		if (entry) {
			out.partitions.push(entry);
		}
		out.failed.push({
			...classifyBatchFailure(part.error, { write: opts.write, local }),
			opIndexes: part.request.items.map((item) => item.opIndex),
			...(entry ? { partitionIndex: offset } : {}),
		});
	}
	return out;
}

/**
 * An entry of one part of a merged answer, with its `partitionIndex` moved by `offset`: the length of
 * the lists of the parts before it.
 */
export function withOffset<T extends { partitionIndex?: number }>(entry: T, offset: number): T {
	return entry.partitionIndex === undefined ? entry : { ...entry, partitionIndex: offset + entry.partitionIndex };
}

/** The entry of the partition that raised a forwarded error, from the head of its routing, with no counts. */
export function raiserPartition(error: unknown): WirePartitionMeta | undefined {
	const raiser = routedError(error)?.routing.servedBy[0];
	if (!raiser) {
		return undefined;
	}
	return {
		actorId: raiser.actorId,
		...(isRangePartition(raiser.ref) ? { rangeDepth: raiser.rangeDepth } : { hashDepth: raiser.hashDepth }),
	};
}
