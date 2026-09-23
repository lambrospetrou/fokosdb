/**
 * PROTOTYPE. Negative cases: each `@ts-expect-error` line must fail to compile, or tsc reports an unused
 * directive. This is what the typed registry buys over `dispatch<Req, Res>(op: string, ...)`.
 */
import type { FokosOperation, FokosOperations } from "./api.js";
import { todo } from "./api.js";
import type { DocsDOOps } from "./example-host.js";
import type { PartitionDO } from "./fokosdb-partition-host.js";

declare const partition: PartitionDO;
declare const ctx: Parameters<PartitionDO["apiGetItem"]>[0];
declare const getReq: Parameters<PartitionDO["apiGetItem"]>[1];

async function dispatchIsTypedByName(): Promise<void> {
	await partition.fokos.dispatch("apiGetItem", ctx, getReq);
	// @ts-expect-error a get request is not a put request
	await partition.fokos.dispatch("apiPutItem", ctx, getReq);
	// @ts-expect-error an operation that the host did not register
	await partition.fokos.dispatch("apiScanItems", ctx, getReq);
	const res = await partition.fokos.dispatch("txReadSnapshot", ctx, { items: [] });
	// @ts-expect-error the result is typed from the spec
	if (res.value.outcome === "rejected") return;
	await partition.fokos.forward({ partitionId: "", doName: "" }, "txCancel", { transactionId: "t", items: [] });
	// @ts-expect-error a forward carries the request of the named operation
	await partition.fokos.forward({ partitionId: "", doName: "" }, "txCancel", getReq);
}

type Ops = { echo: { req: { n: number }; res: { n: number } } };

const wrongResult: FokosOperations<Ops> = {
	// @ts-expect-error `local` must return the response of the spec
	echo: { shape: "point", whileMigrating: "retry", key: () => todo("key"), local: (req) => ({ text: String(req.n) }) },
};

// @ts-expect-error a range operation is read-through only
const rangeMustReadSource: FokosOperation<{ n: number }, { n: number }> = {
	shape: "range",
	whileMigrating: "retry",
	readOnly: true,
	range: () => todo("range"),
	clip: (req) => req,
	local: (req) => req,
	walk: async ({ request }) => request,
};

// A `local` shape has no `whileMigrating` and no key: nothing routes it.
const localShape: FokosOperation<{ n: number }, { n: number }> = { shape: "local", local: (req) => req };

/** The example host's spec is reachable from its class, so a test can type a stub with it. */
type DocScanReq = DocsDOOps["scan"]["req"];
declare const scanReq: DocScanReq;
void [wrongResult, rangeMustReadSource, localShape, scanReq, dispatchIsTypedByName];
