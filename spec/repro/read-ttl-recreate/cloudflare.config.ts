import { bindings, defineConfig, defineWorker, exports } from "cf/config";
import * as entrypoint from "./src/index.ts" with { type: "cf-worker" };

const worker = defineWorker({
	name: "fokosdb-repro-read-ttl-recreate",
	compatibilityDate: "2026-10-05",
	entrypoint,
	exports: {
		ReproPartitionDO: exports.durableObject({ storage: "sqlite" }),
		TransactionCoordinatorDO: exports.durableObject({ storage: "sqlite" }),
	},
});

export default defineConfig({
	worker: {
		...worker,
		env: {
			REPRO_PARTITION_DO: bindings.durableObject<typeof worker, "ReproPartitionDO">({ worker, exportName: "ReproPartitionDO" }),
			TRANSACTION_COORDINATOR_DO: bindings.durableObject<typeof worker, "TransactionCoordinatorDO">({
				worker,
				exportName: "TransactionCoordinatorDO",
			}),
			FOKOS_SHOULD_FETCH_COLO_INFO: bindings.json(false),
		},
	},
});
