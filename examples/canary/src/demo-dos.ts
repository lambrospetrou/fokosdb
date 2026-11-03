import { FokosDBAutoAdminDO, FokosDBPartitionDO, FokosDBTransactionCoordinatorDO } from "fokosdb/server";

// The demo binds its own subclasses instead of the library classes so that `fokosConfig()` and
// `fokosRuntimeConfig()` overrides can be exercised without another binding.

export class DemoPartitionDO extends FokosDBPartitionDO {}

export class DemoTransactionCoordinatorDO extends FokosDBTransactionCoordinatorDO {}

export class DemoAutoAdminDO extends FokosDBAutoAdminDO {}
