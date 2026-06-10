// Pure re-export barrel so existing imports keep compiling while importers migrate to the
// focused modules. Deleted in phase 5 of the layering refactor plan (see
// docs/agent-plans/adr-lib-layering-refactor.md).
export * from "./partition-context.js";
export * from "./partition-id.js";
export * from "./router.js";
export * from "./split-state.js";
export * from "./split-policy.js";
