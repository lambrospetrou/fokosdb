/**
 * The largest serialized value that one Workers RPC can carry: the arguments of a call, or its return
 * value. Above it, workerd fails the call. Each limit on one RPC message is a fraction
 * of this value, so that it stays below the limit with a margin.
 */
export const WORKERS_RPC_MAX_BYTES = 32 * 1024 * 1024;
