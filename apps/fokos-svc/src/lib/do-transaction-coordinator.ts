import { DurableObject } from "cloudflare:workers";

export class TransactionCoordinatorDO extends DurableObject<Env> {}
