import { compileConditionExpression, compileProjectionExpression, compileQueryExpression, compileUpdateExpression } from "./compiler.js";
import {
	refuseCompiledPlan,
	type CompiledConditionPlan,
	type CompiledProjectionPlan,
	type CompiledQueryPlan,
	type CompiledUpdatePlan,
} from "./plan.js";
import type { ConditionExpression, ProjectionExpression, QueryExpressions, UpdateExpression } from "./types.js";

// Each function compiles the expression tree of a request, and refuses a request that carries a
// compiled plan.

export function conditionPlanOf(condition: ConditionExpression): CompiledConditionPlan {
	refuseCompiledPlan(condition);
	return compileConditionExpression(condition);
}

const projectionPlans = new WeakMap<readonly ProjectionExpression[], CompiledProjectionPlan>();

/**
 * One batch read runs one projection for many items: the projection of the request, or the projection
 * of an item. Thus the plan is kept for the tree object, as `updatePlanOf` keeps the plan of an update,
 * and one request compiles each tree one time.
 */
export function projectionPlanOf(projection: readonly ProjectionExpression[]): CompiledProjectionPlan {
	let plan = projectionPlans.get(projection);
	if (plan === undefined) {
		refuseCompiledPlan(projection);
		plan = compileProjectionExpression(projection);
		projectionPlans.set(projection, plan);
	}
	return plan;
}

export function queryPlanOf(query: QueryExpressions): CompiledQueryPlan {
	refuseCompiledPlan(query);
	return compileQueryExpression(query);
}

const updatePlans = new WeakMap<UpdateExpression, CompiledUpdatePlan>();

/**
 * One request runs the plan of an update in more than one statement: the probe, then a write or a
 * lock row. Thus the plan is kept for the tree object, and each statement of the request gets the
 * same plan and the same bound values. The map holds the tree weakly, so the plan lives only as long
 * as the request.
 */
export function updatePlanOf(update: UpdateExpression): CompiledUpdatePlan {
	let plan = updatePlans.get(update);
	if (plan === undefined) {
		refuseCompiledPlan(update);
		plan = compileUpdateExpression(update);
		updatePlans.set(update, plan);
	}
	return plan;
}
