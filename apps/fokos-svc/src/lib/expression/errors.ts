export type ExpressionErrorCode =
	| "invalid_ast"
	| "invalid_literal"
	| "invalid_path"
	| "invalid_function"
	| "invalid_arity"
	| "invalid_type"
	| "complexity_limit"
	| "sql_limit"
	| "runtime_capability";

export class ExpressionError extends Error {
	readonly name = "ExpressionError";

	constructor(
		readonly code: ExpressionErrorCode,
		message: string,
	) {
		super(`fokos/expression: ${message}`);
	}
}
