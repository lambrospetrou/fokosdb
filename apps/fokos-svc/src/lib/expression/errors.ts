export type ExpressionErrorCode = "invalid_ast" | "invalid_literal" | "invalid_path" | "complexity_limit";

export class ExpressionError extends Error {
	readonly name = "ExpressionError";

	constructor(
		readonly code: ExpressionErrorCode,
		message: string,
	) {
		super(`fokos/expression: ${message}`);
	}
}
