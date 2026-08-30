export const EXPRESSION_LIMITS = {
	operatorsAndFunctions: 300,
	astDepth: 32,
	jsonPathDereferences: 32,
	inChoices: 100,
	sqliteFunctionArguments: 32,
	sqlitePatternBytes: 50,
	jsonPathBytes: 4 * 1024,
	canonicalPayloadBytes: 512 * 1024,
	compiledSqlBytes: 100_000,
	completeStatementBindings: 100,
} as const;

export type ExpressionLimitName = keyof typeof EXPRESSION_LIMITS;
