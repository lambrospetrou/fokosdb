/**
 * The limits of one expression: one condition, one update, one projection, or one query filter with
 * its projection.
 *
 * Five values come from limits of SQLite in a Durable Object, from the "SQL storage limits" table of
 * https://developers.cloudflare.com/durable-objects/platform/limits/. A statement above one of them
 * fails in SQLite with an error that does not name the cause, so the expression engine refuses the
 * expression first. Do not make one of these values larger than the platform limit permits.
 *
 * The other values are budgets of this library. Each one bounds the work that one expression can
 * cause, and none comes from the platform.
 */
export const EXPRESSION_LIMITS = {
	/**
	 * The operators and the function calls in one expression, counted across all actions of an update
	 * and all entries of a projection. A budget of this library: it bounds the size of the tree that the
	 * validator and the compiler walk. The compiled SQL can reach `compiledSqlBytes` before the
	 * expression reaches this count.
	 */
	operatorsAndFunctions: 300,
	/**
	 * How deep one expression can nest. A budget of this library: the validator and the compiler are
	 * recursive, so the depth bounds their call stack. One level of the tree becomes more than one
	 * level of SQL, and SQLite refuses a statement whose expression is too deep.
	 */
	astDepth: 32,
	/** The actions in one update expression. A budget of this library. */
	updateActions: 32,
	/**
	 * The entries in one projection. SQLite in a Durable Object limits a result set to 100 columns
	 * (platform limit: "Maximum number of columns per table", 100). Each entry adds two columns to the
	 * statement: its value and its type. The query statement has 3 fixed columns and the point read has
	 * 2, so 48 is the largest count that both statements accept (3 + 2 × 48 = 99). A change of the fixed
	 * columns of `composeQueryStatement` or `composeProjectionStatement` changes this value.
	 */
	projectionEntries: 48,
	/** The member accesses and the array indexes in one JSON path. A budget of this library. */
	jsonPathDereferences: 32,
	/**
	 * The choices of one `in`. A budget of this library. Each distinct choice binds one parameter, so a
	 * condition with 100 distinct choices is already above `completeStatementBindings`.
	 */
	inChoices: 100,
	/** The arguments of one SQLite function call. Platform limit: "Maximum arguments per SQL function", 32. */
	sqliteFunctionArguments: 32,
	/**
	 * The UTF-8 bytes of one `like` or `glob` pattern. Platform limit: "Maximum characters (bytes) in a
	 * LIKE or GLOB pattern", 50 bytes.
	 */
	sqlitePatternBytes: 50,
	/** The UTF-8 bytes of one `as` name of a projection entry. A budget of this library. */
	projectionAliasBytes: 256,
	/** The UTF-8 bytes of one JSON path. A budget of this library. */
	jsonPathBytes: 4 * 1024,
	/**
	 * The payload of one expression: its literals, and each text that is built from all of them. It
	 * bounds four things:
	 *
	 * - The total UTF-8 bytes of the text literals and the base64 literals of one expression.
	 * - The text of one base64 literal.
	 * - The canonical identity text, which the coordinator hashes and the query cursor uses.
	 * - The JSON array that holds all bound values of a projection or a query. This array is one bound
	 *   value.
	 *
	 * The value must stay below the platform limit "Maximum string, BLOB or table row size", 2 MB,
	 * because the JSON array is one string that SQLite gets. 512 KiB is a budget of this library below
	 * that limit. It is above the largest item (`MAX_ITEM_BYTES`), so a literal can hold a value as
	 * large as an item.
	 */
	canonicalPayloadBytes: 512 * 1024,
	/**
	 * The UTF-8 bytes of the complete statement that runs a compiled expression. Platform limit:
	 * "Maximum SQL statement length", 100 KB.
	 */
	compiledSqlBytes: 100_000,
	/**
	 * The parameters that one complete statement binds: the parameters of the compiled expression and
	 * the parameters of the statement around it, such as the keys. Platform limit: "Maximum bound
	 * parameters per query", 100.
	 */
	completeStatementBindings: 100,
} as const;

export type ExpressionLimitName = keyof typeof EXPRESSION_LIMITS;
