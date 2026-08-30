/** https://sqlite.org/lang_corefunc.html */
export const SQLITE_CORE_FUNCTIONS: ReadonlySet<string> = new Set([
	"abs",
	"char",
	"coalesce",
	"concat",
	"concat_ws",
	"glob",
	"hex",
	"ifnull",
	"iif",
	"instr",
	"length",
	"like",
	"lower",
	"ltrim",
	"nullif",
	"octet_length",
	"quote",
	"replace",
	"round",
	"rtrim",
	"sign",
	"substr",
	"substring",
	"trim",
	"typeof",
	"unhex",
	"unicode",
	"upper",
]);

/** https://sqlite.org/lang_mathfunc.html */
export const SQLITE_MATH_FUNCTIONS: ReadonlySet<string> = new Set([
	"acos",
	"acosh",
	"asin",
	"asinh",
	"atan",
	"atan2",
	"atanh",
	"ceil",
	"cos",
	"cosh",
	"degrees",
	"exp",
	"floor",
	"ln",
	"log",
	"log2",
	"mod",
	"pi",
	"pow",
	"radians",
	"sin",
	"sinh",
	"sqrt",
	"tan",
	"tanh",
	"trunc",
]);

export const SQLITE_SCALAR_FUNCTIONS: ReadonlySet<string> = new Set([...SQLITE_CORE_FUNCTIONS, ...SQLITE_MATH_FUNCTIONS]);

export function isAllowedSqliteScalarFunction(name: string): boolean {
	return SQLITE_SCALAR_FUNCTIONS.has(name);
}
