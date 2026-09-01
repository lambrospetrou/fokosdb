type Arity = readonly [minimum: number, maximum: number];

const VARIADIC = Number.POSITIVE_INFINITY;

/** https://sqlite.org/lang_corefunc.html */
const CORE_FUNCTION_ARITY: Record<string, Arity> = {
	abs: [1, 1],
	char: [1, VARIADIC],
	coalesce: [2, VARIADIC],
	concat: [1, VARIADIC],
	concat_ws: [2, VARIADIC],
	glob: [2, 2],
	hex: [1, 1],
	ifnull: [2, 2],
	iif: [2, 3],
	instr: [2, 2],
	length: [1, 1],
	like: [2, 3],
	lower: [1, 1],
	ltrim: [1, 2],
	nullif: [2, 2],
	octet_length: [1, 1],
	quote: [1, 1],
	replace: [3, 3],
	round: [1, 2],
	rtrim: [1, 2],
	sign: [1, 1],
	substr: [2, 3],
	substring: [2, 3],
	trim: [1, 2],
	typeof: [1, 1],
	unhex: [1, 2],
	unicode: [1, 1],
	upper: [1, 1],
};

/** https://sqlite.org/lang_mathfunc.html */
const MATH_FUNCTION_ARITY: Record<string, Arity> = {
	acos: [1, 1],
	acosh: [1, 1],
	asin: [1, 1],
	asinh: [1, 1],
	atan: [1, 1],
	atan2: [2, 2],
	atanh: [1, 1],
	ceil: [1, 1],
	cos: [1, 1],
	cosh: [1, 1],
	degrees: [1, 1],
	exp: [1, 1],
	floor: [1, 1],
	ln: [1, 1],
	log: [1, 2],
	log2: [1, 1],
	mod: [2, 2],
	pi: [0, 0],
	pow: [2, 2],
	radians: [1, 1],
	sin: [1, 1],
	sinh: [1, 1],
	sqrt: [1, 1],
	tan: [1, 1],
	tanh: [1, 1],
	trunc: [1, 1],
};

export const SQLITE_CORE_FUNCTIONS: ReadonlySet<string> = new Set(Object.keys(CORE_FUNCTION_ARITY));

export const SQLITE_MATH_FUNCTIONS: ReadonlySet<string> = new Set(Object.keys(MATH_FUNCTION_ARITY));

export const SQLITE_SCALAR_FUNCTIONS: ReadonlySet<string> = new Set([...SQLITE_CORE_FUNCTIONS, ...SQLITE_MATH_FUNCTIONS]);

/** Minimum and maximum argument counts for every allowed SQLite scalar function. */
export const SQLITE_FUNCTION_ARITY: ReadonlyMap<string, Arity> = new Map([
	...Object.entries(CORE_FUNCTION_ARITY),
	...Object.entries(MATH_FUNCTION_ARITY),
]);

export function isAllowedSqliteScalarFunction(name: string): boolean {
	return SQLITE_SCALAR_FUNCTIONS.has(name);
}
