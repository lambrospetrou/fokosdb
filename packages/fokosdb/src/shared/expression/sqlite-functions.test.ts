import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../../server/do-partition.js";
import { SQLITE_SCALAR_FUNCTIONS } from "./sqlite-functions.js";

const calls = {
	abs: "abs(-1)",
	char: "char(65)",
	coalesce: "coalesce(NULL, 1)",
	concat: "concat('a', 'b')",
	concat_ws: "concat_ws('-', 'a', 'b')",
	glob: "glob('a*', 'abc')",
	hex: "hex('a')",
	ifnull: "ifnull(NULL, 1)",
	iif: "iif(1, 'a', 'b')",
	instr: "instr('abc', 'b')",
	length: "length('abc')",
	like: "like('a%', 'abc')",
	lower: "lower('A')",
	ltrim: "ltrim(' a')",
	nullif: "nullif(1, 1)",
	octet_length: "octet_length('a')",
	quote: "quote('a')",
	replace: "replace('aba', 'a', 'x')",
	round: "round(1.5)",
	rtrim: "rtrim('a ')",
	sign: "sign(-1)",
	substr: "substr('abc', 1, 1)",
	substring: "substring('abc', 1, 1)",
	trim: "trim(' a ')",
	typeof: "typeof(1)",
	unhex: "unhex('61')",
	unicode: "unicode('a')",
	upper: "upper('a')",
	acos: "acos(1)",
	acosh: "acosh(1)",
	asin: "asin(0)",
	asinh: "asinh(0)",
	atan: "atan(0)",
	atan2: "atan2(0, 1)",
	atanh: "atanh(0)",
	ceil: "ceil(1.1)",
	cos: "cos(0)",
	cosh: "cosh(0)",
	degrees: "degrees(0)",
	exp: "exp(0)",
	floor: "floor(1.9)",
	ln: "ln(1)",
	log: "log(10)",
	log2: "log2(2)",
	mod: "mod(5, 2)",
	pi: "pi()",
	pow: "pow(2, 3)",
	radians: "radians(0)",
	sin: "sin(0)",
	sinh: "sinh(0)",
	sqrt: "sqrt(1)",
	tan: "tan(0)",
	tanh: "tanh(0)",
	trunc: "trunc(1.9)",
} as const satisfies Record<string, string>;

describe("SQLite scalar function allowlist", () => {
	it("contains only functions verified by the Workers SQLite runtime", async () => {
		expect([...SQLITE_SCALAR_FUNCTIONS].sort()).toEqual(Object.keys(calls).sort());
		const stub = PartitionDO.getByName(env.PARTITION_DO, `expression-functions.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			for (const call of Object.values(calls)) expect(() => state.storage.sql.exec(`SELECT ${call} AS value`).one()).not.toThrow();
		});
	});
});
