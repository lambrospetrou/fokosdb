import { bench, describe } from "vitest";
import { validateConditionExpression } from "./semantic.js";
import type { ConditionExpression } from "./types.js";

const cases = [
	{
		name: "DynamoDB conditional create",
		expression: { op: "not_exists", args: [{ ref: "hashKey" }] },
	},
	{
		name: "DynamoDB optimistic lock",
		expression: {
			op: "and",
			args: [
				{ op: "exists", args: [{ ref: "hashKey" }] },
				{ op: "eq", args: [{ ref: "v" }, { val: 42 }] },
			],
		},
	},
	{
		name: "DynamoDB workflow transition",
		expression: {
			op: "and",
			args: [
				{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "pending" }] },
				{ op: "in", args: [{ ref: "data", path: "$.region" }, { val: "us-east-1" }, { val: "eu-west-1" }] },
				{ op: "gte", args: [{ ref: "data", path: "$.total" }, { val: 100 }] },
				{ op: "not_exists", args: [{ ref: "data", path: "$.cancelledAt" }] },
			],
		},
	},
	{
		name: "DynamoDB nested access policy",
		expression: {
			op: "and",
			args: [
				{ op: "eq", args: [{ ref: "data", path: "$.tenantId" }, { val: "tenant-123" }] },
				{
					op: "or",
					args: [
						{ op: "eq", args: [{ ref: "data", path: "$.role" }, { val: "admin" }] },
						{ op: "contains", args: [{ ref: "data", path: "$.permissions" }, { val: "write" }] },
					],
				},
				{
					op: "or",
					args: [
						{ op: "not_exists", args: [{ ref: "ttlAt" }] },
						{ op: "gt", args: [{ ref: "ttlAt" }, { val: 1_788_000_000 }] },
					],
				},
			],
		},
	},
	{
		name: "SQLite normalized text",
		expression: {
			op: "and",
			args: [
				{
					op: "eq",
					args: [
						{
							fn: "sqlite.lower",
							args: [{ fn: "sqlite.trim", args: [{ fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.email" }, { val: "" }] }] }],
						},
						{ val: "user@example.com" },
					],
				},
				{
					op: "begins_with",
					args: [
						{ fn: "sqlite.lower", args: [{ fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.displayName" }, { val: "" }] }] },
						{ val: "ali" },
					],
				},
				{ op: "gte", args: [{ fn: "sqlite.octet_length", args: [{ ref: "data", path: "$.notes" }] }, { val: 128 }] },
			],
		},
	},
	{
		name: "SQLite numeric scoring",
		expression: {
			op: "and",
			args: [
				{
					op: "gte",
					args: [
						{
							fn: "sqlite.round",
							args: [
								{ fn: "sqlite.abs", args: [{ fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.score" }, { val: 0 }] }] },
								{ val: 2 },
							],
						},
						{ val: 75 },
					],
				},
				{
					op: "lte",
					args: [
						{
							fn: "sqlite.sqrt",
							args: [{ fn: "sqlite.abs", args: [{ fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.variance" }, { val: 0 }] }] }],
						},
						{ val: 10 },
					],
				},
				{
					op: "eq",
					args: [
						{ fn: "sqlite.sign", args: [{ fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.balance" }, { val: 0 }] }] },
						{ val: 1 },
					],
				},
			],
		},
	},
	{
		name: "SQLite deeply composed text",
		expression: {
			op: "eq",
			args: [
				{
					fn: "sqlite.lower",
					args: [
						{
							fn: "sqlite.trim",
							args: [
								{
									fn: "sqlite.replace",
									args: [
										{
											fn: "sqlite.substr",
											args: [
												{ fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.contact.email" }, { val: "" }] },
												{ val: 1 },
												{ val: 128 },
											],
										},
										{ val: ".invalid" },
										{ val: "" },
									],
								},
							],
						},
					],
				},
				{ val: "user@example.com" },
			],
		},
	},
	{
		name: "SQLite wide concatenation",
		expression: {
			op: "eq",
			args: [
				{
					fn: "sqlite.concat",
					args: Array.from({ length: 32 }, (_, index) => ({ ref: "data" as const, path: `$.segments[${index}]` })),
				},
				{ val: "combined-value" },
			],
		},
	},
] as const satisfies readonly { name: string; expression: ConditionExpression }[];

for (const benchmark of cases) {
	validateConditionExpression(benchmark.expression);

	describe(benchmark.name, () => {
		bench(
			"custom",
			() => {
				validateConditionExpression(benchmark.expression);
			},
			{ time: 500, warmupTime: 100 },
		);
	});
}
