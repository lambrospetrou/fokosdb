import { defineConfig } from "oxlint";

/**
 * The `shared/` modules that the sharding library can import. Keep this list equal to
 * `SHARDING_ALLOWED_SHARED` in `tsdown.config.ts`.
 */
const SHARDING_ALLOWED_SHARED = ["errors", "invariant", "tsutils", "cache-lru", "sql-cursor", "cf-utils", "bloom-filter", "platform"];

export default defineConfig({
	ignorePatterns: ["dist/**", "node_modules/**"],
	options: {
		typeAware: true,
		typeCheck: true,
	},
	rules: {
		curly: "error",
		"typescript/no-floating-promises": "error",
		"typescript/no-misused-promises": "error",
		"typescript/return-await": ["error", "always"],
	},
	overrides: [
		{
			files: ["src/**/*[!test].ts"],
			rules: {
				"typescript/no-unsafe-assignment": "error",
			},
		},
		{
			// The sharding library uses no FokosDB module. The bundle check in `tsdown.config.ts` sees only
			// runtime imports. This rule also sees `import type`, so a FokosDB type cannot get into the
			// published sharding types.
			files: ["src/sharding/**/*.ts"],
			rules: {
				"no-restricted-imports": [
					"error",
					{
						patterns: [
							{
								group: ["**/client/**", "**/server/**", "**/shared/**", ...SHARDING_ALLOWED_SHARED.map((name) => `!**/shared/${name}.js`)],
								message: "The sharding library must not import a FokosDB module.",
							},
						],
					},
				],
			},
		},
		{
			files: ["src/sharding/**/*.test.ts"],
			rules: {
				"no-restricted-imports": "off",
			},
		},
	],
});
