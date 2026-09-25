import { defineConfig } from "oxlint";

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
		// "typescript/no-unsafe-assignment": "warn",
	},
});
