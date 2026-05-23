import { exports } from "cloudflare:workers";
import { it } from "vitest";

it("enables SQL API with migrations", async ({ expect }) => {
	const response = await exports.default.fetch("https://example.com/api/hello/world");
	expect(await response.json()).toEqual({ message: "Hello, world!" });
});
