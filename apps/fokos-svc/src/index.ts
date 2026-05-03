import { DurableObject } from "cloudflare:workers";

export { PartitionDO } from "./lib/do-partition.js";

/** A Durable Object's behavior is defined in an exported Javascript class */
export class MyDurableObject extends DurableObject<Env> {
	/**
	 * The constructor is invoked once upon creation of the Durable Object, i.e. the first call to
	 * 	`DurableObjectStub::get` for a given identifier (no-op constructors can be omitted)
	 *
	 * @param ctx - The interface for interacting with Durable Object state
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 */
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
	}

	/**
	 * The Durable Object exposes an RPC method sayHello which will be invoked when a Durable
	 *  Object instance receives a request from a Worker via the same method invocation on the stub
	 *
	 * @param name - The name provided to a Durable Object instance from a Worker
	 * @returns The greeting to be sent back to the Worker
	 */
	async sayHello(name: string): Promise<string> {
		return `Hello, ${name}!`;
	}
}

export default {
	/**
	 * This is the standard fetch handler for a Cloudflare Worker
	 *
	 * @param request - The request submitted to the Worker from the client
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 * @param ctx - The execution context of the Worker
	 * @returns The response to be sent back to the client
	 */
	async fetch(request, env, ctx): Promise<Response> {
		// Create a stub to open a communication channel with the Durable Object
		// instance named "foo".
		//
		// Requests from all Workers to the Durable Object instance named "foo"
		// will go to a single remote Durable Object instance.
		const stub = env.MY_DURABLE_OBJECT.getByName("foo");

		// Call the `sayHello()` RPC method on the stub to invoke the method on
		// the remote Durable Object instance.
		const greeting = await stub.sayHello("world");

		return new Response(greeting);
	},
} satisfies ExportedHandler<Env>;
