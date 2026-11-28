/**
 * The `host` backend of `fake-snapshots.ts`. This module runs in the Node process of vitest, not in
 * workerd: `vitest.config.ts` gives it to Miniflare as the service binding `FOKOS_SNAPSHOT_HOST`.
 *
 * A copy is the directory `<dir>/<id>/`, with one file for each page, `<seq>.json`, and the file
 * `pages` with the number of pages. The routes:
 *
 * - `PUT /<id>/<seq>` writes a page, and `GET /<id>/<seq>` reads it.
 * - `POST /<id>` writes the number of pages. `GET /<id>` reads it, or answers 404 when the copy is
 *   not complete.
 * - `DELETE /<id>` deletes the directory. With `FOKOS_KEEP_SNAPSHOTS=1`, it renames the directory to
 *   `<id>.kept/` in place of a delete. A GET then does not find the copy, as after a delete, but a
 *   person can read it after the run.
 */
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const ROUTE = /^\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(?:\/(\d+))?$/;

function isMissing(e: unknown): boolean {
	return (e as NodeJS.ErrnoException).code === "ENOENT";
}

export function snapshotHost(opts: { dir?: string; keep?: boolean } = {}) {
	const dir = opts.dir ?? join(tmpdir(), "fokosdb-snapshots");
	return async (request: Request): Promise<Response> => {
		// The ID and the page number become file names, so they must be a UUID and a number.
		const match = ROUTE.exec(new URL(request.url).pathname);
		if (match === null) {
			return new Response("bad snapshot path", { status: 400 });
		}
		const [, id, seq] = match;
		const copyDir = join(dir, id);
		const file = seq === undefined ? join(copyDir, "pages") : join(copyDir, `${seq}.json`);
		switch (request.method) {
			case "PUT":
			case "POST":
				await mkdir(copyDir, { recursive: true });
				await writeFile(file, await request.text());
				return new Response(null, { status: 204 });
			case "GET":
				try {
					return new Response(await readFile(file, "utf8"));
				} catch (e) {
					if (isMissing(e)) {
						return new Response("no such snapshot", { status: 404 });
					}
					throw e;
				}
			case "DELETE":
				if (opts.keep) {
					await rename(copyDir, join(dir, `${id}.kept`)).catch((e: unknown) => {
						if (!isMissing(e)) {
							throw e;
						}
					});
				} else {
					await rm(copyDir, { recursive: true, force: true });
				}
				return new Response(null, { status: 204 });
			default:
				return new Response("method not allowed", { status: 405 });
		}
	};
}
