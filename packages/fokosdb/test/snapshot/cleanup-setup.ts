// Every test file loads this module before it runs. After the file, it deletes the fake snapshot
// copies that the file made, also the files of the `host` backend.
import { afterAll } from "vitest";
import { clearFakeSnapshots } from "./fake-snapshots.js";

afterAll(async () => {
	await clearFakeSnapshots();
});
