---
name: profile-worker-local
description: Capture CPU and heap profiles of a fokosdb http-api worker running locally under `wrangler dev`, by driving the Chrome DevTools Protocol (CDP) inspector with tools/cdp-profile.mjs under k6 load — no browser, no manual clicking. Use when the user asks to profile a local worker, find hot functions in workerd, or automate the DevTools cpu-usage/memory-usage workflow.
---

# Profile a local `wrangler dev` worker

`wrangler dev` runs the worker in workerd with a CDP inspector. The Cloudflare docs
(workers/observability/dev-tools/cpu-usage and /memory-usage) drive it through a DevTools UI, but
every step maps to a CDP command — `tools/cdp-profile.mjs` does it with no browser.

## Rules

- **Traffic during capture.** `Profiler.start`/`stop` and `HeapProfiler.startSampling` record what
  runs in the window. Start the load first, capture second. `--type snapshot` is a point-in-time
  heap dump; traffic is optional but makes the heap realistic.
- **Do not edit code during a capture.** `wrangler dev` restarts the isolate on file change and the
  profile silently ends early or dies.
- **One isolate runs everything.** Locally the routes, every Durable Object, and SQLite share one
  workerd isolate, so one capture sees the whole stack — no per-actor targeting as in production.
- **Keep outputs out of git.** Profiles can hold string data from the isolate. Write to `/tmp`.

## Procedure

### 1. Build and start the dev server

The example imports the built `dist/`, so build first:

```bash
pnpm build
cd examples/http-api && npx wrangler dev --inspector-port 9230
```

- Pass `--inspector-port` explicitly — the default (9229) may be taken or differ between wrangler
  versions, and the capture script needs a known port.
- Wait for `Ready on http://localhost:8787`.
- Confirm the inspector: `curl -sS http://127.0.0.1:9230/json/list` must return a target list with a
  `webSocketDebuggerUrl` (usually a single `Cloudflare Worker` entry).

### 2. Start the load

`tools/k6_profile.js` defaults already fit local dev: `BASE_URL=http://localhost:8787`,
`FOKOS_API_TOKEN=dev-token-1` (a valid token from `examples/http-api/.dev.vars`), `ROOTS=1`.

```bash
k6 run -e TABLE=profile-local -e RPS=150 -e DURATION=120s tools/k6_profile.js
```

- Keep `ROOTS=1`: one root partition concentrates the work, exactly like production profiling.
- Use a fresh `TABLE` per topology — local DO storage persists in `.wrangler/state` across restarts.
- Duration must cover the capture window plus the ~30 s k6 warm-up.

### 3. Capture

```bash
node tools/cdp-profile.mjs --type cpu      --port 9230 --duration 50000 --out /tmp/local-cpu.cpuprofile
node tools/cdp-profile.mjs --type heap     --port 9230 --duration 50000 --out /tmp/local-heap.heapprofile
node tools/cdp-profile.mjs --type snapshot --port 9230                      --out /tmp/local-snap.heapsnapshot
```

- `--type cpu` → V8 `.cpuprofile` (sampling interval `--interval`, default 250 µs).
- `--type heap` → sampling `.heapprofile` (one stack per `--heap-interval` bytes, default 16384).
- `--type snapshot` → full `.heapsnapshot`, taken immediately; ignores `--duration`.
- `--target <substr>` selects one entry when `/json/list` shows several; without it the script takes
  the first target and prints which one.
- There is no rate limit and no duration cap — longer captures give more samples.

### 4. Analyze

The outputs are **V8 formats, not pprof** — `go tool pprof` cannot read them.

- `.cpuprofile` — Chrome DevTools → Performance → load icon, or drag onto speedscope.app.
- `.heapprofile` — DevTools → Memory → load → "Allocation sampling".
- `.heapsnapshot` — DevTools → Memory → load — dominator tree, retained sizes, detachment checks.
- Command-line option: `npx speedscope /tmp/local-cpu.cpuprofile` prints a flame summary, or convert
  to pprof with a converter (for example `node --cpu-prof` files share this format and converters
  such as `pprof-from-v8` exist) — DevTools is the reliable path.

## How to read the result

- Local CPU profiles are far denser than production ones: SQLite runs in the same isolate, so query
  work shows up as JS samples instead of invisible native time. Expect hundreds of samples per
  second of load.
- Function names include `#`-prefixed private methods (`#dispatch`, `#guard`) and the minified-style
  names of the built bundle — the build ships source maps (`upload_source_maps` affects production,
  not local). DevTools applies `dist/*.js.map` names where frames match.
- Local numbers rank functions correctly, but absolute values differ from production: there is no
  network hop to a DO, no per-actor isolation, and the machine is faster than a workers isolate.
  Verify suspected hot spots against a production capture (skill `profile-worker`).

## Failure modes

- `No CDP targets` — `wrangler dev` is not up yet, or the inspector is on a different port. Check
  `curl http://127.0.0.1:<port>/json/list` and pass `--inspector-port` at startup.
- `WebSocket connect failed` — workerd requires an `Origin` header on the handshake; the script
  sends one. If a hand-rolled client fails with `Expected \`Origin\` header`, that is why.
- Empty/sparse CPU profile — the load was not running during `--duration`, or the isolate restarted
  mid-capture. Check the k6 run covered the whole window.
- `401` from the API — pass `-e FOKOS_API_TOKEN=dev-token-1` (or another token from `.dev.vars`).
- Worker stops answering, `/json/list` times out, wrangler logs `Cannot assign requested address` —
  workerd hit ephemeral-port/file-descriptor limits under load. Stay at or below ~150 it/s for a CDP
  session; at ~250 it/s the inspector starves first and the server wedges. Kill `wrangler dev` and
  every `workerd` child of that run, wait for the ports to free, and restart.
- Many targets in `/json/list` — use `--target <substr>` to pick the one whose title or url matches;
  the script prints the list when it cannot pick.
