# A Promotion Cuts Over With the Locks of Its Key

Status: **idea**. Nothing is built. Found on 2026-09-26 during M3 of
`docs/agent-plans/2026-09-26-fokos-sharding-client.md`.

## 1. The problem

A promotion does not cut over while a transaction lock is on its key. The FokosDB host sets this rule in
`beforeCutover` (`packages/fokosdb/src/server/do-partition.ts`):

```ts
beforeCutover: (plan) => plan.kind !== "key_promotion" || this.#store.pendingLockCountForHashKey(promotedKeyOf(plan)) === 0,
```

The runtime calls `beforeCutover` before it initializes the first target, and again inside the cutover
transaction. While it returns false, the plan stays `queued` or `planned`, and the runtime tries again at the flat
lock interval.

So the time before the cutover has no upper limit:

- A hot key gets new transactions all the time. Each lock can hold the cutover again.
- A lock in quarantine stays until an operator calls `debugForceResolveTransaction`.

During that time, the key stays on its hash partition, and that partition is over its promotion threshold. The
range root exists and has no data. Problem 7 of the sharding client spec is one effect of this time: a Bloom false
positive can send a write to that range root.

## 2. What exists now

- **A split does not wait for locks.** Its `beforeCutover` returns true. The host copies the locks of the slice to
  each child in the `pending_tx` stream (`shared/partition/fokos-migration-host.ts`). After every child
  acknowledges, `beforeComplete` deletes the lock copies of the source.
- **The `pending_tx` stream has no filter by kind.** It keeps the rows for which `belongsToTarget` is true. For a
  promotion, `belongsToTarget` tests the hash key. So the stream already copies the locks of the promoted key to
  the range root, if one is there at the cutover.
- **The rule has no recorded reason.** `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md` (section 4.2,
  `beforeCutover`) and `docs/ideas/fokos-sharding/2026-09-09-fokos-partition-runtime.md` (the invariant table)
  state "Promotion cutover never moves a key with local locks". The rule comes from the earlier `PromotionManager`
  design. The comment at `beforeCutover` gives one effect only: without the rule, a forced commit after the
  cutover finds no pending row at the range root. The copy in the `pending_tx` stream removes that effect.

## 3. The idea

Remove the lock condition from the promotion `beforeCutover`, and let the promotion copy the locks of its key, as
a split does. Then:

- The time before the cutover is short: the `fokosInit` of the range root, and nothing more.
- A lock in quarantine moves with its key. `debugForceResolveTransaction` and the stale-transaction job apply a
  terminal outcome through `dispatch`, which resolves the key to the range root.
- The Bloom fallback of M3 stays, but it covers a short time only.

## 4. What must be checked first

1. **The lock copies of the source.** A promotion source stays the owner of its other keys, and `beforeComplete`
   deletes nothing for a promotion. The source cleanup gives back the rows of the key. Check that it also deletes
   the lock rows of the key, and that nothing reads them between the cutover and the cleanup.
2. **The stale-transaction job on the source.** `canSweepLocally()` is true on a promotion source, because it owns
   other keys. Its scan must skip a lock of a key that now routes to a range root, or apply the outcome through
   `dispatch`, as it does now.
3. **A commit or a cancel that arrives during the import.** The range root answers `partition_migrating` until it
   has its pages. The coordinator retries. Check that the retry budget of `runCommit` covers a long import, and
   that a commit decision is never lost.
4. **A prepare that arrives after the cutover.** It goes to the range root, which answers `partition_migrating`
   while it imports. Check that the transaction cancels cleanly, as for a split.
5. **The pending row and the item row move in one import.** The `items` stream runs before `pending_tx`. Check that
   a commit that applies at the source before the cutover, and a lock that the source copies after, cannot give a
   pending row for an item that already committed.
6. **The tests.** `test/partition-do/promotion.test.ts` ("defers cutover to 'promoting' while the key has a
   pending transaction lock") tests the current rule. It must change to test that the lock moves.

## 5. References

- `docs/agent-plans/2026-09-26-fokos-sharding-client.md` (problem 7, section 4.2.10)
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`
- `packages/fokosdb/src/server/do-partition.ts` (`hooks().beforeCutover`, `hooks().beforeComplete`)
- `packages/fokosdb/src/shared/partition/fokos-migration-host.ts`
