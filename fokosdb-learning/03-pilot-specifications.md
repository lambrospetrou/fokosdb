# Initial exercise plans

These are three exercise plans for your personal study.
P1 is delivered as [exercise 0.1](exercises/01-request-flow.md). P2 and P3 remain specifications.
Start with P1. Create P2 and P3 when you reach their prerequisites.
Each exercise needs a complete tested solution before I hand it over.

P1 maps to 0.1, P2 maps to 4.5, and P3 maps to 6.5.
The exercises share the [challenge template](02-challenge-template.md).
Reuse the existing tests as described in the [minimal setup](01-environment-and-delivery-plan.md).
Keep fixtures local to each exercise. No shared pilot framework is needed.

## P1: Follow an item from API call to storage

Grade: medium, 1–2 hours including the lesson.
Prerequisite: entry readiness or F0. No database knowledge.
Mode: runtime, using the repository's existing test Worker and a supplied database factory.

### Challenge and outcomes

Build a small item lifecycle adapter against FokosDB.
Trace its put/get path into the implementation.
The learner must explain identity, version changes, and the storage boundary.

Reuse existing configuration and bindings. I supply imports and isolated test data.
The learner edits adapter functions and adds one boundary test.
Setup code is visible but does not dominate the task.

### Required teaching

Database: durable state, items, composite keys, absent items, versions, and request/response flow.
Explain a key with a library catalogue example before using account profiles in the task.
Explain that the Worker-side client routes operations to a partition.

Language: object fields, optional values, async/await, result narrowing, imports, and assertions.
Supply one unrelated async function example and an error-handling example.

Source map in the initial reference:

- `packages/fokosdb/test/sanity.test.ts`
- `packages/fokosdb/src/client/db.ts`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/shared/partition/partition-store.ts`

### Step contract

| Step | Learner work                                | Acceptance evidence                                                         |
| ---- | ------------------------------------------- | --------------------------------------------------------------------------- |
| 1    | Predict and implement an absent read.       | A missing item is handled according to the public API.                      |
| 2    | Store and retrieve a supplied item.         | The returned key and data match; a different key stays absent.              |
| 3    | Replace its data and inspect the version.   | Replacement follows the version contract; unrelated items remain unchanged. |
| 4    | Delete the item and trace the request.      | The read becomes absent; the trace identifies routing, RPC, and storage.    |
| 5    | Add a boundary test and explain the layers. | The test detects a plausible key or result-handling defect.                 |

The trace records files and symbols, not memorized line numbers.
Automated tests verify behavior.
Discuss the source trace and durable state in this chat.

### Transfer variant and hints

Give a new lifecycle using two sort keys under one hash key.
Ask the learner to predict and verify which item changes.
Omit the exact implementation locations from this variant.

Ask for hints in this chat; no hint files or hint levels need to be prepared.
The solution contains the complete adapter, tests, and an annotated request trace.

### Solution checks

Reject a fake adapter that returns the input without a database operation.
Reject a key mix-up and an adapter that ignores an absent read.
Run the solution through real local storage and repeat in a fresh table namespace.

A repeated read demonstrates stored behavior within the test.
It does not by itself demonstrate persistence across runtime restart.
State that limit in the lesson.

## P2: An empty page does not mean the query is finished

Grade: medium, 1–2 hours.
Prerequisites: 4.1 and basic API use, with local recaps.
Modes: a pure scripted query fixture plus a real runtime example.

### Challenge and outcomes

Repair a supplied pagination adapter.
The reported symptom is “some matching items never reach the caller”.
The learner forms a hypothesis before changing code.

The learner must distinguish evaluated candidates, matches, and continuation.
The learner must preserve query options and item order.
The learner must surface a failed page request rather than return silent partial success.

### Required teaching

Database: sorted access, filters, bounded pages, and cursor-based continuation.
Explain why a page can evaluate candidates without returning a match.
Use a paper catalogue example with a different filter from the task.

Language: loops, async functions, arrays, optional cursors, and exceptions.
Provide a scripted asynchronous source so the learner can observe each request.
Async iteration can be a later extension; it is not a prerequisite for this pilot.

Source map in the initial reference:

- `packages/fokosdb/src/client/db.ts`
- `packages/fokosdb/src/shared/query/query-collector.ts`
- `packages/fokosdb/src/shared/query/page-budget.ts`
- `packages/fokosdb/src/shared/query/cursor.ts`

### Fixture and step contract

The pure fixture returns a deterministic series:
an empty page with continuation, a matching page with continuation, then a terminal page.
Keep the precise fixture data in tests; the starter contains a plausible incorrect stop condition.

| Step | Learner work                                                      | Acceptance evidence                                              |
| ---- | ----------------------------------------------------------------- | ---------------------------------------------------------------- |
| 1    | Predict calls and results from a page transcript.                 | The written prediction separates matches from completion.        |
| 2    | Reproduce the missing-results defect and write a regression test. | The test fails against the starter for its stop behavior.        |
| 3    | Repair the adapter without changing supplied acceptance tests.    | Every expected item appears once in the supplied sequence.       |
| 4    | Handle a terminal empty page and a failed request.                | Execution terminates correctly; failure reaches the caller.      |
| 5    | Run the runtime example and explain its collector path.           | The behavior matches a real filtered query and the source trace. |

The runtime fixture controls data and evaluated-item limits to create an empty intermediate page.
It must not depend on incidental physical row metrics.
The adapter preserves the original query and uses the returned continuation token.

### Independent variant

Supply reverse ordering and a new sequence with several empty intermediate pages.
Include a terminal non-empty page without a cursor.
Ask for a learner-written test and an explanation, without naming the stop condition.

The acceptance contract concerns a fixed dataset.
Do not claim that paginated queries provide a snapshot under concurrent mutation.

### Solution checks

Reject implementations that stop on an empty page, omit the next cursor, duplicate a page, or swallow an error.
Verify the supplied tests against the complete solution and each named mistake.
Provide a separate complete solution for the assessment variant.
The expected answer must follow the contract, not hard-code the fixture sequence.

## P3: Resume a transaction after an interrupted commit

Grade: hard, 2–4 hours with supplied protocol and runtime scaffolding.
Prerequisites: 6.2 and 6.3; include F3's local recap.
Modes: pure durable-state model, then a real runtime recovery test.

### Challenge and outcomes

Complete a bounded recovery driver against a supplied ledger interface.
Predict state and caller knowledge when one participant cannot confirm commit.
Connect the model to the production coordinator.

The learner must distinguish an undecided transaction from a durable commit decision.
The learner must preserve the decision after PREPARED.
The learner must explain why repeated application does not duplicate effects.

### Required teaching

Database: local atomicity, two-phase commit, participant state, idempotency, and durable decisions.
Explain safety and progress separately.
Use a two-resource reservation example before the target account operation.

Language: discriminated unions, exhaustive state handling, async peers, allSettled, and cleanup.
Explain that a rejected promise does not identify every remote effect.

Source map in the initial reference:

- `packages/fokosdb/src/server/do-transaction-coordinator.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/test/transactions/tx-commit-fanout.test.ts`
- `packages/fokosdb/src/server/do-transaction-coordinator.test.ts`

### Model and runtime boundaries

The model stores ledger state independently from its driver object.
Discard the driver and reconstruct it from that ledger to test loss of temporary state.
Script peer responses and record durable transitions and dispatched messages.

The runtime fixture forces a transaction onto at least two real partitions.
It controls a participant failure through existing test seams.
It then invokes the production recovery path and checks persisted state and final effects.

Prototype this fixture before finalizing the learner instructions.
If the seam cannot reproduce the required schedule, revise the fixture and record the limitation.
Do not replace the runtime assertion with a mock-only claim.

### Step contract

| Step | Learner work                                               | Acceptance evidence                                                              |
| ---- | ---------------------------------------------------------- | -------------------------------------------------------------------------------- |
| 1    | Predict the state at defined interruption points.          | The timeline distinguishes durable state, messages, and caller knowledge.        |
| 2    | Resume a supplied pre-decision state in the model.         | The driver follows the specified prepare/recovery contract.                      |
| 3    | Resume PREPARED or COMMITTING with a failed participant.   | The driver preserves the commit decision and incomplete work.                    |
| 4    | Reconstruct the driver and repeat recovery.                | The ledger supplies the decision; repeated application preserves effects.        |
| 5    | Run the real runtime scenario and write a regression test. | The production path finishes recovery without duplicate effects.                 |
| 6    | Explain the model's correspondence and limits.             | The learner identifies actual production symbols and unsupported failure claims. |

Test a repeated recovery call and a persisted decision with incomplete acknowledgements.
Check the contract for operations, locks, and final outcomes.
Avoid exact private call counts unless they are necessary to the stated invariant.

### Independent variant

Supply a lost response after a participant has applied its commit.
Ask the learner to predict replay behavior and provide test evidence.
A second written case covers a rejection before the commit decision.
The learner explains why the cases permit different recovery actions.

This variant must change the failure schedule, not only the account names.
The solution includes the complete driver, runtime regression test, and annotated timelines.

### Solution checks

Reject cancellation after PREPARED, recovery from temporary state alone, and duplicated effects after replay.
Reject premature terminal success while a required acknowledgement is missing.
Verify both the model suite and the production recovery scenario.

A controlled RPC failure does not prove process-restart behavior.
Record exactly which interruption each test simulates.
Keep full restart or eviction claims out until a dedicated fixture verifies them.

## Delivery order

Deliver P1 as soon as its lesson, tests, and solution are ready.
Use it to start learning and adjust explanations through this chat.
No recruitment, formal trial, or release gate across all three exercises is needed.

Build P2 and P3 later, alongside the relevant prerequisite lessons.
Keep the unfamiliar variations so we can check independent understanding.
Run and verify every complete solution before delivery.

You can discuss predictions, mistakes, and test results here.
Progress records, scored submissions, and separate evidence files are unnecessary.
