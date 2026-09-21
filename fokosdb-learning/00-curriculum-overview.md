# FokosDB learning curriculum

Status: [exercise 0.1 is ready](exercises/01-request-flow.md), with a verified solution. Other exercises remain planned.

The goal is independent understanding of FokosDB.
A graduate can trace a request, predict failure behavior, diagnose an unfamiliar defect, and make a tested change.
The graduate explains the invariants that the change preserves.

This plan combines the curriculum and its local learning environment.
The environment uses lessons, exercise files, and the existing FokosDB test setup.
You are the only intended learner. Use this chat for questions, hints, and review.
Web portals, accounts, hosted workspaces, dashboards, and certificates are outside this plan.

## Delivery documents

- [Environment and delivery plan](01-environment-and-delivery-plan.md): minimal files, direct test commands, and the next exercise.
- [Challenge template](02-challenge-template.md): the four teaching sections, tests, and working solutions.
- [Pilot specifications](03-pilot-specifications.md): plans for three initial exercises, delivered individually when useful.

These documents guide exercise creation. There are no custom learning commands to implement.
Each delivered exercise must include a tested, working solution.

## Reference code and maintenance

The learning files live inside the fork. The repository root is `../` from this document.
Its HEAD on 2026-09-20 is `c031ca2752dd1de530903e6e53b766fce688fae6`.
This records the earlier inspection, not a requirement to create another checkout.
Use the existing checkout and record its actual revision when verifying each exercise.

The code has changed since the first curriculum survey.
The query collector now exposes `createQueryPageCollector`.
The repartition code uses `RepartitionSource`, `RepartitionTarget`, and `FokosMigrationHost`.
I check each exercise against the current code before delivering it.
Existing architecture notes can lag behind the code.

Keep source links and the tested revision in the lesson.
If a code change breaks an exercise, update it together in this chat.

## Audience and entry requirements

Assume no database knowledge.
Assume the learner can edit files, run terminal commands, and read basic JavaScript.
A short readiness check covers functions, objects, arrays, exceptions, and promises.
F0 supplies a self-contained bridge when these skills need practice.

Teach database concepts before asking learners to reason about database behavior.
Teach TypeScript features beside the first exercise that needs them.
You can skip familiar material after trying the corresponding independent checkpoint.

## Graduation rubric

| Ability                  | Guided                                          | Independent in a familiar setting                                               | Independent in an unfamiliar setting                                                         |
| ------------------------ | ----------------------------------------------- | ------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Trace a request          | Follow supplied locations and label the stages. | Find the path and identify decisions, durable state, and response construction. | Trace a changed operation or topology and justify each branch with source evidence.          |
| Predict failure behavior | Complete a supplied event timeline.             | Explain persisted state, caller knowledge, and recovery after an interruption.  | Analyze a new interruption schedule and distinguish safety from eventual progress.           |
| Diagnose a defect        | Reproduce a supplied case with hints.           | Form hypotheses, collect evidence, and write a regression test before the fix.  | Localize a new defect without a named function or algorithm hint.                            |
| Make a tested change     | Complete a bounded implementation.              | Select affected modules and preserve their contracts.                           | Deliver a focused change with regression evidence and an explanation of affected invariants. |

Record each ability separately. A high score in one ability cannot replace another.
“Not yet demonstrated” is a valid result.
Graduation needs the unfamiliar-setting level in all four abilities.

Use the rubric as a personal learning target, not a formal certification process.
Discuss request traces, failure timelines, regression tests, and changes in this chat.
I review your reasoning and help identify what needs more practice.
Written notes and separate submissions are optional.

Documentation and repository search remain available during assessment.
Ask for hints freely; no hint tracking is needed.
After solution access, use a fresh variant to assess independent work.
A test pass establishes behavior for the checked cases. It does not establish mastery.

## Teaching method

Each learning unit follows this sequence:

1. Observe a small example.
2. Explain the underlying database and language concepts.
3. Predict a related case before execution.
4. Implement or repair a bounded behavior.
5. Run focused tests and interpret the result.
6. Add one learner-chosen regression or boundary test.
7. Explain the connection to production code.
8. Solve a transfer problem with fewer instructions.

A worked example uses different data or a different setting from the target task.
Hints come through this chat, starting with the concept or a useful experiment.
Keep the final implementation in a separate solution file.

Early exercises name the files and interfaces.
Later exercises provide symptoms and contracts; learners choose files and investigation steps.
Every exercise includes its own concept recap, starter, fixtures, and commands.
An optional completed starter supports exercises that build on earlier work.

“Self-contained” means the learner can access all required material within the exercise.
It does not mean every exercise has the same difficulty or needs no prior practice.

## Difficulty and time

| Grade      | Target active time   | Typical scope                                                |
| ---------- | -------------------- | ------------------------------------------------------------ |
| Easy       | Less than 30 minutes | One concept and one bounded task, with setup supplied.       |
| Medium     | 1–2 hours            | A feature slice, diagnosis, or small module.                 |
| Hard       | 2–4 hours            | A protocol, state machine, or interaction between modules.   |
| Super hard | 8–10 hours           | A bounded project with milestones and independent decisions. |

Time includes reading, implementation, tests, and the short explanation.
Track environment installation separately.
Estimates are provisional; adjust them as you work through each exercise.
Split tasks that exceed their band. Do not remove necessary teaching to meet a time target.

The original beginner estimate of two days is withdrawn.
Stage completion depends on evidence, not elapsed time.

## Database and language foundations

| Foundation                         | What the learner must explain                                                   | Where it first appears |
| ---------------------------------- | ------------------------------------------------------------------------------- | ---------------------- |
| Durable state and items            | A key identifies an item; a successful operation has a defined result.          | 0.1                    |
| Composite keys and access patterns | Hash and sort keys support different parts of an access pattern.                | 0.1, 1.1               |
| Bytes and order                    | An encoding defines comparison and prefix boundaries.                           | 1.1, 1.2               |
| SQL and storage                    | Rows, constraints, transactions, indexes, pages, and covering scans.            | F1, 2.1                |
| Expressions                        | A condition tests state; missing differs from null; values need types.          | F2                     |
| Concurrency and transactions       | Atomicity, isolation, conflicts, and uncertain responses are distinct concepts. | F3                     |
| Distributed recovery               | Local durable state and RPC completion are separate events.                     | F3, 6.5                |
| Partition changes                  | Ownership changes while requests and recovery continue.                         | 3.4, 3.6, 3.8          |
| Queries                            | Candidate scans, matches, result materialization, and continuation differ.      | 4.1, 4.5               |
| Background work                    | Logical expiry differs from physical cleanup; retries need idempotency.         | 2.4, 8.1               |

Teach arrays, maps, unions, narrowing, bytes, promises, and test assertions as needed.
Teach advanced types when reading their actual public API contracts.
Explain the guarantees FokosDB implements and the platform guarantees it relies on.
Use event timelines to explain concurrency; “single-threaded” alone is not a concurrency explanation.

## Core path and prerequisite map

An arrow means the preceding ability must be demonstrated or refreshed.
The challenge catalogue below gives the detailed exercise prerequisites.

| Stage                          | Learning sequence                            | Exit evidence                                                       |
| ------------------------------ | -------------------------------------------- | ------------------------------------------------------------------- |
| Entry                          | Readiness check → F0 when needed             | Run an assertion; explain a promise and an exception.               |
| Request flow                   | 0.1 → 0.2 → 0.3 → A1                         | Independently trace an operation and identify the storage boundary. |
| Keys and local storage         | 1.1 → 1.2 → F1 → 2.1 → 2.2 → 2.3             | Explain a key order, a constraint, and an index access path.        |
| Query behavior                 | 4.1 → 4.5 → 4.3 → 4.2 → A2                   | Diagnose pagination without using result count as completion.       |
| Conditions and local atomicity | F2 → F3 → 6.1 → 2.5 → 6.3                    | Predict a conflict and explain all-or-nothing effects.              |
| Distributed recovery           | 6.2 → 6.5 → 6.4 → 6.6 → A3                   | Explain durable decisions, replay, and uncertain outcomes.          |
| Partition changes              | 3.1 → 3.2 → 3.4 → 3.6 → 3.8 → 8.1 → 8.2 → A4 | Diagnose ownership and recovery during a partition change.          |
| Contribution                   | 7.1 → 7.2 → G                                | Deliver an unfamiliar repair and a bounded feature change.          |

P1 describes the first exercise, 0.1.
P2 and P3 describe later exercises, 4.5 and 6.5.
These labels identify lesson plans, not a pilot programme or tooling dependency.

A learner revisits earlier reasoning at each checkpoint.
For example, A3 revisits key identity and A4 revisits idempotent recovery.
Use a new variant after a later study session to check retention.
Do not repeat the original recipe as the delayed assessment.

## Core exercise catalogue

The entry check is the prerequisite when a row says “Entry”.
Prerequisites describe teaching order. Each exercise still contains a local recap and starting state.
Exercise 0.1 is available in the linked lesson above. Other entries remain planned.

| ID  | Challenge                                | Grade  | Prerequisites | Observable outcome                                                                       |
| --- | ---------------------------------------- | ------ | ------------- | ---------------------------------------------------------------------------------------- |
| F0  | JavaScript, TypeScript, and test bridge  | Medium | Entry         | Explain values, promises, union narrowing, and a failing assertion.                      |
| 0.1 | Hello FokosDB and trace one request (P1) | Medium | Entry or F0   | Use put/get/delete and connect the API to durable storage.                               |
| 0.2 | Where does my key live?                  | Easy   | 0.1           | Predict the root destination using the supplied hash helper.                             |
| 0.3 | Read the metadata                        | Easy   | 0.1           | Distinguish response counts, storage metrics, and forwarding.                            |
| 1.1 | Compare keys like SQLite                 | Easy   | 0.1           | Order supplied encoded keys; explain text, bytes, and the absent sentinel.               |
| 1.2 | Successor and separator                  | Medium | 1.1           | Derive prefix bounds and separate ordered keys; explain boundary cases.                  |
| F1  | SQL and local atomicity                  | Medium | 0.1           | Use constraints and rollback; explain rows, indexes, and pages with examples.            |
| 2.1 | A mini item store                        | Medium | F1, 1.1       | Build CRUD with composite uniqueness and version increments.                             |
| 2.2 | Count without fetching payloads          | Medium | 2.1           | Explain a covering query plan; use metrics as supporting evidence.                       |
| 2.3 | Measure an item in SQL                   | Medium | 2.1, 1.1      | Enforce the item-size contract for text, bytes, and JSON.                                |
| 4.1 | Eight operators, one interval            | Medium | 1.2           | Normalize bounds and identify empty intervals.                                           |
| 4.5 | Diagnose and repair a paginator (P2)     | Medium | 4.1, 0.1      | Continue through empty pages; preserve order and surface failures.                       |
| 4.3 | Candidate collection and page budgets    | Medium | 4.5           | Explain evaluated rows, matched rows, materialization, and stop cursors.                 |
| 4.2 | An honest cursor                         | Medium | 4.3           | Validate continuation identity and explain fields excluded from the fingerprint.         |
| F2  | Conditions and document values           | Medium | 2.1           | Distinguish missing, null, and typed values; use a condition without writing a compiler. |
| F3  | Concurrency and transaction foundations  | Medium | F1, F2        | Use event timelines to explain atomicity, isolation, retries, and uncertain outcomes.    |
| 6.1 | Move balances atomically                 | Medium | F3            | Attach conditions to updates and explain positional cancellation results.                |
| 2.5 | Watermarks that never go back            | Medium | F3            | Explain timestamp monotonicity when two clocks contribute writes.                        |
| 6.3 | The prepare check pass                   | Medium | 2.5, 6.1      | Predict acceptance from locks, conditions, and timestamp watermarks.                     |
| 6.2 | Fingerprint a transaction                | Medium | 6.1, 1.1      | Explain replay identity, field presence, and operation order.                            |
| 6.5 | The coordinator ledger (P3)              | Hard   | 6.2, 6.3      | Resume from persisted state and preserve the decision after PREPARED.                    |
| 6.4 | Read twice, trust once                   | Hard   | F3, 6.3       | Detect changed committed state, including delete/recreate cases.                         |
| 6.6 | Stale locks and recovery                 | Hard   | 6.5           | Distinguish recovery, cancellation, and quarantine using ownership and age.              |
| 3.1 | Hash a key down the tree                 | Easy   | 0.2, 1.1      | Explain per-level routing with deterministic examples.                                   |
| 3.2 | The opaque partition ID                  | Medium | 3.1           | Encode a supplied binary layout and explain why callers use helpers.                     |
| 3.4 | The split state machine                  | Hard   | F3, 3.2       | Preserve initialization and acknowledgement invariants under repeated events.            |
| 3.6 | Route through a split                    | Hard   | 3.4, 6.5      | Locate ownership, forwarding, and backpressure decisions in the real code.               |
| 3.8 | Promote a large hash key                 | Hard   | 3.6           | Explain cutover to range partitions and its interaction with pending transactions.       |
| 8.1 | One alarm, several jobs                  | Medium | 6.5, 3.4      | Choose the next required alarm and preserve retries after a failed job.                  |
| 8.2 | Hold a migration                         | Hard   | 3.6, 8.1      | Diagnose an unavailable partition and design a bounded, operation-aware retry.           |
| 7.1 | A new error code                         | Easy   | 0.3           | Add a contractual code and test its attributes.                                          |
| 7.2 | Errors across an RPC boundary            | Medium | 7.1, F3       | Preserve meaningful error fields and explain the caller's recovery choice.               |

For 6.1, attach an account condition to that account's update.
A separate check of the same key would violate transaction key uniqueness.
Teach that distinction explicitly.

For 4.3, check the collector interface in the current checkout.
The initial reference uses a consumer callback and delayed payload decoding.
Teach the collector separately from the page-level partition-visit budget.

## Specialization catalogue

These exercises extend the core path. They retain the original catalogue IDs where possible.

| ID   | Challenge                       | Grade      | Prerequisites      | Observable outcome                                                                      |
| ---- | ------------------------------- | ---------- | ------------------ | --------------------------------------------------------------------------------------- |
| 1.3  | The complement of a prefix      | Medium     | 1.2, 4.1           | Build ordered sub-queries and explain boundary cases.                                   |
| 1.4  | A tiny LRU cache                | Easy       | Entry or F0        | Explain eviction and Map order; identify whether the reference uses this utility.       |
| 2.4  | A budgeted TTL sweeper          | Hard       | 2.1, 8.1, 6.3      | Separate logical expiry from cleanup and preserve lock-related restrictions.            |
| 3.3  | A compact topology cache        | Medium     | 3.2                | Update a bounded tree cache and explain stale knowledge.                                |
| 3.5  | Where to cut a range            | Hard       | 1.2, 2.3, 3.8      | Choose non-degenerate boundaries from stored byte estimates.                            |
| 3.7  | Recover a repartition transfer  | Super hard | 3.8, 8.2, 6.6      | Resume transfer and acknowledgement from durable checkpoints.                           |
| 4.4  | Walk a range tree               | Hard       | 4.2, 4.3, 3.8      | Preserve continuation and direction across several children.                            |
| 5.1  | Validate a document path        | Medium     | F2                 | Parse a bounded grammar and reject malformed or excessive paths.                        |
| 5.2  | A type checker for conditions   | Medium     | 5.1                | Derive value types and explain incompatible operations.                                 |
| 5.3  | Compile a condition to SQL      | Hard       | 5.2, F1            | Bind literals safely and preserve missing/null semantics.                               |
| 5.4  | Rebuild a projected item        | Medium     | F2, 4.3            | Decode positional values while distinguishing missing and null.                         |
| 5.5  | Extend an expression registry   | Hard       | 5.3, 5.4           | Carry a specified operation through validation, compilation, and runtime tests.         |
| 5.6  | Extend updates with numeric add | Super hard | 5.5, 6.5           | Specify absent-value and type behavior, then test updates through transactions.         |
| 6.7  | A single-item update API        | Super hard | 5.3, 6.5, 3.6, 7.2 | Preserve conditions, routing, and errors across a public API change.                    |
| 9.1  | A Bloom filter                  | Medium     | 1.1, 3.1           | Demonstrate no false negatives for retained inserts and measure false positives.        |
| 9.2  | Layers instead of resizing      | Hard       | 9.1, 3.8           | Preserve membership through growth and snapshot restoration.                            |
| 10.1 | A list endpoint                 | Medium     | 4.5, 7.2           | Map validation and pagination contracts to an HTTP interface.                           |
| 10.2 | A transactional secondary index | Super hard | 6.1, 6.2, 4.5      | Preserve a defined index invariant during create, rename, delete, and competing writes. |

For 3.7, use the current repartition interfaces, not the removed SplitMigration design.
For 5.3, teach standalone condition bindings first; compare query pool bindings afterward.
For 5.6, define FokosDB numeric-add semantics explicitly. Array and set operations are outside its first scope.
For 10.2, keep session expiry and load tests as separate extensions.
A concurrency test supplies evidence for an invariant; it does not prove every possible execution.

## Independent checkpoints

| ID  | Grade      | Prerequisites       | Assessment                                                                                |
| --- | ---------- | ------------------- | ----------------------------------------------------------------------------------------- |
| A1  | Medium     | 0.2, 0.3            | Trace an unfamiliar delete or conditional write without supplied function names.          |
| A2  | Medium     | 4.2, 4.3            | Diagnose a new paging defect and write a regression test.                                 |
| A3  | Hard       | 6.4, 6.5, 6.6       | Predict persisted state and recovery under a new interruption schedule.                   |
| A4  | Hard       | 3.8, 8.2, A3        | Investigate cancellation after split completion or a lost migration acknowledgement.      |
| G   | Super hard | A1, A2, A3, A4, 7.2 | Repair an unfamiliar defect and deliver a bounded feature change with invariant evidence. |

G has separate milestones for reproduction, diagnosis, regression, implementation, and review.
The author chooses a defect and feature whose combined scope fits the 8–10 hour target.
The learner receives contracts and fixtures, but no named faulty function.
Use this chat to review all four abilities; successful code alone is insufficient.

## Exercise delivery

Start with [exercise 0.1](exercises/01-request-flow.md).
It includes the lesson, starter, tests, and verified solution using the existing test setup.
Use your questions and experience to improve the next exercise.

P2 follows the query prerequisites. P3 follows the transaction prerequisites.
They do not need to be completed before you start learning.

Keep the rubric, core path, and independent variations.
Remove infrastructure that does not directly help you study or verify an exercise.
The [minimal setup](01-environment-and-delivery-plan.md) defines the files and direct test workflow.
