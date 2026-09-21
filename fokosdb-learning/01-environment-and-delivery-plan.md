# Minimal setup and next steps

This is a personal learning workspace for you.
Use Markdown, the existing FokosDB checkout, and its existing test tools.
Use this chat for hints, explanations, and review.

There is no learning framework to build first.

## Files and location

Keep the curriculum and lessons in `fokosdb-learning/` inside this repository.
Put exercise code beside the library tests so it can reuse their dependencies and configuration.

```text
fokosdb/
  fokosdb-learning/
    00-curriculum-overview.md
    01-environment-and-delivery-plan.md
    02-challenge-template.md
    03-pilot-specifications.md
    exercises/
      01-request-flow.md
  packages/fokosdb/test/learning/
    01-request-flow/
      challenge.ts
      challenge.test.ts
      solution.ts
```

Each lesson links to its code and gives the exact command to run.
Add a helper only when that exercise needs one.
For a larger task, list the source files to edit in the existing checkout.
Use ordinary Git commits or a branch when useful; no automatic checkout or reset system.

## Setup

Use the Node and pnpm setup already needed by FokosDB.
Install the repository dependencies if they are missing.
Run one existing focused test to confirm the environment works.
Fix setup issues together in this chat.

Reuse the repository's test runner and runtime configuration for the first exercises.
A pure function can use that runner too.
Add a separate configuration only if an actual exercise needs it.

Record the tested repository revision in the lesson as a short note.
If the code changes, inspect and update the affected lesson together.
No reference manifest, generated clone, or automatic version gate is needed.

## Your learning loop

1. Read the lesson and its examples.
2. Predict the next behavior.
3. Edit the challenge file.
4. Run the current step's test.
5. Discuss a question or failed test here when needed.
6. Run the complete exercise tests.
7. Try a small unfamiliar variation and explain the result here.

Use Vitest directly. For example, from the FokosDB repository root:

```sh
pnpm --filter fokosdb exec vitest run test/learning/01-request-flow/challenge.test.ts -t "step 1"
pnpm --filter fokosdb exec vitest run test/learning/01-request-flow/challenge.test.ts
```

These commands run the delivered [first exercise](exercises/01-request-flow.md).
Each delivered lesson must include commands verified against its actual files.
The focused command selects one step. Run the whole exercise to catch regressions.
Unfinished exercise tests can fail during a full repository test run; use focused runs while learning.

There are no custom learn or author commands.
There are no hint files, progress records, metadata schemas, generators, or learning-specific CI jobs.
Your explanations can stay in this chat. Written notes are optional.

## What I deliver for each exercise

- A self-contained lesson with the four agreed sections.
- Starter code and clear tests grouped by step.
- A complete, working solution file.
- A small follow-up problem to check independent understanding.
- A short note with the tested revision, commands, and results.

I complete the exercise myself before handing it over.
I run the same behavioral tests against the solution and check that the starter fails for the intended reason.
I check important edge cases and at least one plausible incorrect approach.
This is ordinary exercise verification, not a new authoring toolchain.
Solution verification must preserve your work and restore the normal test import afterward.

Tests check behavior. Our discussion checks the reasoning behind it.
When you ask for help, I start with a question or a small hint.
I provide the full answer when you ask for it.

## Next steps

1. Open the [request-flow lesson](exercises/01-request-flow.md); its code and solution are ready.
2. Work through it together and adjust the explanations to your needs.
3. Continue along the prerequisite path, writing the next useful exercise as we go.

The pagination and transaction-recovery specifications are later exercise plans.
They do not block the first lesson, and they do not need a shared pilot framework.

Keep the graduation goal: trace, predict, diagnose, and change FokosDB independently.
Spend the effort on explanations, examples, tests, and solutions.
