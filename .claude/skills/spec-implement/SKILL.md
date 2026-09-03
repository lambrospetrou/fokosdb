---
name: spec-implement
description: Read a spec, RFC, or agent plan, clarify ambiguities, and implement the written milestones one by one. Pause between milestones for manual review, and never commit or push git changes.
---

# Spec implement

Implement a written specification through sequential milestones.

You implement the approved specification. You do not redesign the architecture.

## Rules

- **Never commit or push changes.** Do not run `git commit` or `git push`. Do not alter git history or branch state.
  The user creates all commits and pushes all changes.
- **Ask about ambiguities.** When a requirement or an edge case is unclear, stop. Ask the user immediately.
  Give concrete suggestions and options with trade-offs.
- **Implement one milestone at a time.** Focus only on the current milestone. Do not implement future milestones early.
- **Pause between milestones.** Stop after you complete each milestone. Show the changes and the test results.
  Wait for the user to review and approve the work before you continue.
- **Verify before pause.** Run the project build, type checks, and tests before you ask for review. All checks must pass.
- **Write minimal code.** Write only the code necessary to satisfy the milestone. Do not add unused abstractions.
- **No dead discussion references.** Comments must not refer to discussions, meeting labels, or rejected ideas.

## Procedure

### 1. Read the specification and project rules

Read the entire specification document first. Note all goals, constraints, and out-of-scope items.

Read `AGENTS.md` and `CLAUDE.md` at the repository root. Follow all repository rules and conventions.

Read the source files and the tests that the specification touches.

### 2. Extract the milestone list

List every milestone in the specification with:
- The milestone identifier and title.
- The deliverables and acceptance criteria.
- The test requirements.

Identify which milestone to implement first. Unless the user specifies another milestone, start with the first
incomplete milestone.

### 3. Check for ambiguities

Before you write code, examine the current milestone for ambiguities.

Check for:
- Missing error handling specifications.
- Incomplete interface definitions or type contracts.
- Unspecified edge cases or boundary conditions.
- Conflicts between the specification and existing code.

When you find an ambiguity:
1. Stop before you make changes.
2. Ask the user.
3. Provide two or three viable solutions with pros, cons, and your recommended option.
4. Wait for the user to choose.

### 4. Implement the milestone

Write the code for the current milestone only.

Follow these principles:
- Keep existing code style, naming patterns, and file structure.
- Add unit tests or integration tests that prove the new functionality works.
- Keep changes minimal, clear, and reliable.
- Do not modify files unrelated to the milestone.

### 5. Verify the milestone

Run the project verification commands:
- Run the build and the type check.
- Run the test suite. When repository rules specify how to run tests (such as in a subagent), follow those rules.
- Make sure that all new tests pass and all existing tests continue to pass.

Fix any test failures or type errors before you proceed.

### 6. Present the milestone report and pause

When all verification passes, stop. Do not commit changes.

Provide a concise summary in the chat using this format:

```markdown
## Milestone Complete: <Milestone ID> — <Milestone Title>

### Changes Made
- `<file-path>`: <brief description of changes>

### Verification
- Build: <Passed | Skipped with reason>
- Typecheck: <Passed | Skipped with reason>
- Tests: <command run and summary of results>

### Git Status
```
<short git status output>
```

### Notes for Reviewer
- <important observations or areas for specific review>

### Next Step
Waiting for manual review. When approved, I will proceed to <Next Milestone ID>.
```

Stop and wait for the user to review the code and reply.

### 7. Handle review feedback

When the user requests adjustments:
- Apply the requested changes.
- Run verification again.
- Present the updated summary and pause again for review.

When the user approves the milestone:
- Proceed to the next milestone and repeat the procedure from step 3.
