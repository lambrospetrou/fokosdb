---
name: spec-review
description: Review a spec, RFC, design doc, or agent plan for correctness, consistency of transactions and operations, performance, reliability, and availability. Use when the user asks to review, critique, audit, poke holes in, or sign off on a document under docs/ (agent-plans, adr, ideas), or gives a design document before implementation starts.
---

# Spec review

Review a written design against the code that must implement it. Find the defects
that become bugs, data loss, or an outage after the spec ships.

You review. You do not rewrite the document and you do not implement it. If the
user wants the document reformatted, use the `spec-rewrite` skill instead.

## Rules

- Read the code before you judge the spec. A spec is wrong when it disagrees with
  the system it changes, not when it disagrees with your memory.
- Every finding must name a failure. Give the sequence of events, the state, and
  the wrong result. If you cannot write that sequence, the finding is not a finding.
- Quote the spec. Give the section number or the heading, and the sentence.
- Do not pad the report. No praise, no summary of the spec back to the author, no
  findings that only say "consider adding more detail".
- Do not invent a requirement the spec does not have. A spec is allowed to leave
  work out of scope.
- Use Simplified Technical English. Short sentences. Active voice. One idea per
  sentence.

## Procedure

### 1. Read the spec fully

Read the document from start to end before you look at any code. Note the scope
section: findings about out-of-scope work are not defects.

### 2. Read the system

Read `AGENTS.md` and `CLAUDE.md` at the root of the service. Then read the source
files, tests, and prior documents that the spec changes or depends on. Follow the
References list in the spec. Look for prior documents that the spec contradicts.

Do not skip this step for a "small" spec.

### 3. Extract the claims

Write a short private list of:

- **Operations** the spec adds or changes, and their inputs and outputs.
- **State machines**: each state, each transition, and what causes it.
- **Invariants** the spec states or assumes.
- **Persistence points**: what is written, in what order, and before which
  outbound call.
- **Failure assumptions**: what the spec expects when a call fails, times out, or
  is retried.

This list is your model. The review compares the model against the code and
against itself.

### 4. Run the review passes

Read `references/review-checklist.md` and run all five passes:

1. Correctness
2. Consistency of transactions and operations
3. Performance
4. Reliability
5. Availability

Then run the FokosDB hazards pass in the same file. That pass holds the failure
modes that this codebase repeats.

### 5. Verify each candidate finding

For each candidate, do one of:

- Find the code, the test, or the spec sentence that proves the defect. Mark it
  **Confirmed**.
- Fail to prove it, but keep a concrete failure sequence. Mark it **Plausible**
  and say what you could not check.
- Fail to write a failure sequence. Delete it.

Delete any finding the spec already answers in a section you had not read.

### 6. Write the report

Use the format below. Order by severity, then by section number.

```markdown
## Spec review: <document title>

**Verdict:** <one of: ready to implement | ready after the blockers are fixed | needs a rework>
**Scope reviewed:** <sections and code you read>

### Blockers
#### B1. <one-line defect> — §<section> [Confirmed|Plausible]
**Claim:** "<quote from the spec>"
**Failure:** <the sequence of events and the wrong result>
**Evidence:** <file:line, test name, or spec section that proves it>
**Fix direction:** <one or two sentences; do not design the fix>

### Major
#### M1. ...

### Minor
#### N1. ...

### Questions for the author
- Q1. <a question that changes the design, not a request for prose>
```

Severity:

- **Blocker** — data loss, lost or leaked locks, a broken invariant, an operation
  that cannot be made correct as written, or an outage under normal load.
- **Major** — correct in the normal path, wrong under a failure, a retry, a
  concurrent operation, or a load spike.
- **Minor** — a limit, a cost, or a gap that the team can accept with a note.
- **Question** — the answer changes the design.

If a pass finds nothing, say so in one line. Do not manufacture findings to fill
a section.

### 7. Offer the next step

End with one line: offer to open the blockers as edits to the spec, or to run
`spec-rewrite` if the document also needs reformatting. Do not start either
without the user's word.
