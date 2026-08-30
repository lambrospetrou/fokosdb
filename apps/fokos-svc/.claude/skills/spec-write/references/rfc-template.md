# RFC template

The structure from https://www.lambrospetrou.com/articles/rfc-template/, written
for a markdown file in this repository.

The RFC is a document for people. A reviewer who does not know the system must be
able to read it from the top and understand why the change is worth doing.

---

## Section order

1. Title and Reviewers
2. Table of Contents
3. Overview and Context
4. Goals and Requirements
5. Timeline and Milestones
6. Proposed Solution
7. Alternative Options (optional)
8. Frequently Asked Questions
9. Appendix (optional)

---

## 1. Title and Reviewers

The title says what the change is, not that a document exists.

The state is one of: `Draft`, `Under-review`, `Approved`, `In-progress`,
`Completed`. Keep the state current.

For a document inside this repository, the reviewer table is optional. Keep the
title, the state, the date, and the References list.

```markdown
# RFC — <what the change is>

**State:** Draft
**Date:** YYYY-MM-DD
**Author:** <name>

| Reviewer | Team | Status | Date |
| --- | --- | --- | --- |
| <name> | <team> | not-reviewed | |

References:

- `docs/agent-plans/<date>-<file>.md`
- [<title>](<url>)
```

## 2. Table of Contents

Include it when the document has more than about five sections. A markdown list
of the headings is enough.

The list here should be hyperlinks to each section heading.

## 3. Overview and Context

Describe the problem the RFC solves. Give the context a reader without prior
knowledge needs to see the benefit of the change.

Answer three things:

- What is wrong today, with evidence. A number, a limit reached, an incident, a
  customer request.
- Why it must be solved now.
- What the reader must know about the current system to follow the proposal.

Optional subsections:

- **Glossary** — only for terms this document uses in a specific way.
- **Customer or business impact** — the evidence for the priority.

## 4. Goals and Requirements

Two explicit lists.

- **In scope** — each goal is a statement that can be true or false when the work
  is finished.
- **Out of scope** — the work a reader will expect and will not get, with the
  reason.

Requirements that constrain the solution (a latency budget, a compatibility rule,
an operational limit) belong here, not in the solution.

## 5. Milestones

The key milestones. Update this section as the work moves.

Give the implementation order when the change ships in parts, and say what each
part delivers on its own.

## 6. Proposed Solution

Two subsections. The split is the point of the template.

### 6.1 High-level overview

Complete on its own. A reader who stops at the end of this subsection must
understand the solution, not a vague description of it. Written for everyone,
including a reader who does not work on this system.

One or two diagrams. Keep the diagrams in the document, in a fenced block.

### 6.2 Technical details

The implementation. Use subsections, diagrams, tables, and code. Cover:

- The data model and every schema change.
- Each operation: inputs, outputs, errors, and which errors the caller retries.
- Each state machine: the states, the transitions, and the trigger for each.
- The invariants, and the mechanism that holds each one.
- The failure and recovery paths.
- Concurrency: what runs at the same time, and what protects it.
- The tradeoff at each decision, with the reason. A decision without a reason is
  not a decision.
- Performance: the cost of the common path, and how it grows.
- The deployment, migration, and rollback plan.
- Testing: what proves the change is correct.

### 6.3 Open Questions (optional)

Each open question gets a subsection here.

Unresolved items that do not block approval. One question per item, with the
options and what the answer changes. Move each answer into the body when it is
resolved, and delete the question.

## 7. Alternative Options (optional)

Each option that was considered and rejected, or that is different enough to be
worth recording. For each: what it is, and why it was not chosen. Keep it short.
No account of the discussion.

## 8. Frequently Asked Questions

The questions a reviewer will ask, answered before the review. Add each question
that comes up during the review, with its answer, so it is asked once.

## 9. Appendix (optional)

Secondary material: a long diagram, raw data, a detailed table. Keep it small. An
appendix is not a place to move content that is too messy to write properly.

---

## Adaptations for this repository

- An ADR under `docs/adr/` keeps its own format: Title, Date, Status, Context,
  Decision, Consequences. Do not convert an ADR into an RFC unless the user asks.
- A document under `docs/agent-plans/` or `docs/ideas/` uses the RFC structure. A
  `Status` line that says what is built and what is not is more useful here than
  a reviewer table.
- Use the existing heading numbering (`## 1. Goal and scope`) when the document
  already numbers its sections.
- Refer to code with backticked paths: `src/lib/do-partition.ts`.
