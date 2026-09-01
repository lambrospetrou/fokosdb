# ADR 000: Record Architecture Decisions

Date: 2026-06-20

## Status

Accepted

## Context

Architecture decisions are made throughout the life of a project. Without a lightweight record of what was decided, why, and what the known trade-offs are, the reasoning behind the current design is lost. New contributors reverse-engineer intent from code, and past mistakes get repeated because no one remembers why an alternative was rejected.

We need a format that is easy to write, easy to find, and durable across refactors. It should live in the repository next to the code it describes, not in an external wiki or document system.

## Decision

We will record significant architecture decisions in Architecture Decision Records (ADRs), following the format described by Michael Nygard in [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

Each ADR is a short Markdown file in `docs/adr/`, numbered sequentially: `000-record-architecture-decisions.md`, `001-query-items-operation.md`, etc.

### When to write an ADR

Write an ADR when you make a decision that:

- Affects the structure of the system (new components, changed data flow, new protocol between services).
- Involves a non-obvious trade-off where a reasonable person could have chosen differently.
- Has consequences that are hard to reverse without understanding why the choice was made.

Do not write an ADR for routine implementation choices, library upgrades, or bug fixes unless they change the architecture.

### Format

Every ADR uses these sections:

- **Title**: `ADR NNN: Short Descriptive Title`. The title should name the decision, not the problem.
- **Date**: when the decision was made or last revised.
- **Status**: one of `Proposed`, `Accepted`, `Deprecated`, or `Superseded by [ADR NNN](NNN-title.md)`.
- **Context**: the forces at play — what problem we faced, what constraints existed, why we needed to decide. Write for a reader who has no prior context.
- **Decision**: what we chose and how it works. This is the bulk of the document. Describe the design at a level that remains valid across refactors: components, flows, invariants, key algorithms, and the reasoning behind non-obvious choices. Include inline code snippets when they clarify a type contract or algorithm, but do not reference specific file paths or line numbers.
- **Consequences**: what follows from the decision — both positive outcomes and known limitations, deferred work, or risks. Be honest about what this does not solve.

### Conventions

- Keep ADRs immutable once accepted. If a decision is reversed, write a new ADR that supersedes the old one and update the old ADR's status to `Superseded by [ADR NNN](NNN-title.md)`.
- Write in present tense for the decision ("We use X", not "We will use X") and past tense for the context ("Before this change, the system had no way to...").
- One decision per ADR. If a single piece of work involves multiple independent decisions, write separate ADRs.
- Numbers are never reused. A deprecated or superseded ADR keeps its number.

### Template

New ADRs should follow this skeleton:

```markdown
# ADR NNN: Title

Date: YYYY-MM-DD

## Status

Proposed | Accepted | Deprecated | Superseded by [ADR NNN](NNN-title.md)

## Context

What is the issue that we're seeing that is motivating this decision or change?

## Decision

What is the change that we're proposing and/or doing?

## Consequences

What becomes easier or more difficult to do because of this change?
```

## Consequences

Architecture decisions are captured close to the code and versioned alongside it. New contributors can read the `docs/adr/` directory to understand why the system is shaped the way it is, without needing to archaeology through commit history or ask the original authors.

The cost is small: one short document per significant decision. The format is deliberately lightweight to keep the barrier to writing low.
