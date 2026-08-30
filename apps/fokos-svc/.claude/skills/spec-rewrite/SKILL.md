---
name: spec-rewrite
description: Rewrite a spec, plan, design doc, or RFC into the project RFC format and into ASD-STE100 Simplified Technical English, and remove commentary that belongs to the discussion instead of the spec. Use when the user asks to rewrite, reformat, clean up, tighten, or "make an RFC" out of a document under docs/ (agent-plans, adr, ideas), or when a generated plan must become a reviewable spec.
---

# Spec rewrite

Turn a document into a spec a reviewer can approve: the project RFC structure,
Simplified Technical English, and nothing in it except the specification.

You rewrite the form. You do not change the design.

## Rules

- **Keep every technical fact.** Move it, rename it, shorten it. Do not delete it
  because it is inconvenient or because you disagree with it.
- **Do not decide open questions.** An unresolved question moves to Open
  Questions. It does not become a decision.
- **Do not invent.** No new requirement, no new number, no new milestone, no
  invented reviewer, no invented date. If a required section has no source
  content, write the heading and `TODO: <what the author must supply>`.
- **Report what you cut.** Every removal is listed in the summary you give the
  user.
- **One document, one voice.** The finished spec reads as if one author wrote it
  in one sitting.
- **No moving target code references.** If you have to reference code, refer to file and function names,
  not to a line number or a commit hash. The code may change, but the function name is stable.
- **Use 120 characters per line.** If a sentence is longer, break it into two or more sentences.

## Procedure

### 1. Read and inventory

Read the whole document. Then sort each block into one of four piles:

- **Spec content** — it describes the problem, the goal, the solution, a
  tradeoff, a rejected option, or a question.
- **Misplaced content** — spec content under the wrong heading.
- **Non-spec content** — see "What to remove" below.
- **Unclear** — you cannot tell what it means. Never delete this pile. Keep the
  text, mark it, and ask the author.

### 2. Build the skeleton

Read `references/rfc-template.md`. Create the section order it gives. Keep the
optional sections only when there is content for them.

Preserve the document's own numbering style when it already numbers sections, and
keep the existing file name unless the user asks for a new one.

### 3. Place the content

Move each block into its section. Merge duplicates: when the same rule appears
three times, keep the clearest statement in the section that owns it, and let the
other places reference it.

Order inside a section: the rule first, then the reason, then the example.

### 4. Rewrite the language

Read `references/ste-rules.md` and apply it sentence by sentence.

Keep code, identifiers, table names, SQL, field names, and quoted error strings
exactly as they are. Simplified Technical English applies to prose, not to code.

### 5. Remove the non-spec content

Delete the pile from step 1. Keep the list of what you deleted.

### 6. Self-check

Run every check in `references/rewrite-checklist.md` before you show the result.

### 7. Write the file and report

Write the rewritten document. Then give the user a short summary:

```markdown
**Structure:** <sections added, renamed, merged, or reordered>
**Removed:** <each removed block, one line each, with the reason>
**Needs the author:** <each TODO and each unclear block, with the question>
```

Keep the summary in the chat. Do not put a change log inside the spec.

## What to remove

Delete all of this. It is discussion, not specification.

- **Process narration.** "As discussed above", "in the previous iteration", "we
  first tried", "after feedback", "per the earlier analysis", "updated after
  review".
- **Dead references.** Labels such as W1, W2, Option B, Phase 0, report XYZ, a
  chat session, or a plan that never shipped. A reference must point to a
  document in the repository or a public URL. Nothing else.
- **Agent voice.** "I have now added", "let me know if", "hope this helps", "note
  that I assumed", "TODO for me", and any second person address to the reader.
- **Self-praise and marketing.** "elegant", "robust", "blazing fast",
  "production-ready", "comprehensive", "seamless", emoji, and bold text used for
  excitement.
- **Padding.** A summary of the section the reader just read, a restatement of
  the title, "in this section we will", and a conclusion that adds nothing.
- **Stale status text.** Old completion percentages, checkboxes for work already
  done, and a change log of the document itself. Version history belongs in git.
- **Uncited claims.** A benchmark number, a limit, or a quota with no source.
  Either cite the source page, or mark it `TODO: measure`.
- **Rejected designs told as a story.** Keep the decision and the reason, in
  Alternative Options. Delete the account of how the team got there.

Keep, always:

- Every constraint, invariant, limit, and failure case.
- Every tradeoff and the reason for it.
- Every open question.
- Every reference to a real file, document, or paper.

## When the source is not one document

If the user gives several documents or a chat transcript, do the same procedure
across all of it. Say in the summary which source each section came from, and
name any conflict between the sources instead of choosing a side.
