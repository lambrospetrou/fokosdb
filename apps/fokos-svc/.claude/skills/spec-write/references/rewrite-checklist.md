# Rewrite self-check

Run every check before you show the result. Fix what fails.

## Content

- [ ] Every technical fact from the source is in the new document, or in the
      removal list with a reason.
- [ ] No fact, number, date, name, or milestone that the source did not have.
- [ ] Every open question is in Open Questions. None was silently answered.
- [ ] Every reference points to a file that exists in the repository, or to a
      public URL. Run the check; do not assume.
- [ ] Each requirement uses `must`, `can`, or `must not`.

## Structure

- [ ] The section order matches `rfc-template.md`.
- [ ] The high-level overview is complete on its own. A reader who stops there
      understands the solution.
- [ ] Nothing in the technical details repeats the overview word for word.
- [ ] Each rule appears in one place. Other places refer to it.
- [ ] The appendix holds only secondary material.
- [ ] Optional sections with no content were removed, not left empty.

## Language

- [ ] No sentence is longer than 50 words
- [ ] Passive voice appears only where the actor is genuinely unknown.
- [ ] One name per concept, through the whole document.
- [ ] No word from the replacement table survives.
- [ ] No double negative.
- [ ] Every "fast", "large", "soon", or "many" has a number or a `TODO: measure`.
      For common things like network round trips add napkin math guesstimates.

## Removals

- [ ] No process narration, agent voice, or self-praise.
- [ ] No reference to a discussion label, a chat, or a plan that never shipped.
- [ ] No change log of the document itself.
- [ ] No emoji.
- [ ] The removal list in the summary covers every deletion.
