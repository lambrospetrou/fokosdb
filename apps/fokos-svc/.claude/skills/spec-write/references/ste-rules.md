# ASD-STE100 Simplified Technical English

The rules that apply to a technical spec. They make a document short, plain, and
hard to misread.

## The core rules

1. **One word, one meaning.** Choose one word for a concept and use it everywhere.
   Do not vary the word for style. `partition` stays `partition`; it does not
   become `shard`, `node`, or `piece` later in the document.
2. **One meaning, one word.** Do not use one word for two things. If `commit`
   means both the 2PC step and the git action, rename one of them.
3. **Short sentences.** At most 20 words in a descriptive sentence. At most 25 in
   an instruction. One idea per sentence.
4. **Short paragraphs.** At most six sentences. A new topic starts a new
   paragraph.
5. **Active voice.** "The coordinator writes the row." Not "the row is written".
   Name the actor. Passive voice hides who does the work, and a spec is about who
   does the work.
6. **Present tense** for how the system behaves. Reserve `will` for future work.
7. **Simple tenses only.** Avoid `would have been`, `is being`, `has been going`.
8. **Articles are required.** "The parent forwards the write to the child." Not
   "parent forwards write to child".
9. **No -ing verb forms as nouns or clauses** where a simple form works. Write
   "the coordinator writes the state, then it sends the RPC", not "writing the
   state before sending the RPC".
10. **At most three nouns in a row.** Break up "partition split boundary byte
    seek". Write "the byte seek for the split boundary of a partition".
11. **Positive form.** "The write fails when the lock is held." Not "the write
    does not succeed unless the lock is not held". Never use two negatives.
12. **Same order, same words.** When two paragraphs describe the same kind of
    thing, use the same sentence pattern for both.
13. **Lists for steps and sets.** A sequence becomes a numbered list. A set
    becomes a bulleted list. Do not hide a list inside a paragraph.
14. **Say the condition first.** "When the partition is migrating, the write
    fails." The reader learns the case before the result.
15. **Give the number.** "Large", "fast", "soon", "many" are not specifications.
    Write the number and the unit, or write `TODO: measure`.

## Word replacements

| Do not write | Write |
| --- | --- |
| utilize, leverage | use |
| in order to | to |
| due to the fact that, owing to | because |
| in the event that, in case | if, when |
| prior to | before |
| subsequent to, following | after |
| at this point in time | now |
| a number of, several | the number, or "some" |
| perform a check | check |
| make a decision | decide |
| provide support for | support |
| is able to, has the ability to | can |
| it is necessary that | must |
| approximately | about |
| terminate | stop, end |
| commence, initiate | start |
| attempt | try |
| assist | help |
| require | need |
| additional | more, extra |
| ensure | make sure |
| obtain, acquire | get |
| eliminate | remove |
| implement (in prose about behavior) | do, build, write |
| in terms of, with respect to | for, about |
| basically, essentially, simply, just | (delete) |
| very, quite, extremely, significantly | (delete, or give the number) |
| note that, it is worth noting | (delete) |
| robust, elegant, clean, seamless | (delete, or say the property) |

## Modal verbs

Use the same three words for requirements, everywhere in the document:

- **must** — a requirement. Breaking it is a defect.
- **can** — an allowed option or a capability.
- **does not / must not** — a prohibition.

Do not use `should`, `may`, `might`, or `ought to` for a requirement. `should`
hides whether the rule is optional.

## What Simplified Technical English does not touch

Keep exactly as written:

- Code, commands, and fenced blocks.
- Identifiers, type names, table names, column names, and field names.
- Error strings and log messages in quotes.
- Titles of cited papers and documents.
- Standard technical terms: `idempotent`, `quorum`, `two-phase commit`,
  `serializable`, `alarm`, `Durable Object`. A technical name is one word with
  one meaning, which is what the standard asks for.

## Before and after

> Previously, we had been considering whether it might potentially be beneficial
> to leverage a mechanism whereby the coordinator would, prior to the dispatching
> of any outbound RPCs, persist its state — this was ultimately deemed necessary
> in order to ensure durability guarantees are not violated.

Becomes:

> The coordinator writes each state transition to SQLite before it sends an
> outbound RPC. This keeps the decision durable when the coordinator stops
> between the two steps.
