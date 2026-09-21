# Simple challenge template

Each challenge has one lesson, starter code, step tests, and a complete solution.
Use the [minimal setup](01-environment-and-delivery-plan.md).
The learner is you; questions and hints happen in this chat.

At the top of the lesson, list the difficulty, expected time, prerequisites, code links, and tested repository revision.
Use ordinary Markdown. No machine-readable metadata is needed.

## Writing style

Use clean, simple English.
Avoid fluff, filler, drama, emphatic language, and exaggeration.
For a complex topic, explain its simpler concepts first.
Use intuitive examples, then show how those concepts combine.
Keep the focus on learning and progression.

## 1. Challenge and learning outcomes

Describe the concrete problem and why it matters in FokosDB.
List three to five things you can demonstrate afterward.
For example: “Explain why an empty page can still require another request.”

Name the files to edit and provide the exact test commands.
Include a short recap of prerequisite concepts.
Early tasks can name one function. Later tasks leave the investigation to you.

## 2. Database and system learning

Assume no prior database knowledge.
Explain the concept intuitively, then show a small worked example.
Include intermediate states and a common misconception.

Ask for a prediction before running the example.
Connect the example to the actual FokosDB source.
Name the invariant and explain what would break it.

For concurrent operations, distinguish durable state, temporary state, messages, and caller-visible results.
Use a timeline when it helps.
Essential knowledge belongs in the lesson; external reading is optional.

The worked example must teach the concept without supplying the target algorithm.
Use a related problem or different setting so you still have reasoning to do.

## 3. Language learning

Explain the TypeScript features needed for this exercise.
Use short examples for primitives, idioms, types, and abstractions.
Introduce only what the task needs.

For example, teach promises and exceptions before an asynchronous operation.
Teach union narrowing before asking you to handle several result types.
Explain why the idiom fits, and show a plausible mistake.

Keep these examples separate from the challenge implementation.

## 4. Step-by-step test-driven challenge

For each step, supply:

1. One goal and a prediction question.
2. The behavior contract, including relevant boundaries.
3. The work to attempt without prescribing the algorithm.
4. The exact focused test command.
5. What a passing result establishes.

Group test names by step so Vitest can select them directly.
The starter must load; initial failures must come from unfinished behavior.
Run all exercise tests after a step when checking for regressions.

Ask you to add one meaningful boundary or regression test.
Discuss why it detects a plausible defect.
End with a small variation that changes the problem rather than merely renaming values.

You can explain your predictions and conclusions in this chat.
There are no required submission files or assessment tooling.

## Help and solution

Ask for hints here whenever you need them.
I start with the concept, an overlooked boundary, or a useful experiment.
I avoid giving away the implementation unless you request it.

Keep the complete answer in `solution.ts`, separate from the starter.
Larger exercises can include several complete solution files.
Include a short explanation of the solution and the limits of its tests.
If the follow-up needs different code, provide a verified solution for that too.

## Before I deliver the exercise

- Complete it myself through the documented steps.
- Run the same behavioral tests against the complete solution.
- Confirm that starter failures reflect the missing behavior.
- Check important edge cases and a plausible wrong approach.
- Verify the source links and exact test commands.
- Record the tested revision and result in a short lesson note.

No automated publication system or learner trial is required.
We improve clarity and time estimates as you work through the lessons.
