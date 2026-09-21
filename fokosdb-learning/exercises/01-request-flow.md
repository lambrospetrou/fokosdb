# 0.1 — Follow an item from a call to storage

Difficulty: medium. Allow 1–2 hours for reading, coding, and tracing the request.
Prerequisites: basic functions, objects, and terminal use. No database knowledge is assumed.

## 1. Challenge and learning outcomes

Build three functions for a small text store.
The database already exists. Your functions translate between its API and a smaller application interface.

After this exercise, you can:

- Identify an item using its complete key.
- Distinguish a missing item from an empty text value.
- Explain what changes when an existing item is written again.
- Delete one item without affecting a neighbouring key.
- Follow a read or write from the client into SQLite.

Open [challenge.ts](../../packages/fokosdb/test/learning/01-request-flow/challenge.ts) to write your code.
The supplied [tests](../../packages/fokosdb/test/learning/01-request-flow/challenge.test.ts) call a real local FokosDB instance.
They provide the setup and initial data.

You can ask questions in this chat at any point.
A hint can explain a concept or suggest an experiment without giving you the implementation.

### The interface you will implement

| Function                   | Result                        | Contract                                                                      |
| -------------------------- | ----------------------------- | ----------------------------------------------------------------------------- |
| `readText(db, key)`        | `{ text, version }` or `null` | Return the stored text and version. Return null only when the item is absent. |
| `writeText(db, key, text)` | A number                      | Store the text and return the database's version for that write.              |
| `removeText(db, key)`      | A boolean                     | Return whether this operation removed an item.                                |

An existing JSON or byte item is not a text item.
For this exercise, `readText` must reject it with a `TypeError`.
That is our adapter's policy, not a restriction on FokosDB.

Use the passed `db` object for storage.
Keep the supplied function signatures.
The functions must support any valid string hash key and an optional string sort key.

## 2. Database and system learning

### Start with an item

A database stores information so later operations can read or change it.
A key-value database associates a key with a value.
The stored entry is an item.

Think of a library shelf record:

| Key       | Value     |
| --------- | --------- |
| `shelf#6` | `History` |
| `shelf#7` | `Science` |

A read asks for the value at one key.
A write stores a value at that key.
A delete removes the item at that key.

The key and value have different jobs.
Changing a value does not change the key that identifies it.

A JavaScript variable keeps a value in memory.
FokosDB stores items in SQLite inside a Durable Object.
A Durable Object combines an addressable service with its own storage.
This exercise uses the local test runtime.

We will observe real database operations.
We will not restart the runtime, so these tests do not demonstrate recovery after a process restart.

### A FokosDB key has two parts

The `hashKey` is required.
The `sortKey` is optional.

For example:

| hashKey   | sortKey    | data           |
| --------- | ---------- | -------------- |
| `shelf#6` | `label`    | `History`      |
| `shelf#6` | `location` | `Second floor` |
| `shelf#7` | `label`    | `Science`      |

The first two items share a hash key.
They remain different items because their sort keys differ.
The first and third share a sort key but have different hash keys.

Think of the pair as an address:
the hash key names the shelf; the sort key names a particular record about it.

Changing the label for shelf 6 must leave its location unchanged.

The hash key helps the database choose a partition.
A partition stores part of the database.
Within a hash key, sort keys also provide order for queries.
We will study that order later. Here, we use the sort key to select one exact item.

When you omit the sort key, FokosDB encodes an absent-sort-key sentinel.
That means one specific key, not “all items under this hash key”.
Pass an omitted sort key through unchanged; you do not need to construct the sentinel.

### Missing, empty, and a different data kind

These states are different:

| State                                       | Meaning                                 |
| ------------------------------------------- | --------------------------------------- |
| No item at the key                          | There is nothing to read.               |
| Text item with `data: ""`                   | The item exists and its text is empty.  |
| JSON item with `data: { label: "History" }` | The item exists and holds a JSON value. |

FokosDB reports absence through `found`.
For a found item, `kind` describes the value.

Here are abbreviated results:

```ts
// No item at the requested key:
const absent = {
  found: false,
  item: { hashKey: "shelf#6", sortKey: "label" },
  meta: {},
};

// An existing empty text item:
const emptyText = {
  found: true,
  item: {
    hashKey: "shelf#6",
    sortKey: "label",
    kind: "text",
    data: "",
    version: 1,
  },
  meta: {},
};
```

These abbreviated examples omit the contents of metadata.
Real results include the database metadata.

Do not infer absence from the truthiness of the data.
In JavaScript, an empty string is falsy even when its item exists.

Our adapter returns `null` for absence.
FokosDB itself returns a result object with `found: false`.
The adapter translates between those interfaces.

A failed request is another case.
An invalid key or unavailable service is not the same as an absent item.
Let request errors reach the caller instead of converting them to null.

### Writes and versions

A successful first write creates the item with version 1.
A later successful put at that same key replaces its data and increments the version.

For one shelf label:

| Operation                 | Stored text afterward | Stored version |
| ------------------------- | --------------------- | -------------- |
| Put `History`             | `History`             | 1              |
| Get                       | `History`             | 1              |
| Put `Local history`       | `Local history`       | 2              |
| Put `Local history` again | `Local history`       | 3              |

The version counts these writes, not distinct text values.
A read does not increment it.

The database owns the version.
Your adapter returns the version from the write result.
Calculating it locally would require knowledge of every other writer.

A version is metadata about the stored item.
It is not a permanent identity across deletion and recreation.
We will examine that distinction in transaction exercises.

### Deletion

A delete result has a `deleted` field.
It is true when the operation removed an item.
It is false when that key had no item.

Deleting the same key twice can return true, then false.
The second result does not mean the database failed.
It means there was no item left to remove.

### The three public operations

These are the relevant API shapes:

```ts
db.getItem({ hashKey, sortKey });
// Result: found, item, meta.

db.putItem({ hashKey, sortKey, data });
// Result: item, version, meta.

db.deleteItem({ hashKey, sortKey });
// Result: item, deleted, meta.
```

The names above are variables representing the arguments.
Each call returns a promise.
Await the result before interpreting its fields.

The `meta` field describes work performed by the database.
Your adapter does not need to return it.
For example, storage metrics and the serving partition describe execution, not your text value.

### From call to storage

A successful write in this exercise follows this route:

```text
your function
    |
    v
FokosDB client: validate and encode the request
    |
    v
router: choose a partition
    |
    v
RPC: call the partition's method
    |
    v
PartitionDO: check whether it can handle the operation
    |
    v
PartitionStore: execute SQL in SQLite
    |
    v
result travels back to the caller
```

RPC means remote procedure call.
A stub lets the client call a method on an addressed Durable Object.
Even in a local test, that call crosses the runtime's RPC boundary.

SQLite stores data in tables.
A row is one record in a table.
The store uses SQL, a language for operations such as selecting, inserting, and deleting rows.

For example, this SQL reads one field from a matching row:

```sql
SELECT label FROM shelves WHERE shelf_id = 6;
```

`SELECT` names the fields to read.
`FROM` names the table.
`WHERE` describes which rows match.
FokosDB uses its item table and both encoded key fields for an exact read.

The client decides where to send the request.
The partition decides how to handle it.
The store contains the SQL.

The small datasets in this exercise do not trigger a split.
You will see migration and forwarding branches while reading the source.
Follow the ordinary local branch first. Those other branches are later topics.

The fixture creates a unique table name for each test.
That gives each test separate data without needing you to manage cleanup.

The repository already configures the test Worker and bindings.
Cloudflare's [testing documentation](https://developers.cloudflare.com/durable-objects/examples/testing-with-durable-objects/) explains the runtime approach.
Use the installed repository versions; this exercise does not need a dependency migration.

## 3. Language learning

### Object fields and spread

An object groups named values:

```ts
const destination = { room: "archive", cabinet: "B" };
const delivery = { ...destination, parcel: "maps" };
// delivery has room, cabinet, and parcel fields.
```

Object spread copies fields into a new object.
It is useful when an operation needs the existing address plus one new field.
It does not change the original object.

In `TextKey`, `sortKey?: string` means the property is optional.
A valid object can contain only `hashKey`.

### Async functions and await

An async function returns a promise.
The promise eventually resolves to a value or rejects with an error.

```ts
async function fetchTemperature(): Promise<number> {
  return 18;
}

const temperature = await fetchTemperature();
// temperature is 18, not a Promise.
```

A promise is not the completed result.
Use `await` when you need the result's fields.

A rejected promise causes `await` to throw.
Without a catch, the error travels to the caller.
That is appropriate when your function cannot recover from the failure.

### Narrowing a union

A union type permits several shapes.
A field can tell TypeScript which shape you have.

```ts
type Reading = { available: false } | { available: true; temperature: number };

function describeReading(reading: Reading): string {
  if (!reading.available) return "No reading";
  return `Temperature: ${reading.temperature}`;
}
```

After the early return, TypeScript knows the reading is available.
Accessing `temperature` there is safe.

For FokosDB, first consider `found`.
For a found item, consider `kind`.
Checking these fields is more useful than forcing a type with `as string`.
A cast does not validate a value at runtime.

### Types and local errors

The starter uses `import type` to name FokosDB as a parameter type.
It does not create a database. The test supplies the database object.

`Promise<TextSnapshot | null>` describes an eventual snapshot or null.
It does not permit returning the full database response.
The return type helps you check the interface before running tests.

A function can reject an unsupported input with a local error:

```ts
function double(value: unknown): number {
  if (typeof value !== "number") throw new TypeError("Expected a number");
  return value * 2;
}
```

The error explains a type mismatch.
Our adapter uses the same error class when an existing item has a non-text kind.

### Returning a smaller result

A function can expose only the fields its caller needs:

```ts
const measurement = { value: 18, unit: "C", sensor: "north" };
const display = { value: measurement.value, unit: measurement.unit };
```

Our text adapter follows that idea.
The caller needs the text and version, rather than the whole database response.

### Reading a test

```ts
await expect(fetchTemperature()).resolves.toBe(18);
```

The test waits for the promise, then checks the resolved value.
A rejected promise fails that assertion.

`toEqual` compares object contents.
`toBeNull` checks for null.
`rejects.toBeInstanceOf(TypeError)` checks the local error your adapter throws.

This TypeError is local application code.
Do not generalize that assertion to errors crossing RPC; those need the library's error guards.

A test also needs an independent observation.
After your adapter writes an item, the supplied test reads it through `db.getItem`.
A function that only returns a plausible version cannot pass that storage check.

## 4. The challenge

### Start

Run commands from the existing FokosDB repository root:

If dependencies are absent, run `pnpm install --frozen-lockfile` there.
No deployment or Cloudflare login is part of this exercise.

Edit [challenge.ts](../../packages/fokosdb/test/learning/01-request-flow/challenge.ts).
Its functions currently throw messages identifying the unfinished step.
Those failures are expected.

Each step command selects that step only.
Run the complete file after completing the functions to check their interaction.
A module-load error is a setup problem; paste it here so we can resolve it.

### Step 1 — Read an item

Before coding, predict these cases:

- A key with no item.
- A text item whose value is an empty string.
- A JSON item at the requested key.

Implement `readText` using the contract from section 1.
Return the stored version without calculating a replacement.
The non-text policy is a local `TypeError`.

Run:

```sh
pnpm --filter fokosdb exec vitest run test/learning/01-request-flow/challenge.test.ts -t "step 1"
```

Passing this step means you distinguish absence, text, and other stored kinds.
The tests seed items through FokosDB, so they do not depend on your write function.

### Step 2 — Write text

Predict the first version for a new key.
Implement `writeText`.
Use one put operation and return its reported version.
An omitted sort key must remain supported.

Run:

```sh
pnpm --filter fokosdb exec vitest run test/learning/01-request-flow/challenge.test.ts -t "step 2"
```

The test checks the stored value through the database directly.
Returning the input without writing it will fail.

### Step 3 — Replace text

Predict the version after writing the same text twice.
Predict whether a read between those writes changes it.

Run:

```sh
pnpm --filter fokosdb exec vitest run test/learning/01-request-flow/challenge.test.ts -t "step 3"
```

Your existing functions can already satisfy this step.
If they fail, examine where your version comes from.
You do not need a local counter or a separate “replace” operation.

Then find the SQL responsible for the increment in the store.
Explain why the database performs it instead of your adapter.

### Step 4 — Delete one item

Predict the results of deleting a missing key, an existing key, and that same key again.
Implement `removeText`.
Preserve the complete key.

Run:

```sh
pnpm --filter fokosdb exec vitest run test/learning/01-request-flow/challenge.test.ts -t "step 4"
```

The tests also keep a second item under the same hash key.
Explain why deleting the first item must preserve the second.

### Step 5 — Use what you learned

Do this prediction before running the next test.

There are three keys:

| Name       | hashKey    | sortKey |
| ---------- | ---------- | ------- |
| draft      | notebook#4 | draft   |
| title      | notebook#4 | title   |
| otherDraft | notebook#5 | draft   |

In order, the test:

1. Writes `rough` to draft.
2. Writes `Field notes` to title.
3. Writes `other` to otherDraft.
4. Writes `revised` to draft.
5. Deletes title.

Predict the final read result for each key, including its version when present.
Then run:

```sh
pnpm --filter fokosdb exec vitest run test/learning/01-request-flow/challenge.test.ts -t "step 5"
```

Add one test of your own in `challenge.test.ts`.
Choose an edge case and explain which incorrect implementation it would catch.
You can discuss your choice here before writing it.

Run the complete exercise:

```sh
pnpm --filter fokosdb exec vitest run test/learning/01-request-flow/challenge.test.ts
```

### Trace the implementation

Use these files as entry points:

- [Client](../../packages/fokosdb/src/client/db.ts)
- [Router](../../packages/fokosdb/src/shared/partition-topology/router.ts)
- [Partition](../../packages/fokosdb/src/server/do-partition.ts)
- [Store](../../packages/fokosdb/src/shared/partition/partition-store.ts)

Follow a put and a get. Answer in this chat or in your own notes:

1. Where does the client encode a key?
2. Which call crosses the RPC boundary?
3. Which method executes the write SQL?
4. Where does the version increase?
5. Which branch turns an absent row into `found: false`?
6. Why do the read and write results contain the caller's keys again?

You can search those files for public operation names and follow the calls.
You do not need to read each whole file.

For a final variation, trace the delete path without a supplied list of method names.
Explain which part of the full key reaches the delete statement.

Passing tests checks the adapter's behavior.
The trace checks whether you can connect that behavior to the implementation.

<details>
<summary>After your attempt: solution and explanation</summary>

The complete implementation is in [solution.ts](../../packages/fokosdb/test/learning/01-request-flow/solution.ts).

The read function separates absence from an existing value, then checks the kind.
The write function uses the version returned by the put.
The delete function uses the boolean returned by the delete.

In the current implementation, the public client wraps each operation and calls a private implementation.
The private implementation encodes keys, chooses a partition, and obtains a stub.
The stub calls `apiPutItem`, `apiGetItem`, or `apiDeleteItem`.

For a local put, the partition calls `PartitionStore.upsertItem` inside a storage transaction.
The SQL inserts version 1, or uses `v = v + 1` when the composite key already exists.
For a local get, `readItemLocally` calls the store and handles the absence of a returned row.
The client reconstructs public results using the caller's keys.

The transfer test ends with draft holding `revised` at version 2.
Title is absent.
OtherDraft still holds `other` at version 1.

This solution is not an atomic sequence across the three functions.
The exercise tests individual operations and ordered calls from one caller.
Concurrent workflows and transactions are later lessons.

</details>

### Verification note

Verified on 2026-09-21 against repository revision `c031ca2752dd1de530903e6e53b766fce688fae6`.
Local versions: Node 25.9.0, pnpm 11.25.0, Vitest 4.1.11, and TypeScript 5.9.3.

The existing sanity test passed.
The complete solution passed all 13 exercise tests.
The focused step 1 command passed its five tests.
The unfinished starter produced 13 expected failures from its placeholder functions.

The tests rejected two deliberate mistakes:
treating empty text as absent, and returning version 1 for every write.
The correct solution and the starter import were restored afterward.

The library type check passed with:

```sh
pnpm --filter fokosdb check
```

The three TypeScript files and this lesson were formatted with the repository's installed Prettier.
The full repository test suite was not run for this exercise-only addition.
Your normal test command imports `challenge.ts`; the solution remains separate.
