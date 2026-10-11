// The effect of one writer on one key: a put of `value`, or a delete.
type tWriteEffect = (deleted: bool, value: tValue);

// The result of a read transaction equals the state after some prefix of some serial order of the
// writers. A writer is a write transaction with the decision commit, a single-partition transaction,
// or a single-item write. A serial order keeps, on each key, the order in which the partitions
// applied the writers. A key that a writer has not applied yet comes after every applied write of
// the key, and its value is not known yet, so no read result can match it. The prefix holds every
// transaction whose client got committed before the read started. Each test case starts with no
// item.
//
// The writes of a single-partition transaction and of a single-item write carry no transaction id,
// so each of them counts as one writer of one key. For a single-partition transaction, the monitor
// then accepts more orders than the code can give.
spec ReadSerializable observes eDecision, eItemWritten, eClientAnswer, eReadStart, eReadResult {
  // The keys that each writer puts or deletes.
  var writeKeys: map[int, set[tKey]];
  // The writers of each key, in apply order, and the effect of each one.
  var applied: map[tKey, seq[int]];
  var effects: map[(writer: int, key: tKey), tWriteEffect];
  var committedAnswers: set[int];
  // The transactions that answered committed before each read transaction started.
  var mustSee: map[(client: machine, reqId: int), set[int]];
  // The id of the last writer with no transaction id. Transaction ids are above 0.
  var lastAnonymous: int;

  start state Watching {
    on eDecision do (d: tDecision) {
      if (d.commit && sizeof(d.writeKeys) > 0) {
        writeKeys[d.tx] = d.writeKeys;
      }
    }

    on eItemWritten do (w: tItemWritten) {
      var writer: int;
      writer = w.tx;
      if (writer == 0) {
        lastAnonymous = lastAnonymous - 1;
        writer = lastAnonymous;
      }
      if (!(writer in writeKeys)) {
        writeKeys[writer] = default(set[tKey]);
      }
      writeKeys[writer] += (w.key);
      if (!(w.key in applied)) {
        applied[w.key] = default(seq[int]);
      }
      applied[w.key] += (sizeof(applied[w.key]), writer);
      effects[(writer = writer, key = w.key)] = (deleted = w.deleted, value = w.value);
    }

    on eClientAnswer do (a: tClientAnswer) {
      if (a.answer == ANSWER_COMMITTED) {
        committedAnswers += (a.tx);
      }
    }

    on eReadStart do (r: tReadStart) {
      if (r.transaction) {
        mustSee[(client = r.client, reqId = r.reqId)] = committedAnswers;
      }
    }

    on eReadResult do (r: tReadResult) {
      var writers: seq[int];
      var writer: int;
      var placed: seq[int];
      if (!r.transaction) {
        return;
      }
      foreach (writer in keys(writeKeys)) {
        writers += (sizeof(writers), writer);
      }
      assert Search(writers, placed, default(set[int]), r, mustSee[(client = r.client, reqId = r.reqId)]),
        format("read transaction {0} of client {1} returned {2}, which no prefix of a serial order of the writers {3} gives",
          r.reqId, r.client, r.results, applied);
    }
  }

  // A depth-first search over the prefixes of the serial orders. `placed` is the prefix, and
  // `inPrefix` holds the same writers.
  fun Search(writers: seq[int], placed: seq[int], inPrefix: set[int], r: tReadResult, must: set[int]): bool {
    var writer: int;
    var longer: seq[int];
    var longerSet: set[int];
    if (Matches(placed, inPrefix, r, must)) {
      return true;
    }
    foreach (writer in writers) {
      if (!(writer in inPrefix) && CanPlace(writer, writers, inPrefix)) {
        longer = placed;
        longer += (sizeof(longer), writer);
        longerSet = inPrefix;
        longerSet += (writer);
        if (Search(writers, longer, longerSet, r, must)) {
          return true;
        }
      }
    }
    return false;
  }

  // A writer can come next when every writer that precedes it on one of its keys is in the prefix.
  fun CanPlace(writer: int, writers: seq[int], inPrefix: set[int]): bool {
    var key: tKey;
    var other: int;
    var at: int;
    var otherAt: int;
    foreach (key in writeKeys[writer]) {
      at = ApplyIndex(writer, key);
      foreach (other in writers) {
        if (other != writer && !(other in inPrefix) && key in writeKeys[other]) {
          otherAt = ApplyIndex(other, key);
          if (otherAt >= 0 && (at < 0 || otherAt < at)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  // The position of the writer in the apply order of the key, or -1 when it has not applied the key.
  fun ApplyIndex(writer: int, key: tKey): int {
    var i: int;
    if (!(key in applied)) {
      return -1;
    }
    i = 0;
    while (i < sizeof(applied[key])) {
      if (applied[key][i] == writer) {
        return i;
      }
      i = i + 1;
    }
    return -1;
  }

  // The prefix holds every writer that the read must see, and gives each value of the read.
  fun Matches(placed: seq[int], inPrefix: set[int], r: tReadResult, must: set[int]): bool {
    var tx: int;
    var result: tReadValue;
    var i: int;
    var last: int;
    var effect: tWriteEffect;
    foreach (tx in must) {
      if (tx in writeKeys && !(tx in inPrefix)) {
        return false;
      }
    }
    foreach (result in r.results) {
      last = 0;
      i = 0;
      while (i < sizeof(placed)) {
        if (result.key in writeKeys[placed[i]]) {
          last = placed[i];
        }
        i = i + 1;
      }
      if (last == 0) {
        if (result.found) {
          return false;
        }
      } else {
        if (!((writer = last, key = result.key) in effects)) {
          return false;
        }
        effect = effects[(writer = last, key = result.key)];
        if (result.found == effect.deleted || (result.found && result.value != effect.value)) {
          return false;
        }
      }
    }
    return true;
  }
}
