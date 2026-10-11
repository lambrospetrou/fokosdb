// On each key, a committed put or delete of a two-phase transaction has a timestamp above the stamp
// of every earlier committed operation of the key. A committed check of a two-phase transaction has
// a timestamp above the stamp of every earlier committed put or delete. A single-item write and a
// single-partition transaction add their stamps, but the rule does not apply to them, because MAX
// absorbs a clock that lags.
//
// A check of an absent item adds no stamp: the code keeps no watermark for it, and the locks order it
// against the writers of the key. Except for that case, the timestamp order is a serial order of the
// two-phase transactions.
spec WriteSerializable observes eItemWritten, eStamped {
  // The highest stamp of a put or a delete of each key.
  var writeStamp: map[tKey, int];
  // The highest stamp of a put, a delete, or a check of a present item, of each key.
  var readStamp: map[tKey, int];
  var present: set[tKey];

  start state Watching {
    on eItemWritten do (w: tItemWritten) {
      if (w.deleted) {
        present -= (w.key);
      } else {
        present += (w.key);
      }
    }

    on eStamped do (s: tStamped) {
      if (!(s.key in writeStamp)) {
        writeStamp[s.key] = 0;
        readStamp[s.key] = 0;
      }
      if (s.operation == OP_CHECK) {
        assert !s.twoPhase || s.ts > writeStamp[s.key],
          format("a check of key {0} committed at {1}, but a put or a delete of the key committed earlier at {2}",
            s.key, s.ts, writeStamp[s.key]);
        if (s.key in present) {
          readStamp[s.key] = Max(readStamp[s.key], s.ts);
        }
        return;
      }
      assert !s.twoPhase || s.ts > readStamp[s.key],
        format("a write of key {0} committed at {1}, but an operation of the key committed earlier at {2}",
          s.key, s.ts, readStamp[s.key]);
      writeStamp[s.key] = Max(writeStamp[s.key], s.ts);
      readStamp[s.key] = Max(readStamp[s.key], s.ts);
    }
  }
}
