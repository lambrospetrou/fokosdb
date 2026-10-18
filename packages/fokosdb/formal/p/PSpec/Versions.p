// On each key, over all partitions, each revision is above every earlier revision of the key, also
// after a delete and a recreate. A new value has its v as its revision, and the delete of a present
// row has the delete revision v + 1. The delete of an absent key announces v = 0 and has no revision.
spec VersionIncreases observes eItemWritten {
  var maxV: map[tKey, int];

  start state Watching {
    on eItemWritten do (w: tItemWritten) {
      if (w.deleted && w.v == 0) {
        return;
      }
      if (w.key in maxV) {
        assert w.v > maxV[w.key],
          format("key {0} got revision {1}, but an earlier revision of the key was {2}", w.key, w.v, maxV[w.key]);
      }
      maxV[w.key] = w.v;
    }
  }
}
