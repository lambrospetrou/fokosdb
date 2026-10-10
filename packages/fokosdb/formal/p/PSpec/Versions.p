// On each key, over all partitions, each new value has a v above every earlier v of the key, also
// after a delete and a recreate.
spec VersionIncreases observes eItemWritten {
  var maxV: map[tKey, int];

  start state Watching {
    on eItemWritten do (w: tItemWritten) {
      if (w.deleted) {
        return;
      }
      if (w.key in maxV) {
        assert w.v > maxV[w.key],
          format("key {0} got v {1}, but an earlier value of the key had v {2}", w.key, w.v, maxV[w.key]);
      }
      maxV[w.key] = w.v;
    }
  }
}
