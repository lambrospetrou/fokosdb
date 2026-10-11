// The root partition of each hash key. A hash split of a root moves each of its hash keys to one of
// its hashSplitN children.
type tTopology = map[string, machine];

fun RootOf(topology: tTopology, key: tKey): machine {
  return topology[key.hk];
}

fun HASH_SPLIT_N(): int { return 2; }

// The child index of a hash key in a hash split: hash key c goes to child 1, and every other hash key
// to child 0.
fun ChildIndexOf(hk: string): int {
  if (hk == "c") {
    return 1;
  }
  return 0;
}
