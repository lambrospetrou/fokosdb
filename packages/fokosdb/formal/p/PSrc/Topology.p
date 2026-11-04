// The root partition of each hash key. The code selects the root by the first digit of the
// routingHash of the hash key, so all items of one hash key are on one root. A hash split of a root
// moves each of its hash keys to one of its hashSplitN children.
type tTopology = map[string, machine];

fun RootOf(topology: tTopology, key: tKey): machine {
  return topology[key.hk];
}

fun HASH_SPLIT_N(): int { return 2; }

// The child index of a hash key in a hash split: hash key c goes to child 1, and every other hash key
// to child 0. In the code, the child index is the next digit of the routingHash of the hash key.
// The model needs only a fixed function of the hash key, so it does not compute the routingHash.
fun ChildIndexOf(hk: string): int {
  if (hk == "c") {
    return 1;
  }
  return 0;
}
