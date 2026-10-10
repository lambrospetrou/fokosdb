// The root partition of each hash key. The topology is fixed: no split and no promotion.
type tTopology = map[string, machine];

fun RootOf(topology: tTopology, key: tKey): machine {
  return topology[key.hk];
}
