# FokosDB testing

## General

- [TigerStyle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)

## Deterministic Simulation Testing - DST

- [Deterministic simulation testing - how it works and when to use it](https://antithesis.com/docs/resources/deterministic_simulation_testing/)
- [Testing Distributed Systems for Linearizability](https://anishathalye.com/testing-distributed-systems-for-linearizability/)
- [Linearizability testing S2 with deterministic simulation](https://s2.dev/blog/linearizability)
- [What's the big deal about Deterministic Simulation Testing?](https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html)
- [Protocol-Aware Deterministic Simulation Testing](https://tigerbeetle.com/blog/2026-08-20-protocol-aware-dst/)
- (video) [How to write your own Deterministic Simulator](https://www.youtube.com/watch?v=JoYjji1DZCE)
- (video) [Testing Distributed Systems w/ Deterministic Simulation by Will Wilson](https://www.youtube.com/watch?v=4fFDFbi3toc)

## Chaos and fault injection

- Custom RPC / Durable Object fault injection proxy.

## Tools

- https://github.com/anishathalye/porcupine - "Porcupine is a fast linearizability checker used in both academia and industry for testing the correctness of distributed systems. It takes a sequential specification as executable Go code, along with a concurrent history, and it determines whether the history is linearizable with respect to the sequential specification. Porcupine also implements a visualizer for histories and linearization points."

- https://www.npmjs.com/package/fast-check - "Property based testing framework for JavaScript/TypeScript"
