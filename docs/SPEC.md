# raftkv — spec

A replicated key-value store built on a from-scratch implementation of the Raft
consensus algorithm (Ongaro & Ousterhout, 2014), tested for linearizability
under injected faults, with a browser visualizer of the same code compiled to
WebAssembly. No Raft libraries.

## Goals (v1)

| # | Capability | Done when |
|---|---|---|
| G1 | Leader election with randomized timeouts and PreVote (a partitioned node can't disrupt a healthy cluster on rejoin) | election-safety and liveness tests under partitions |
| G2 | Log replication with the fast-backup optimisation (conflict term/index hints) | log-matching and leader-completeness properties checked after every simulated step |
| G3 | Snapshots and log compaction, `InstallSnapshot` for lagging followers | a follower 10,000 entries behind catches up via snapshot |
| G4 | KV state machine (`Get`, `Put`, `Append`) with client sessions: duplicate requests are applied exactly once | retry-after-timeout tests show no double appends |
| G5 | Linearizable reads, served through the log (ReadIndex left for later) | Porcupine finds zero violations over 1,000 randomized fault schedules |
| G6 | Deterministic simulator: seeded network with drops, delays, reordering, partitions, crashes and restarts from persisted state | the same seed replays the exact same run |
| G7 | Real deployment mode: one process per node, HTTP transport, file-backed WAL and snapshots, HTTP client API | 3-node local cluster survives `kill -9` of the leader |
| G8 | Browser visualizer: the simulator compiled to WASM; kill, restart and partition nodes by clicking | runs on GitHub Pages |

## Non-goals (v1)

- Membership changes (joint consensus). The cluster size is fixed at start.
- Multi-Raft / sharding.
- Byzantine faults.

## Architecture

The core (`raft`) is a pure, deterministic state machine in the style of etcd/raft:
it never does I/O, never reads a clock, and never starts goroutines. Callers feed
it `Tick()` and incoming messages via `Step(msg)`, and drain a `Ready` batch of
{hard state + entries to persist, messages to send, committed entries to apply,
snapshot to install}. Persistence must complete before messages are sent.

The same core runs under two drivers:

```
                   ┌──────────── raft (pure state machine) ────────────┐
                   │ Tick() · Step(msg) · Propose(data) · Ready()     │
                   └──────────────┬──────────────────────┬────────────┘
     sim driver (tests, WASM)     │                      │   node driver (real processes)
  seeded network · virtual clock  │                      │   HTTP transport · file WAL · wall clock
  crash/restart · partitions      ▼                      ▼   KV HTTP API
                               kv state machine (sessions, snapshot/restore)
```

This split is what makes G5/G6 possible: the simulator can run 1,000 fault
schedules in seconds, and a failing seed replays exactly.

## Success metrics (measured, committed)

- **Linearizability:** Porcupine check over 1,000 randomized fault schedules (partitions, drops, reorders, crash-restarts): 0 violations. Any violation found during development is kept as a regression seed.
- **Safety invariants** asserted after every simulated step: election safety (≤1 leader per term), log matching, leader completeness, state-machine safety.
- **Throughput and latency** vs cluster size (3/5/7) for the real-process mode on one machine, committed with the machine spec.
- **Coverage** ≥ 85% on `raft`, `kv`, `sim`, `node`.

## Complexity

- Append/commit per entry: O(1) amortised; replication fan-out O(n) per round.
- Commit index advance: O(n log n) (sort match indices) per AppendEntries response.
- Log storage: O(entries since last snapshot); snapshot threshold configurable.
