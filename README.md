# raftkv

A replicated key-value store on a from-scratch Raft implementation, tested for
linearizability under injected faults. Work in progress. See [docs/SPEC.md](docs/SPEC.md).

## Results

<!-- RESULTS:START -->
### Linearizability under faults

1000 seeded schedules (seeds 1..1000), each: 5 nodes, 6 clients x 30 ops on 3 keys, 800 steps of nemesis (random splits, leader isolation, crashes with and without immediate restart, leader crashes, 0-40% message loss, 1-5 tick delays with reordering), then healing. PreVote, CheckQuorum, append batch size, snapshot interval and fault frequency are randomized per seed. Raft safety invariants checked during the run; history checked with Porcupine.

**1,000 schedules, 0 violations.** 180,000 client operations checked; 16,661 crashes, 19,218 partitions, 9,364 leader terms, 385,919 of 5,964,056 messages dropped. Wall time 7 s on 8 cores.

### Does the harness catch real bugs?

A checker that never fails proves nothing, so each classic consensus bug below was injected into a copy of the code (300 schedules per bug, plus the targeted scenario tests). **6 of 6 caught.**

| injected bug | randomized harness | targeted scenario test |
|---|---|---|
| Voters skip the up-to-date log check (§5.4.1) | 136/300 schedules fail: leader completeness: leader 4 (term 4) lacks committed entry 1 (seed 1, t=56) | — |
| A node may vote for several candidates in one term | 226/300 schedules fail: election safety: nodes 1 and 4 both led term 1 (seed 4, t=21) | `TestVotePersistsAcrossCrash` |
| Leader commits earlier-term entries by counting replicas (Figure 8 bug) | not caught | `TestFigure8` |
| Follower commits up to the leader's index without checking its log matches that far | not caught | `TestFollowerCommitsOnlyVerifiedPrefix` |
| The vote isn't persisted, so a restarted node can vote twice in a term | not caught | `TestVotePersistsAcrossCrash` |
| The state machine re-executes retried requests | 247/300 schedules fail: non-linearizable history | — |

Three bugs need timing that random schedules almost never produce, such as the leader-change sequence in Figure 8 of the Raft paper, so they're covered by scripted scenario tests that drive exact message interleavings.

### Throughput and latency vs cluster size

all nodes and the load generator on one machine (localhost HTTP); 32 and 128 concurrent clients (one run each), sequential PUTs of 64-byte values over 1,000 keys for 20s after a 3s warm-up; each request commits through the log; fsync on = the WAL record for each batch is fsynced before messages are sent (group commit). On macOS Go's File.Sync is F_FULLFSYNC, a full flush of the drive cache, much slower than Linux fdatasync. Machine: Apple M1 Pro, 8 cores.

| nodes | fsync | clients | writes/s | p50 | p99 | errors |
|---|---|---|---|---|---|---|
| 3 | on | 32 | 586 | 53.8 ms | 104.9 ms | 0 |
| 3 | on | 128 | 2,202 | 54.4 ms | 105.0 ms | 0 |
| 3 | off | 32 | 14,653 | 1.7 ms | 16.6 ms | 0 |
| 3 | off | 128 | 22,187 | 4.1 ms | 20.7 ms | 0 |
| 5 | on | 32 | 489 | 63.5 ms | 122.2 ms | 0 |
| 5 | on | 128 | 1,766 | 66.6 ms | 135.6 ms | 0 |
| 5 | off | 32 | 9,887 | 2.5 ms | 22.7 ms | 0 |
| 5 | off | 128 | 15,926 | 5.8 ms | 28.9 ms | 0 |
| 7 | on | 32 | 433 | 71.2 ms | 147.2 ms | 0 |
| 7 | on | 128 | 1,510 | 76.6 ms | 174.2 ms | 0 |
| 7 | off | 32 | 9,227 | 2.6 ms | 27.7 ms | 0 |
| 7 | off | 128 | 12,180 | 8.5 ms | 33.3 ms | 0 |

With fsync on, latency is set by the disk flush (leader, then followers, in sequence), so throughput scales with concurrency: 3 nodes go from 586 to 2,202 writes/s between 32 and 128 clients at the same p50, because concurrent requests share one WAL record and one fsync (group commit).
<!-- RESULTS:END -->
