# Adversarial review, round 1

Three independent reviewers (Raft safety, durability and concurrency of the real
server, honesty of the published results) tried to break the code and the claims,
reproducing each finding with a test or a rerun. Everything below was fixed with a
regression test, or the claim was corrected.

## Safety

| finding | fix |
|---|---|
| **Installing a snapshot kept the stored log suffix.** Raft discarded the divergent entries in memory, but both the simulator's storage and the real WAL kept them. A crash before the next append brought them back, splicing two histories (terms decreasing), and the restarted node could vote for a candidate missing a committed entry. The simulator's checks compared only in-memory logs, so they couldn't see it. | the Ready contract now says an installed snapshot replaces all stored entries; both drivers drop them. The simulator checks that persisted and in-memory log terms never decrease, and the bug is now a mutation the harness must catch |
| Porcupine treated operations that returned and started in the same tick as concurrent, so a stale read within one tick couldn't be caught | calls stamped at 2t+1 and returns at 2t |
| A leader kept leading after an append response carried a higher term. No test covered it | scenario test `TestLeaderStepsDownOnHigherTermResponse` |

## Real server (durability, concurrency, robustness)

| finding | fix |
|---|---|
| One unauthenticated `POST /raft` with a snapshot-less `MsgSnap` or non-contiguous entries panicked the process; forged vote responses from non-peers counted toward elections | `raft.Step` drops messages from non-peers or self, for other nodes, or with malformed snapshots/entries; optional shared peer token on `/raft` |
| A zero-filled WAL tail (a common power-loss artifact) made the node refuse to start | zero-length records are impossible by construction and zero/short tails are treated as torn |
| Corruption in the middle of the WAL was treated as a torn tail and truncated, silently discarding acknowledged records after it | only a bad *final* record is truncated; anything else is a hard error |
| After a storage error the loop could keep running and retry writes, possibly acknowledging data that wasn't durable | any storage error halts the node: waiters fail, the loop exits, nothing is written again |
| Server-assigned client ids could collide across nodes, silently returning a cached result instead of applying a write; the session table grew forever | requests without `X-Client-ID` are at-most-once and not recorded; a client id without a positive `X-Seq` is rejected |
| "Lost" responses claimed failure when the request might have committed | renamed to *outcome unknown*, with the only safe retry spelled out |
| WAL creation didn't fsync the directory; snapshot errors leaked the WAL handle; fsync-off still fsynced snapshots; queued requests waited out their timeout on shutdown; flaky redirect test | fixed |

## Honesty of the claims

| finding | fix |
|---|---|
| "0 violations" didn't say how much of the workload actually ran under faults | results now report the share of operations in flight during an active fault and the share that completed only after healing |
| The harness checks the pure Raft/KV core under an idealized storage model, not the real server's write ordering | stated in the README; three real-server bugs added to the mutation table, including misses |
| "6 of 6 caught" hid that half were caught only by tests written for those bugs | the headline separates randomized catches from targeted-test catches |
| Throughput: fsync-off still fsynced during snapshots; one run per point; up to 7 nodes plus the load generator on 8 cores | fsync-off honoured everywhere; closed-loop clients and CPU contention disclosed |
| Spec said every invariant was checked every step | wording matches what's checked and how often |
