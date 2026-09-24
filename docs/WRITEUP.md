# raftkv: design notes

## The one decision that mattered

The Raft core is a pure state machine: it never touches the disk, the network or
a clock. A driver calls `Tick()` and `Step(msg)`, then drains a `Ready` batch
that says what to persist, what to send and what to apply, in that order. etcd
structures its Raft library the same way, and for this project it pays off twice:

- **Testing.** A simulator can drive five nodes through a thousand randomized
  fault schedules in seconds, with a virtual clock and a seeded network, and check
  Raft's safety properties after every step. A failing seed replays exactly.
- **Reuse.** The same code runs under three drivers: the simulator, real
  processes with a write-ahead log, and the browser via WebAssembly.

## Testing the tests

A linearizability checker that has never failed tells you nothing. After the
first 1,000-schedule run came back clean, I injected classic consensus bugs into
a copy of the code (voting without checking the candidate's log, voting twice,
committing an old-term entry by counting replicas, and others) to see if the
harness would notice.

**It caught 2 of 7.** Three causes:

1. **Defence in depth masked single bugs.** PreVote and the real vote both check
   that the candidate's log is up to date, so breaking one was covered by the
   other. Legitimate, but the mutation has to break both to be meaningful.
2. **The fault schedule was too gentle.** Faults every 25 steps, always with
   PreVote and CheckQuorum on, rarely produced the timing these bugs need. I
   rewrote the nemesis: leader isolation, crash-and-immediately-restart, leader
   crashes, 1-5 tick delays, small append batches, fault gaps down to 5 steps,
   and PreVote/CheckQuorum randomized per seed so the optional defences can't
   hide bugs in the core algorithm.
3. **Some bugs need an exact interleaving.** Figure 8 of the Raft paper (a
   leader committing an earlier-term entry) requires a specific sequence of
   three leaders. Random schedules essentially never produce it, so it's a
   scripted scenario test that drives each message by hand and asserts the
   intermediate states are reached. My first version of that test passed on
   the buggy code because the script never reached the dangerous state; the
   assertions on intermediate states exist because of that.

After those changes all six catchable bugs are caught: three by the randomized
harness, three by scenario tests.

## Group commit, found by the benchmark

The first throughput run of the real-process mode, 3 nodes with fsync on,
managed about **29 writes per second** (a trial run, not a committed result)
with p99 latency above a second. The event loop persisted every proposal
separately, and each persist was three fsyncs: the WAL append, plus an atomic
rewrite of a hard-state file (file fsync and directory fsync) because the commit
index changes on almost every message.

Two changes fixed it:

1. The hard state moved into the WAL as its own record type, so one batch is one
   record and one fsync.
2. The loop now drains everything already queued (incoming messages and client
   proposals) before persisting, so concurrent requests share a single fsync.

The same configuration now does about 590 writes/s at 32 clients and about 2,200
at 128, at the same median latency. That's the signature of group commit:
latency is set by the disk flush, and throughput by how many requests share one.

## A harness bug that looked like a store bug

At 128 clients the benchmark reported thousands of errors. The cause was the load
generator: each run reused the same client ids with sequence numbers restarting
at 1, and the store's client sessions (correctly) rejected those as stale
duplicates. The store was doing exactly its job. The fix was in the benchmark.

## What the reviewers found

Three adversarial reviewers went through the code and the claims ([REVIEW.md](REVIEW.md)).
The most important finding was a real persistence bug the simulator couldn't
see. When a follower installed a snapshot, Raft threw away its divergent log
suffix in memory, but storage kept it. A crash at the wrong moment brought the
old entries back, splicing two histories together. The simulator missed it
because its invariants only looked at in-memory logs. It now also checks that
the *persisted* log's terms never decrease, and the bug is part of the mutation
suite.

The lesson matches the one from mutation testing: a checker only sees what
it's pointed at. The randomized harness is strong on the algorithm and blind to
how the real server orders its disk writes. The README now says that, and lists
the server-level bugs the tests miss.

## Trade-offs

- **Reads through the log.** Every read is a log entry, which is simple and
  linearizable with no clock assumptions, but costs a replication round. ReadIndex
  or leader leases would be the next step.
- **JSON everywhere on disk.** The WAL records and snapshots are JSON: slower
  than a binary encoding but inspectable, and the fsync dominates anyway.
- **Fixed membership.** Joint consensus is out of scope.
