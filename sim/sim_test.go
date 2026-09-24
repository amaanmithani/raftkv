package sim

import (
	"fmt"
	"testing"

	"github.com/amaanmithani/raftkv/kv"
	"github.com/amaanmithani/raftkv/raft"
)

func cluster(t *testing.T, n int, seed int64, mut func(*Config)) *Cluster {
	t.Helper()
	cfg := Config{N: n, Seed: seed, PreVote: true, CheckQuorum: true}
	if mut != nil {
		mut(&cfg)
	}
	return New(cfg)
}

func mustRun(t *testing.T, c *Cluster, steps int) {
	t.Helper()
	if err := c.Run(steps); err != nil {
		t.Fatal(err)
	}
}

func waitLeader(t *testing.T, c *Cluster, max int) raft.ID {
	t.Helper()
	for i := 0; i < max; i++ {
		if l := c.Leader(); l != raft.None {
			return l
		}
		mustRun(t, c, 1)
	}
	t.Fatalf("no leader after %d steps (seed %d)", max, c.cfg.Seed)
	return raft.None
}

func propose(t *testing.T, c *Cluster, key, val string) (uint64, uint64) {
	t.Helper()
	l := waitLeader(t, c, 200)
	idx, term, err := c.Nodes[l].Raft.Propose(kv.Command{Op: kv.Put, Key: key, Value: val}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	return idx, term
}

func TestElectsSingleLeader(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		c := cluster(t, 5, seed, nil)
		l := waitLeader(t, c, 200)
		mustRun(t, c, 100)
		if c.Leader() != l {
			t.Fatalf("seed %d: stable cluster changed leader %d -> %d", seed, l, c.Leader())
		}
	}
}

func TestReplicatesAndAppliesEverywhere(t *testing.T) {
	c := cluster(t, 5, 7, nil)
	for i := 0; i < 20; i++ {
		propose(t, c, fmt.Sprint("k", i%3), fmt.Sprint(i))
	}
	mustRun(t, c, 100)
	for _, id := range c.IDs {
		v, _ := c.Nodes[id].KV.Get("k1")
		if v != "19" {
			t.Fatalf("node %d has k1=%q, want 19", id, v)
		}
	}
}

func TestLeaderCrashNewLeaderKeepsCommitted(t *testing.T) {
	c := cluster(t, 5, 3, nil)
	for i := 0; i < 10; i++ {
		propose(t, c, "x", fmt.Sprint(i))
	}
	mustRun(t, c, 50)
	old := c.Leader()
	c.Crash(old)
	l := waitLeader(t, c, 300)
	if l == old {
		t.Fatal("crashed leader still leader")
	}
	propose(t, c, "x", "after")
	mustRun(t, c, 50)
	c.Restart(old)
	mustRun(t, c, 200)
	for _, id := range c.IDs {
		if v, _ := c.Nodes[id].KV.Get("x"); v != "after" {
			t.Fatalf("node %d: x=%q", id, v)
		}
	}
}

func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := cluster(t, 5, 11, nil)
	waitLeader(t, c, 200)
	mustRun(t, c, 20)
	l := c.Leader()
	others := []raft.ID{}
	for _, id := range c.IDs {
		if id != l {
			others = append(others, id)
		}
	}
	// Leader alone with one follower: a minority.
	c.Partition([]raft.ID{l, others[0]}, others[1:])
	before := c.Nodes[l].Raft.Status().Commit
	if _, _, err := c.Nodes[l].Raft.Propose(kv.Command{Op: kv.Put, Key: "lost", Value: "1"}.Encode()); err != nil {
		t.Fatal(err)
	}
	mustRun(t, c, 60)
	if got := c.Nodes[l].Raft.Status().Commit; got != before && c.Nodes[l].Raft.Status().Role == raft.Leader {
		t.Fatalf("minority leader advanced commit %d -> %d", before, got)
	}
	// CheckQuorum: the isolated leader steps down.
	if c.Nodes[l].Raft.Status().Role == raft.Leader {
		t.Fatal("minority leader should step down (CheckQuorum)")
	}
	// Majority elects its own leader and commits.
	nl := c.Leader()
	if nl == raft.None || nl == l {
		t.Fatalf("majority side should have a new leader, got %d", nl)
	}
	c.Heal()
	mustRun(t, c, 200)
	for _, id := range c.IDs {
		if _, ok := c.Nodes[id].KV.Get("lost"); ok {
			t.Fatalf("node %d applied an entry proposed to a minority leader", id)
		}
	}
}

func TestPreVoteRejoiningNodeDoesNotDisrupt(t *testing.T) {
	c := cluster(t, 5, 5, nil)
	l := waitLeader(t, c, 200)
	mustRun(t, c, 20)
	var victim raft.ID
	for _, id := range c.IDs {
		if id != l {
			victim = id
			break
		}
	}
	term := c.Nodes[l].Raft.Status().Term
	c.Partition(nil) // isolate everyone...
	var rest []raft.ID
	for _, id := range c.IDs {
		if id != victim {
			rest = append(rest, id)
		}
	}
	c.Partition(rest)  // ...then reconnect all but the victim
	mustRun(t, c, 300) // the victim times out repeatedly while isolated
	c.Heal()
	mustRun(t, c, 100)
	if c.Leader() != l || c.Nodes[l].Raft.Status().Term != term {
		t.Fatalf("rejoining node disrupted the leader: leader %d term %d -> leader %d term %d",
			l, term, c.Leader(), c.Nodes[c.Leader()].Raft.Status().Term)
	}
	if vt := c.Nodes[victim].Raft.Status().Term; vt > term {
		t.Fatalf("isolated node inflated its term to %d (PreVote should prevent it)", vt)
	}
}

func TestSnapshotCatchUp(t *testing.T) {
	c := cluster(t, 3, 21, func(cfg *Config) { cfg.SnapshotEvery = 50 })
	l := waitLeader(t, c, 200)
	var lagger raft.ID
	for _, id := range c.IDs {
		if id != l {
			lagger = id
			break
		}
	}
	c.Crash(lagger)
	for i := 0; i < 400; i++ {
		propose(t, c, fmt.Sprint("k", i%7), fmt.Sprint(i))
		if i%20 == 0 {
			mustRun(t, c, 5)
		}
	}
	mustRun(t, c, 50)
	if c.Nodes[c.Leader()].Raft.Status().SnapIndex == 0 {
		t.Fatal("leader should have compacted its log")
	}
	c.Restart(lagger)
	mustRun(t, c, 300)
	for i := 0; i < 7; i++ {
		k := fmt.Sprint("k", i)
		want, _ := c.Nodes[c.Leader()].KV.Get(k)
		if got, _ := c.Nodes[lagger].KV.Get(k); got != want {
			t.Fatalf("lagger %s=%q want %q", k, got, want)
		}
	}
	if c.Nodes[lagger].Raft.Status().SnapIndex == 0 {
		t.Fatal("lagger should have caught up via snapshot")
	}
}

func TestCrashRestartFromPersistedState(t *testing.T) {
	c := cluster(t, 3, 8, func(cfg *Config) { cfg.SnapshotEvery = 10 })
	for i := 0; i < 30; i++ {
		propose(t, c, "a", fmt.Sprint(i))
	}
	mustRun(t, c, 50)
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Restart(id)
	}
	mustRun(t, c, 300)
	for _, id := range c.IDs {
		if v, _ := c.Nodes[id].KV.Get("a"); v != "29" {
			t.Fatalf("node %d after full restart: a=%q", id, v)
		}
	}
}

func TestDeterministicReplay(t *testing.T) {
	run := func() (int, int, uint64) {
		c := cluster(t, 5, 99, func(cfg *Config) { cfg.DropRate = 0.1; cfg.MaxDelay = 4 })
		for i := 0; i < 5; i++ {
			propose(t, c, "k", fmt.Sprint(i))
		}
		c.Crash(c.Leader())
		mustRun(t, c, 300)
		return c.Sent, c.Dropped, c.Nodes[c.Leader()].Raft.Status().Term
	}
	s1, d1, t1 := run()
	s2, d2, t2 := run()
	if s1 != s2 || d1 != d2 || t1 != t2 {
		t.Fatalf("same seed diverged: (%d,%d,%d) vs (%d,%d,%d)", s1, d1, t1, s2, d2, t2)
	}
}

func TestLogMatchingDetectsDivergence(t *testing.T) {
	a := []raft.Entry{{Term: 1, Index: 1, Data: []byte("x")}, {Term: 2, Index: 2}}
	b := []raft.Entry{{Term: 1, Index: 1, Data: []byte("y")}, {Term: 2, Index: 2}}
	if logMatching(a, b) == nil {
		t.Fatal("checker missed a divergent prefix")
	}
	if logMatching(a, a) != nil || logMatching(a, nil) != nil {
		t.Fatal("false positive")
	}
}

// A vote must survive a crash: otherwise a restarted node can vote for a
// second candidate in the same term, and two leaders can be elected.
func TestVotePersistsAcrossCrash(t *testing.T) {
	c := cluster(t, 3, 1, func(cfg *Config) { cfg.PreVote = false; cfg.CheckQuorum = false })
	n2 := c.Nodes[2]
	n2.Raft.Step(raft.Message{Type: raft.MsgVote, From: 1, To: 2, Term: 5, LastLogIndex: 0, LastLogTerm: 0})
	if err := c.process(n2); err != nil {
		t.Fatal(err)
	}
	c.Crash(2)
	c.Restart(2)
	n2 = c.Nodes[2]
	n2.Raft.Step(raft.Message{Type: raft.MsgVote, From: 3, To: 2, Term: 5, LastLogIndex: 0, LastLogTerm: 0})
	rd := n2.Raft.Ready()
	for _, m := range rd.Messages {
		if m.Type == raft.MsgVoteResp && m.Granted {
			t.Fatal("restarted node voted twice in term 5")
		}
	}
}

// An installed snapshot must replace the whole stored log: a crash right
// after installing it must not bring back entries from the old history.
func TestSnapshotInstallDropsStoredSuffix(t *testing.T) {
	c := cluster(t, 3, 1, nil)
	n := c.Nodes[3]
	// Node 3 has a stale branch at term 1, indexes 1..4.
	n.st.entries = []raft.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}, {Term: 1, Index: 4}}
	c.Crash(3)
	c.Restart(3)
	n = c.Nodes[3]
	store := kv.New()
	n.Raft.Step(raft.Message{Type: raft.MsgSnap, From: 1, To: 3, Term: 3,
		Snapshot: &raft.Snapshot{Index: 2, Term: 2, Peers: c.IDs, Data: store.Snapshot()}})
	if err := c.process(n); err != nil {
		t.Fatal(err)
	}
	c.Crash(3)
	c.Restart(3)
	for _, e := range c.Nodes[3].Raft.Entries() {
		if e.Index > 2 && e.Term < 2 {
			t.Fatalf("stale entry %d@t%d came back after a restart", e.Index, e.Term)
		}
	}
	if err := c.check(); err != nil {
		t.Fatal(err)
	}
}
