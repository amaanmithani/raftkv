package raft

import (
	"math/rand"
	"testing"
)

func newNode(id ID, peers []ID, st InitialState, mut func(*Config)) *Node {
	cfg := Config{ID: id, Peers: peers, ElectionTick: 10, HeartbeatTick: 1, Rand: rand.New(rand.NewSource(int64(id)))}
	if mut != nil {
		mut(&cfg)
	}
	return New(cfg, st)
}

func drain(n *Node) Ready {
	rd := n.Ready()
	n.Advance(rd)
	return rd
}

func msgsOf(rd Ready, t MsgType) []Message {
	var out []Message
	for _, m := range rd.Messages {
		if m.Type == t {
			out = append(out, m)
		}
	}
	return out
}

func TestSingleNodeElectsAndCommits(t *testing.T) {
	n := newNode(1, []ID{1}, InitialState{}, nil)
	for i := 0; i < 20 && n.Status().Role != Leader; i++ {
		n.Tick()
	}
	if n.Status().Role != Leader {
		t.Fatal("single node should elect itself")
	}
	idx, _, err := n.Propose([]byte("x"))
	if err != nil || idx != 2 { // 1 is the leader's no-op
		t.Fatalf("propose: %d %v", idx, err)
	}
	rd := drain(n)
	if len(rd.CommittedEntries) != 2 || string(rd.CommittedEntries[1].Data) != "x" || !rd.HardStateChanged {
		t.Fatalf("ready: %+v", rd)
	}
	if n.HasReady() {
		t.Fatal("nothing left after Advance")
	}
}

func TestProposeOnFollowerFails(t *testing.T) {
	n := newNode(1, []ID{1, 2, 3}, InitialState{}, nil)
	if _, _, err := n.Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("got %v", err)
	}
}

func TestVoteRules(t *testing.T) {
	n := newNode(1, []ID{1, 2, 3}, InitialState{Entries: []Entry{{Term: 2, Index: 1}}, HardState: HardState{Term: 2}}, nil)
	// Stale log (last term 1 < 2): rejected even with a higher term.
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3, LastLogIndex: 5, LastLogTerm: 1})
	if r := msgsOf(drain(n), MsgVoteResp); len(r) != 1 || r[0].Granted {
		t.Fatalf("stale candidate got a vote: %+v", r)
	}
	// Up-to-date candidate in term 4: granted; a second candidate same term: rejected.
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 4, LastLogIndex: 1, LastLogTerm: 2})
	n.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 4, LastLogIndex: 1, LastLogTerm: 2})
	r := msgsOf(drain(n), MsgVoteResp)
	if len(r) != 2 || !r[0].Granted || r[1].Granted || n.Status().Vote != 2 {
		t.Fatalf("one vote per term: %+v vote=%d", r, n.Status().Vote)
	}
	// Re-request from the same candidate is idempotent.
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 4, LastLogIndex: 1, LastLogTerm: 2})
	if r := msgsOf(drain(n), MsgVoteResp); !r[0].Granted {
		t.Fatal("repeat vote request from the same candidate should be granted")
	}
}

func TestPreVoteDoesNotChangeTerm(t *testing.T) {
	n := newNode(1, []ID{1, 2, 3}, InitialState{HardState: HardState{Term: 5}}, nil)
	n.Step(Message{Type: MsgPreVote, From: 2, To: 1, Term: 6})
	rd := drain(n)
	if r := msgsOf(rd, MsgPreVoteResp); !r[0].Granted || r[0].Term != 6 || n.Status().Term != 5 {
		t.Fatalf("pre-vote: %+v term=%d", r, n.Status().Term)
	}
	n.Step(Message{Type: MsgPreVote, From: 2, To: 1, Term: 4}) // stale
	if r := msgsOf(drain(n), MsgPreVoteResp); r[0].Granted || r[0].Term != 5 {
		t.Fatalf("stale pre-vote: %+v", r)
	}
}

func TestAppendConflictTruncatesAndFastBackupHint(t *testing.T) {
	st := InitialState{HardState: HardState{Term: 3}, Entries: []Entry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 2, Index: 3}, {Term: 2, Index: 4}, {Term: 2, Index: 5}}}
	n := newNode(1, []ID{1, 2, 3}, st, nil)
	// Leader in term 3 claims prev (5, 3): mismatch; hint = first index of term 2.
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 3, PrevIndex: 5, PrevTerm: 3})
	r := msgsOf(drain(n), MsgAppResp)[0]
	if r.Success || r.ConflictTerm != 2 || r.ConflictIndex != 3 {
		t.Fatalf("hint: %+v", r)
	}
	// prev beyond our log.
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 3, PrevIndex: 9, PrevTerm: 3})
	if r := msgsOf(drain(n), MsgAppResp)[0]; r.Success || r.ConflictIndex != 6 || r.ConflictTerm != 0 {
		t.Fatalf("short log hint: %+v", r)
	}
	// Matching at 2, then conflicting entries overwrite 3..5.
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 3, PrevIndex: 2, PrevTerm: 1, Commit: 3,
		Entries: []Entry{{Term: 3, Index: 3, Data: []byte("a")}}})
	rd := drain(n)
	if r := msgsOf(rd, MsgAppResp)[0]; !r.Success || r.MatchIndex != 3 {
		t.Fatalf("accept: %+v", r)
	}
	if n.Status().LastIndex != 3 || n.Status().Commit != 3 {
		t.Fatalf("truncate + commit: %+v", n.Status())
	}
	if len(rd.Entries) != 1 || rd.Entries[0].Index != 3 {
		t.Fatalf("driver must be told to persist from the truncation point: %+v", rd.Entries)
	}
	// A duplicate, already-present append is a no-op.
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 3, PrevIndex: 2, PrevTerm: 1, Entries: []Entry{{Term: 3, Index: 3, Data: []byte("a")}}})
	if rd := drain(n); len(rd.Entries) != 0 {
		t.Fatalf("duplicate append rewrote entries: %+v", rd.Entries)
	}
}

func TestStaleLeaderToldNewTerm(t *testing.T) {
	n := newNode(1, []ID{1, 2, 3}, InitialState{HardState: HardState{Term: 7}}, nil)
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 3})
	if r := msgsOf(drain(n), MsgAppResp); len(r) != 1 || r[0].Term != 7 {
		t.Fatalf("stale leader should learn term 7: %+v", r)
	}
}

func TestLeaderAdvancesCommitOnlyForCurrentTerm(t *testing.T) {
	n := newNode(1, []ID{1, 2, 3}, InitialState{HardState: HardState{Term: 1}, Entries: []Entry{{Term: 1, Index: 1}}}, nil)
	n.Campaign()
	n.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: 2, Granted: true})
	if n.Status().Role != Leader {
		t.Fatal("should lead")
	}
	drain(n)
	// Follower 2 acknowledges only the old-term entry: must not commit (§5.4.2).
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, Success: true, MatchIndex: 1})
	if n.Status().Commit != 0 {
		t.Fatalf("committed an old-term entry by counting replicas: %d", n.Status().Commit)
	}
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, Success: true, MatchIndex: 2})
	if n.Status().Commit != 2 {
		t.Fatalf("no-op in current term should commit everything: %d", n.Status().Commit)
	}
	// Rejection with a hint moves next back and resends.
	n.Step(Message{Type: MsgAppResp, From: 3, To: 1, Term: 2, ConflictIndex: 1})
	if apps := msgsOf(drain(n), MsgApp); len(apps) == 0 || apps[len(apps)-1].PrevIndex != 0 {
		t.Fatalf("resend from hint: %+v", apps)
	}
	if s := n.Status(); s.Match[2] != 2 || s.Match[1] != 2 {
		t.Fatalf("status match: %+v", s.Match)
	}
}

func TestRestoreFromSnapshotAndCompact(t *testing.T) {
	snap := &Snapshot{Index: 10, Term: 3, Peers: []ID{1}, Data: []byte("sm")}
	n := newNode(1, []ID{1}, InitialState{Snapshot: snap, HardState: HardState{Term: 3, Commit: 11},
		Entries: []Entry{{Term: 3, Index: 9}, {Term: 3, Index: 11, Data: []byte("x")}}}, nil)
	rd := drain(n)
	if len(rd.CommittedEntries) != 1 || rd.CommittedEntries[0].Index != 11 {
		t.Fatalf("restart should re-apply (snapshot, commit]: %+v", rd.CommittedEntries)
	}
	if _, err := n.Compact(99, nil); err == nil {
		t.Fatal("compacting beyond applied must fail")
	}
	s, err := n.Compact(11, []byte("sm2"))
	if err != nil || s.Index != 11 || s.Term != 3 || n.Status().SnapIndex != 11 {
		t.Fatalf("compact: %+v %v", s, err)
	}
	if again, _ := n.Compact(5, nil); again != s {
		t.Fatal("compacting below the snapshot returns the existing one")
	}
}

func TestConfigValidation(t *testing.T) {
	mustPanic := func(f func()) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		f()
	}
	mustPanic(func() { New(Config{ID: 1, Peers: []ID{2}}, InitialState{}) })
	mustPanic(func() { New(Config{ID: 1, Peers: []ID{1}, ElectionTick: 2, HeartbeatTick: 2}, InitialState{}) })
	mustPanic(func() { New(Config{ID: 1, Peers: []ID{1}}, InitialState{HardState: HardState{Commit: 5}}) })
	if MsgApp.String() != "App" || MsgType(99).String() == "" || Leader.String() != "leader" {
		t.Fatal("names")
	}
	if (Ready{}).Empty() != true {
		t.Fatal("empty ready")
	}
}
