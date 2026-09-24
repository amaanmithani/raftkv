package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// net is a hand-driven network for scripting exact interleavings.
type net struct {
	t       *testing.T
	nodes   map[ID]*Node
	ids     []ID
	applied map[ID]map[uint64]Entry
	allow   func(Message) bool
	// rewrite, if set, edits a message in flight (after allow).
	rewrite func(Message) Message
}

func newNet(t *testing.T, n int) *net {
	nw := &net{t: t, nodes: map[ID]*Node{}, applied: map[ID]map[uint64]Entry{}, allow: func(Message) bool { return true }}
	for i := 1; i <= n; i++ {
		nw.ids = append(nw.ids, ID(i))
	}
	for _, id := range nw.ids {
		// Huge election timeout: elections happen only when the script says so.
		nw.nodes[id] = New(Config{ID: id, Peers: nw.ids, ElectionTick: 1000, HeartbeatTick: 999, MaxEntriesPerMsg: 1,
			Rand: rand.New(rand.NewSource(int64(id)))}, InitialState{})
		nw.applied[id] = map[uint64]Entry{}
	}
	return nw
}

// pump delivers allowed messages until the network is quiet, recording
// applied entries and checking state-machine safety.
func (nw *net) pump() {
	for round := 0; round < 1000; round++ {
		busy := false
		for _, id := range nw.ids {
			n := nw.nodes[id]
			if !n.HasReady() {
				continue
			}
			busy = true
			rd := n.Ready()
			for _, e := range rd.CommittedEntries {
				nw.applied[id][e.Index] = e
				for _, other := range nw.ids {
					if o, ok := nw.applied[other][e.Index]; ok && (o.Term != e.Term || string(o.Data) != string(e.Data)) {
						nw.t.Fatalf("state machine safety: node %d applied %d@t%d, node %d applied %d@t%d",
							id, e.Index, e.Term, other, o.Index, o.Term)
					}
				}
			}
			n.Advance(rd)
			for _, m := range rd.Messages {
				if nw.allow(m) {
					if nw.rewrite != nil {
						m = nw.rewrite(m)
					}
					nw.nodes[m.To].Step(m)
				}
			}
		}
		if !busy {
			return
		}
	}
	nw.t.Fatal("network never went quiet")
}

func (nw *net) elect(id ID) {
	for i := 0; i < 5 && nw.nodes[id].Status().Role != Leader; i++ {
		nw.nodes[id].Campaign()
		nw.pump()
	}
	if nw.nodes[id].Status().Role != Leader {
		nw.t.Fatalf("node %d failed to become leader: %+v", id, nw.nodes[id].Status())
	}
}

// restart simulates a crash and restart from persisted state (term, vote,
// commit and log survive; everything else is lost).
func (nw *net) restart(id ID) {
	old := nw.nodes[id]
	st := old.Status()
	nw.nodes[id] = New(Config{ID: id, Peers: nw.ids, ElectionTick: 1000, HeartbeatTick: 999, MaxEntriesPerMsg: 1,
		Rand: rand.New(rand.NewSource(int64(id)))},
		InitialState{HardState: HardState{Term: st.Term, Vote: st.Vote, Commit: st.Commit}, Entries: old.Entries()})
	// Entries at or below commit were applied before the crash.
	nw.nodes[id].applied = st.Commit
}

func (nw *net) logf(label string) {
	for _, id := range nw.ids {
		st := nw.nodes[id].Status()
		var terms []uint64
		for _, e := range nw.nodes[id].Entries() {
			terms = append(terms, e.Term)
		}
		nw.t.Logf("%s node %d role=%s term=%d commit=%d log-terms=%v", label, id, st.Role, st.Term, st.Commit, terms)
	}
}

func within(group ...ID) func(Message) bool {
	in := map[ID]bool{}
	for _, id := range group {
		in[id] = true
	}
	return func(m Message) bool { return in[m.From] && in[m.To] }
}

// TestFigure8 scripts the scenario from Figure 8 of the Raft paper: a leader
// must not commit an entry from an earlier term just because a majority
// stores it, or a later leader can overwrite it after it was applied.
func TestFigure8(t *testing.T) {
	nw := newNet(t, 5)
	nw.elect(1) // term 1, no-op at index 1 committed everywhere
	nw.pump()

	// (a) Leader 1 appends X at index 2 but only node 2 receives it.
	nw.allow = within(1, 2)
	if _, _, err := nw.nodes[1].Propose([]byte("X")); err != nil {
		t.Fatal(err)
	}
	nw.pump()

	// (b) Node 5 wins term 2 with votes from 3 and 4 and appends its no-op
	// at index 2, which nobody else receives.
	nw.allow = func(m Message) bool { return within(3, 4, 5)(m) && m.Type != MsgApp }
	nw.elect(5)

	// (c) Node 1 crashes and restarts, wins a later term with 2 and 3, and
	// replicates X (index 2, term 1) to node 3, but its own no-op (index 3)
	// doesn't get through.
	nw.restart(1)
	nw.allow = within(1, 2, 3)
	nw.rewrite = func(m Message) Message {
		var keep []Entry
		for _, e := range m.Entries {
			if e.Index < 3 {
				keep = append(keep, e)
			}
		}
		m.Entries = keep // entries at index 3 are lost in transit
		return m
	}
	nw.elect(1)
	nw.pump()
	nw.rewrite = nil
	nw.logf("after (c)")
	if nw.nodes[3].Status().LastIndex != 2 || nw.nodes[1].Status().Match[3] != 2 {
		t.Fatalf("script did not reach Figure 8 state (c): node 3 must hold X at index 2")
	}
	// X is now on a majority (1, 2, 3) but only from term 1: it must NOT be
	// committed.
	if c := nw.nodes[1].Status().Commit; c >= 2 {
		t.Fatalf("leader committed index 2 from an earlier term by counting replicas (commit=%d)", c)
	}

	// (d) Node 5 comes back, wins with 2, 3, 4 (its last term 2 beats their
	// term-1 entries) and overwrites index 2 everywhere. Safety holds only if
	// nobody applied X.
	nw.restart(1)
	nw.allow = within(2, 3, 4, 5)
	nw.restart(5)
	nw.elect(5)
	nw.pump()
	if _, _, err := nw.nodes[5].Propose([]byte("Y")); err != nil {
		t.Fatal(err)
	}
	nw.pump()
	nw.allow = func(Message) bool { return true }
	for i := 0; i < 3; i++ {
		nw.nodes[5].Tick()
		nw.pump()
	}
	if e := nw.applied[1][2]; string(e.Data) == "X" {
		t.Fatal("X was applied and later overwritten")
	}
	if got := fmt.Sprint(nw.nodes[1].Status().Commit); got == "0" {
		t.Fatal("old leader never caught up")
	}
}

// A follower may only commit entries it has verified match the leader's log.
// Committing up to its own last index can commit a stale suffix.
func TestFollowerCommitsOnlyVerifiedPrefix(t *testing.T) {
	st := InitialState{HardState: HardState{Term: 1}, Entries: []Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2, Data: []byte("stale")}}}
	n := newNode(1, []ID{1, 2, 3}, st, nil)
	// A term-2 leader whose index 2 differs heartbeats with prev=1 and commit=2.
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 2, PrevIndex: 1, PrevTerm: 1, Commit: 2})
	rd := drain(n)
	if n.Status().Commit != 1 {
		t.Fatalf("follower committed its unverified suffix: commit=%d", n.Status().Commit)
	}
	for _, e := range rd.CommittedEntries {
		if string(e.Data) == "stale" {
			t.Fatal("applied a stale entry")
		}
	}
}
