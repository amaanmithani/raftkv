// Package sim runs a raft cluster in a deterministic, seeded simulation:
// virtual time, a lossy reordering network, partitions, crashes and restarts
// from persisted state. Safety invariants are checked after every step, and a
// seed replays the exact same run.
package sim

import (
	"fmt"
	"math/rand"
	"slices"

	"github.com/amaanmithani/raftkv/kv"
	"github.com/amaanmithani/raftkv/raft"
)

// Config configures a simulated cluster.
type Config struct {
	N                int
	Seed             int64
	ElectionTick     int
	HeartbeatTick    int
	PreVote          bool
	CheckQuorum      bool
	DropRate         float64 // probability a message is lost
	DupRate          float64 // probability a message is delivered twice
	MaxDelay         int     // messages take 1..MaxDelay ticks
	SnapshotEvery    uint64  // compact when this many entries applied since the last snapshot (0 = never)
	MaxEntriesPerMsg int     // raft append batch size (0 = raft default)
	// CheckEvery runs the O(log) cross-node invariants every k steps (0 = every step).
	CheckEvery int
}

// storage is what survives a crash.
type storage struct {
	hs      raft.HardState
	snap    *raft.Snapshot
	entries []raft.Entry
}

// Node is one simulated server.
type Node struct {
	ID    raft.ID
	Raft  *raft.Node
	KV    *kv.Store
	Alive bool
	st    storage
	// leaderTermChecked is the last term in which leader completeness was
	// verified for this node.
	leaderTermChecked uint64
}

type envelope struct {
	m    raft.Message
	sent int
	at   int
}

// InFlight is a message on the wire (for visualisation).
type InFlight struct {
	Msg    raft.Message
	SentAt int
	DueAt  int
}

// InFlight returns messages currently in transit.
func (c *Cluster) InFlight() []InFlight {
	out := make([]InFlight, 0, len(c.inflight))
	for _, e := range c.inflight {
		out = append(out, InFlight{Msg: e.m, SentAt: e.sent, DueAt: e.at})
	}
	return out
}

// Connected reports whether a and b can currently exchange messages.
func (c *Cluster) Connected(a, b raft.ID) bool { return c.connected(a, b) }

// maxInflight caps messages in transit across the whole simulated network.
const maxInflight = 20_000

// ApplyFunc observes every applied entry (used by client workloads).
type ApplyFunc func(node raft.ID, e raft.Entry, cmd kv.Command, res kv.Result, err error)

// Cluster is a simulated cluster.
type Cluster struct {
	cfg       Config
	rng       *rand.Rand
	Nodes     map[raft.ID]*Node
	IDs       []raft.ID
	inflight  []envelope
	group     map[raft.ID]int // partition group; messages cross only within a group
	Now       int
	committed map[uint64]raft.Entry // index -> entry, across all nodes
	leaders   map[uint64]raft.ID    // term -> leader
	OnApply   ApplyFunc
	Sent      int
	Dropped   int
	steps     int
}

// New builds a cluster of cfg.N fresh nodes.
func New(cfg Config) *Cluster {
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = 2
	}
	if cfg.MaxDelay == 0 {
		cfg.MaxDelay = 2
	}
	c := &Cluster{cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed)), Nodes: map[raft.ID]*Node{},
		group: map[raft.ID]int{}, committed: map[uint64]raft.Entry{}, leaders: map[uint64]raft.ID{}}
	for i := 1; i <= cfg.N; i++ {
		c.IDs = append(c.IDs, raft.ID(i))
	}
	for _, id := range c.IDs {
		n := &Node{ID: id}
		c.Nodes[id] = n
		c.start(n)
	}
	return c
}

// Rand exposes the cluster's seeded RNG so workloads stay deterministic.
func (c *Cluster) Rand() *rand.Rand { return c.rng }

func (c *Cluster) start(n *Node) {
	// Each node gets its own RNG derived from the cluster seed, so timeouts
	// are randomized but reproducible.
	cfg := raft.Config{ID: n.ID, Peers: c.IDs, ElectionTick: c.cfg.ElectionTick, HeartbeatTick: c.cfg.HeartbeatTick,
		PreVote: c.cfg.PreVote, CheckQuorum: c.cfg.CheckQuorum, MaxEntriesPerMsg: c.cfg.MaxEntriesPerMsg,
		Rand: rand.New(rand.NewSource(c.rng.Int63()))}
	n.Raft = raft.New(cfg, raft.InitialState{HardState: n.st.hs, Snapshot: n.st.snap, Entries: slices.Clone(n.st.entries)})
	n.KV = kv.New()
	if n.st.snap != nil {
		if err := n.KV.Restore(n.st.snap.Data); err != nil {
			panic(err)
		}
	}
	n.Alive = true
	n.leaderTermChecked = 0
}

// Crash stops a node, discarding everything not persisted.
func (c *Cluster) Crash(id raft.ID) {
	n := c.Nodes[id]
	n.Alive = false
	n.Raft, n.KV = nil, nil
}

// Restart brings a crashed node back from its persisted state.
func (c *Cluster) Restart(id raft.ID) {
	if n := c.Nodes[id]; !n.Alive {
		c.start(n)
	}
}

// Partition splits the cluster: groups[i] are the members of group i.
// Nodes not listed are isolated alone.
func (c *Cluster) Partition(groups ...[]raft.ID) {
	c.group = map[raft.ID]int{}
	for _, id := range c.IDs {
		c.group[id] = -int(id) // isolated by default
	}
	for g, ids := range groups {
		for _, id := range ids {
			c.group[id] = g + 1
		}
	}
}

// Partitioned reports whether any partition is in effect.
func (c *Cluster) Partitioned() bool { return len(c.group) > 0 }

// Heal removes all partitions.
func (c *Cluster) Heal() { c.group = map[raft.ID]int{} }

// SetDropRate changes message loss.
func (c *Cluster) SetDropRate(p float64) { c.cfg.DropRate = p }

func (c *Cluster) connected(a, b raft.ID) bool { return c.group[a] == c.group[b] }

// Leader returns the leader with the highest term among live nodes, or None.
func (c *Cluster) Leader() raft.ID {
	var best raft.ID
	var bestTerm uint64
	for _, id := range c.IDs {
		n := c.Nodes[id]
		if n.Alive {
			if st := n.Raft.Status(); st.Role == raft.Leader && st.Term >= bestTerm {
				best, bestTerm = id, st.Term
			}
		}
	}
	return best
}

// Step advances virtual time by one tick: tick every live node, deliver due
// messages, process Ready batches, and check invariants.
func (c *Cluster) Step() error {
	c.Now++
	c.steps++
	for _, id := range c.IDs {
		if n := c.Nodes[id]; n.Alive {
			n.Raft.Tick()
		}
	}
	due := c.inflight[:0:0]
	rest := c.inflight[:0:0]
	for _, e := range c.inflight {
		if e.at <= c.Now {
			due = append(due, e)
		} else {
			rest = append(rest, e)
		}
	}
	c.inflight = rest
	for _, e := range due {
		n := c.Nodes[e.m.To]
		if n.Alive && c.connected(e.m.From, e.m.To) {
			n.Raft.Step(e.m)
		}
	}
	for _, id := range c.IDs {
		if err := c.process(c.Nodes[id]); err != nil {
			return err
		}
	}
	return c.check()
}

// Run steps n times, stopping at the first invariant violation.
func (c *Cluster) Run(n int) error {
	for i := 0; i < n; i++ {
		if err := c.Step(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) process(n *Node) error {
	for n.Alive && n.Raft.HasReady() {
		rd := n.Raft.Ready()
		// 1. Persist.
		if rd.HardStateChanged {
			n.st.hs = rd.HardState
		}
		if rd.Snapshot != nil {
			// An installed snapshot replaces the whole log: stored entries
			// after it come from a divergent history (see raft.Ready).
			n.st.snap = rd.Snapshot
			n.st.entries = nil
		}
		if len(rd.Entries) > 0 {
			first := rd.Entries[0].Index
			kept := n.st.entries[:0:0]
			for _, e := range n.st.entries {
				if e.Index < first {
					kept = append(kept, e)
				}
			}
			n.st.entries = append(kept, rd.Entries...)
		}
		// 2. Send.
		for _, m := range rd.Messages {
			c.Sent++
			// A finite network: when the in-flight queue is full, messages are
			// dropped, as a switch would. Also bounds memory when a buggy node
			// floods the network.
			if len(c.inflight) >= maxInflight {
				c.Dropped++
				continue
			}
			if c.rng.Float64() < c.cfg.DropRate {
				c.Dropped++
				continue
			}
			c.inflight = append(c.inflight, envelope{m: m, sent: c.Now, at: c.Now + 1 + c.rng.Intn(c.cfg.MaxDelay)})
			if c.cfg.DupRate > 0 && c.rng.Float64() < c.cfg.DupRate {
				// A duplicate, possibly arriving much later (e.g. a retransmit).
				c.inflight = append(c.inflight, envelope{m: m, sent: c.Now, at: c.Now + 1 + c.rng.Intn(4*c.cfg.MaxDelay)})
			}
		}
		// 3. Apply.
		if rd.Snapshot != nil {
			if err := n.KV.Restore(rd.Snapshot.Data); err != nil {
				return err
			}
		}
		for _, e := range rd.CommittedEntries {
			if prev, ok := c.committed[e.Index]; ok {
				if prev.Term != e.Term || string(prev.Data) != string(e.Data) {
					return fmt.Errorf("state machine safety: node %d applied %d@t%d, another applied %d@t%d (seed %d, t=%d)",
						n.ID, e.Index, e.Term, prev.Index, prev.Term, c.cfg.Seed, c.Now)
				}
			} else {
				c.committed[e.Index] = e
			}
			cmd, res, err := n.KV.ApplyEntry(e.Data)
			if c.OnApply != nil {
				c.OnApply(n.ID, e, cmd, res, err)
			}
		}
		n.Raft.Advance(rd)
		// 4. Compact.
		if every := c.cfg.SnapshotEvery; every > 0 {
			st := n.Raft.Status()
			if st.Applied-st.SnapIndex >= every {
				s, err := n.Raft.Compact(st.Applied, n.KV.Snapshot())
				if err != nil {
					return err
				}
				n.st.snap = s
				n.st.entries = trimTo(n.st.entries, s.Index)
			}
		}
	}
	return nil
}

func trimTo(es []raft.Entry, idx uint64) []raft.Entry {
	out := es[:0:0]
	for _, e := range es {
		if e.Index > idx {
			out = append(out, e)
		}
	}
	return out
}

// termsMonotonic reports whether a log's terms never decrease, starting from
// the snapshot's term. Any decrease means entries from different histories
// were spliced together.
func termsMonotonic(snapTerm uint64, es []raft.Entry) bool {
	prev := snapTerm
	for _, e := range es {
		if e.Term < prev {
			return false
		}
		prev = e.Term
	}
	return true
}

// check verifies Raft's safety properties (Figure 3 of the paper).
func (c *Cluster) check() error {
	for _, id := range c.IDs {
		n := c.Nodes[id]
		var snapTerm uint64
		if n.st.snap != nil {
			snapTerm = n.st.snap.Term
		}
		// Persisted state must be a single coherent history, crashed or not.
		if !termsMonotonic(snapTerm, n.st.entries) {
			return fmt.Errorf("persisted log of node %d splices histories (terms decrease) (seed %d, t=%d)", id, c.cfg.Seed, c.Now)
		}
		if !n.Alive {
			continue
		}
		st := n.Raft.Status()
		// Election safety: at most one leader per term.
		if st.Role == raft.Leader {
			if l, ok := c.leaders[st.Term]; ok && l != id {
				return fmt.Errorf("election safety: nodes %d and %d both led term %d (seed %d, t=%d)", l, id, st.Term, c.cfg.Seed, c.Now)
			}
			c.leaders[st.Term] = id
			// Leader completeness: a leader holds every committed entry.
			if n.leaderTermChecked != st.Term {
				n.leaderTermChecked = st.Term
				log := indexLog(n.Raft.Entries())
				idxs := make([]uint64, 0, len(c.committed))
				for idx := range c.committed {
					idxs = append(idxs, idx)
				}
				slices.Sort(idxs) // deterministic first-violation reporting
				for _, idx := range idxs {
					e := c.committed[idx]
					if idx <= st.SnapIndex {
						continue
					}
					if got, ok := log[idx]; !ok || got.Term != e.Term {
						return fmt.Errorf("leader completeness: leader %d (term %d) lacks committed entry %d (seed %d, t=%d)",
							id, st.Term, idx, c.cfg.Seed, c.Now)
					}
				}
			}
		}
	}
	if c.cfg.CheckEvery > 0 && c.steps%c.cfg.CheckEvery != 0 {
		return nil
	}
	// Log matching: if two logs agree on the term at an index, they agree
	// on every entry up to it.
	var logs [][]raft.Entry
	for _, id := range c.IDs {
		if n := c.Nodes[id]; n.Alive {
			logs = append(logs, n.Raft.Entries())
		}
	}
	for i := 0; i < len(logs); i++ {
		for j := i + 1; j < len(logs); j++ {
			if err := logMatching(logs[i], logs[j]); err != nil {
				return fmt.Errorf("%w (seed %d, t=%d)", err, c.cfg.Seed, c.Now)
			}
		}
	}
	return nil
}

func indexLog(es []raft.Entry) map[uint64]raft.Entry {
	m := make(map[uint64]raft.Entry, len(es))
	for _, e := range es {
		m[e.Index] = e
	}
	return m
}

func logMatching(a, b []raft.Entry) error {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	lo := max(a[0].Index, b[0].Index)
	hi := min(a[len(a)-1].Index, b[len(b)-1].Index)
	at := func(es []raft.Entry, i uint64) raft.Entry { return es[i-es[0].Index] }
	// Find the highest index where the terms agree...
	top := uint64(0)
	for i := hi; i >= lo && i > 0; i-- {
		if at(a, i).Term == at(b, i).Term {
			top = i
			break
		}
	}
	// ...then everything at or below it must be identical.
	for i := lo; top != 0 && i <= top; i++ {
		ea, eb := at(a, i), at(b, i)
		if ea.Term != eb.Term || string(ea.Data) != string(eb.Data) {
			return fmt.Errorf("log matching: logs agree at %d but differ at %d", top, i)
		}
	}
	return nil
}

// Committed returns how many distinct indexes have been applied anywhere.
func (c *Cluster) Committed() int { return len(c.committed) }
