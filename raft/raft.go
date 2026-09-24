package raft

import (
	"fmt"
	"math/rand"
	"slices"
)

// Config configures a node.
type Config struct {
	ID    ID
	Peers []ID // every member, including ID
	// ElectionTick is the base election timeout in ticks; the actual timeout
	// is randomized in [ElectionTick, 2*ElectionTick).
	ElectionTick int
	// HeartbeatTick is the leader's heartbeat interval in ticks.
	HeartbeatTick int
	// PreVote makes a would-be candidate first check it could win before
	// bumping its term, so a node rejoining from a partition can't depose a
	// healthy leader (§9.6 of the Raft thesis).
	PreVote bool
	// CheckQuorum makes a leader step down if it hasn't heard from a quorum
	// within an election timeout, and makes followers ignore vote requests
	// while they believe a leader is alive.
	CheckQuorum bool
	// MaxEntriesPerMsg bounds an append's size (0 = 64).
	MaxEntriesPerMsg int
	// Rand drives timeout randomization; seed it for deterministic runs.
	Rand *rand.Rand
}

// InitialState restores a node from persisted state.
type InitialState struct {
	HardState HardState
	Snapshot  *Snapshot
	Entries   []Entry
}

type progress struct {
	match, next uint64
	active      bool // heard from since the last quorum check
}

// Node is a Raft participant. Not safe for concurrent use; the driver
// serialises calls.
type Node struct {
	id    ID
	peers []ID
	cfg   Config
	rng   *rand.Rand

	role   Role
	term   uint64
	vote   ID
	leader ID
	log    *raftLog

	commit  uint64
	applied uint64

	electionElapsed   int
	heartbeatElapsed  int
	randomizedTimeout int

	votes map[ID]bool
	prs   map[ID]*progress

	msgs         []Message
	pendingSnap  *Snapshot // received snapshot to hand to the driver
	lastSnapshot *Snapshot // latest local snapshot, for lagging followers
	prevHS       HardState
}

// New creates a node, restoring from st (zero value for a fresh node).
func New(cfg Config, st InitialState) *Node {
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = 1
	}
	if cfg.HeartbeatTick >= cfg.ElectionTick {
		panic("raft: HeartbeatTick must be less than ElectionTick")
	}
	if cfg.MaxEntriesPerMsg <= 0 {
		cfg.MaxEntriesPerMsg = 64
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(int64(cfg.ID)))
	}
	if !slices.Contains(cfg.Peers, cfg.ID) {
		panic("raft: Peers must include ID")
	}
	n := &Node{id: cfg.ID, peers: slices.Clone(cfg.Peers), cfg: cfg, rng: cfg.Rand}
	slices.Sort(n.peers)
	n.log = newLog(st.Snapshot, st.Entries)
	n.term, n.vote = st.HardState.Term, st.HardState.Vote
	if st.Snapshot != nil {
		n.applied = st.Snapshot.Index
		n.lastSnapshot = st.Snapshot
	}
	n.commit = max(st.HardState.Commit, n.applied)
	if n.commit > n.log.lastIndex() {
		panic(fmt.Sprintf("raft: commit %d beyond last index %d", n.commit, n.log.lastIndex()))
	}
	n.prevHS = n.hardState()
	n.becomeFollower(n.term, None)
	return n
}

func (n *Node) hardState() HardState { return HardState{Term: n.term, Vote: n.vote, Commit: n.commit} }

func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

func (n *Node) resetTimeout() {
	n.electionElapsed, n.heartbeatElapsed = 0, 0
	n.randomizedTimeout = n.cfg.ElectionTick + n.rng.Intn(n.cfg.ElectionTick)
}

func (n *Node) becomeFollower(term uint64, leader ID) {
	if term > n.term {
		n.term, n.vote = term, None
	}
	n.role, n.leader = Follower, leader
	n.resetTimeout()
}

func (n *Node) becomePreCandidate() {
	n.role, n.leader = PreCandidate, None
	n.votes = map[ID]bool{n.id: true}
	n.resetTimeout()
}

func (n *Node) becomeCandidate() {
	n.term++
	n.role, n.vote, n.leader = Candidate, n.id, None
	n.votes = map[ID]bool{n.id: true}
	n.resetTimeout()
}

func (n *Node) becomeLeader() {
	n.role, n.leader = Leader, n.id
	n.resetTimeout()
	n.prs = map[ID]*progress{}
	for _, p := range n.peers {
		n.prs[p] = &progress{next: n.log.lastIndex() + 1, active: true}
	}
	// A no-op in the new term lets entries from earlier terms commit (§5.4.2).
	n.appendEntries(Entry{Data: nil})
	n.broadcastAppend()
}

// Tick advances the logical clock by one tick.
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		n.electionElapsed++
		if n.cfg.CheckQuorum && n.electionElapsed >= n.cfg.ElectionTick {
			n.electionElapsed = 0
			if !n.checkQuorumActive() {
				n.becomeFollower(n.term, None)
				return
			}
		}
		if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.randomizedTimeout {
		n.Campaign()
	}
}

func (n *Node) checkQuorumActive() bool {
	active := 0
	for id, pr := range n.prs {
		if id == n.id || pr.active {
			active++
		}
		pr.active = false
	}
	return active >= n.quorum()
}

// Campaign starts an election now (PreVote first, if enabled).
func (n *Node) Campaign() {
	if n.role == Leader {
		return
	}
	if n.cfg.PreVote {
		n.becomePreCandidate()
		n.requestVotes(MsgPreVote, n.term+1)
	} else {
		n.becomeCandidate()
		n.requestVotes(MsgVote, n.term)
	}
}

func (n *Node) requestVotes(t MsgType, term uint64) {
	if len(n.votes) >= n.quorum() { // single-node cluster
		n.winVote(t)
		return
	}
	for _, p := range n.peers {
		if p != n.id {
			n.send(Message{Type: t, To: p, Term: term, LastLogIndex: n.log.lastIndex(), LastLogTerm: n.log.lastTerm()})
		}
	}
}

func (n *Node) winVote(t MsgType) {
	if t == MsgPreVote {
		n.becomeCandidate()
		n.requestVotes(MsgVote, n.term)
		return
	}
	n.becomeLeader()
}

func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term
	}
	n.msgs = append(n.msgs, m)
}

// inLease reports whether this node recently heard from a live leader.
func (n *Node) inLease() bool {
	return n.cfg.CheckQuorum && n.leader != None && n.electionElapsed < n.cfg.ElectionTick
}

// Step processes an incoming message.
func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.term:
		if (m.Type == MsgPreVote || m.Type == MsgVote) && n.inLease() {
			return // leader stickiness: don't let a partitioned node depose a live leader
		}
		switch {
		case m.Type == MsgPreVote:
			// A pre-vote doesn't change our term.
		case m.Type == MsgPreVoteResp && m.Granted:
			// A granted pre-vote response carries the proposed term.
		case m.Type == MsgApp || m.Type == MsgSnap:
			n.becomeFollower(m.Term, m.From)
		default:
			n.becomeFollower(m.Term, None)
		}
	case m.Term < n.term:
		switch m.Type {
		case MsgApp, MsgSnap:
			// Tell a stale leader about the newer term so it steps down.
			n.send(Message{Type: MsgAppResp, To: m.From})
		case MsgPreVote:
			n.send(Message{Type: MsgPreVoteResp, To: m.From, Granted: false})
		}
		return
	}

	switch m.Type {
	case MsgPreVote, MsgVote:
		n.handleVoteRequest(m)
	case MsgPreVoteResp, MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.followLeader(m.From)
		n.handleAppend(m)
	case MsgSnap:
		n.followLeader(m.From)
		n.handleSnapshot(m)
	case MsgAppResp:
		n.handleAppendResp(m)
	}
}

func (n *Node) followLeader(from ID) {
	if n.role != Follower {
		n.becomeFollower(n.term, from)
	}
	n.leader = from
	n.electionElapsed = 0
}

func (n *Node) handleVoteRequest(m Message) {
	upToDate := n.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)
	if m.Type == MsgPreVote {
		// Grant if the candidate could win a real election with that term.
		granted := upToDate && m.Term > n.term
		resp := Message{Type: MsgPreVoteResp, To: m.From, Granted: granted}
		if granted {
			resp.Term = m.Term
		}
		n.send(resp)
		return
	}
	canVote := n.vote == m.From || (n.vote == None && n.leader == None)
	granted := canVote && upToDate
	if granted {
		n.vote = m.From
		n.electionElapsed = 0
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Granted: granted})
}

func (n *Node) handleVoteResp(m Message) {
	want := MsgVoteResp
	if n.role == PreCandidate {
		want = MsgPreVoteResp
	} else if n.role != Candidate {
		return
	}
	if m.Type != want {
		return
	}
	n.votes[m.From] = m.Granted
	granted, rejected := 0, 0
	for _, g := range n.votes {
		if g {
			granted++
		} else {
			rejected++
		}
	}
	switch {
	case granted >= n.quorum():
		if want == MsgPreVoteResp {
			n.winVote(MsgPreVote)
		} else {
			n.winVote(MsgVote)
		}
	case rejected >= n.quorum():
		n.becomeFollower(n.term, None)
	}
}

func (n *Node) handleAppend(m Message) {
	prevIndex, prevTerm, entries := m.PrevIndex, m.PrevTerm, m.Entries
	// Entries at or below our snapshot are committed, so they match.
	if prevIndex < n.log.snapIndex {
		skip := n.log.snapIndex - prevIndex
		if uint64(len(entries)) <= skip {
			n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: n.log.snapIndex})
			return
		}
		entries = entries[skip:]
		prevIndex, prevTerm = n.log.snapIndex, n.log.snapTerm
	}
	if prevIndex > n.log.lastIndex() {
		n.send(Message{Type: MsgAppResp, To: m.From, ConflictIndex: n.log.lastIndex() + 1})
		return
	}
	if t, _ := n.log.term(prevIndex); t != prevTerm {
		n.send(Message{Type: MsgAppResp, To: m.From, ConflictTerm: t,
			ConflictIndex: n.log.firstIndexOfTerm(t, prevIndex)})
		return
	}
	for i, e := range entries {
		if e.Index <= n.log.lastIndex() {
			if t, _ := n.log.term(e.Index); t == e.Term {
				continue
			}
			if e.Index <= n.commit {
				panic(fmt.Sprintf("raft: node %d asked to overwrite committed entry %d", n.id, e.Index))
			}
			n.log.truncateFrom(e.Index)
		}
		n.log.append(entries[i:]...)
		break
	}
	lastNew := prevIndex + uint64(len(entries))
	if c := min(m.Commit, lastNew); c > n.commit {
		n.commit = c
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew})
}

func (n *Node) handleSnapshot(m Message) {
	s := m.Snapshot
	if s.Index <= n.commit {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: n.commit})
		return
	}
	if t, ok := n.log.term(s.Index); ok && t == s.Term {
		// We already have the entries; just commit up to the snapshot.
		n.commit = s.Index
	} else {
		n.log.restore(s)
		n.commit, n.applied = s.Index, s.Index
		n.pendingSnap = s
		n.lastSnapshot = s
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: s.Index})
}

func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader {
		return
	}
	pr := n.prs[m.From]
	if pr == nil {
		return
	}
	pr.active = true
	if m.Success {
		if m.MatchIndex > pr.match {
			pr.match = m.MatchIndex
		}
		pr.next = max(pr.next, pr.match+1)
		if n.maybeCommit() {
			n.broadcastAppend() // tell followers the new commit index promptly
		} else if pr.next <= n.log.lastIndex() {
			n.sendAppend(m.From)
		}
		return
	}
	next := m.ConflictIndex
	if m.ConflictTerm != 0 {
		if li := n.log.lastIndexOfTerm(m.ConflictTerm); li != 0 {
			next = li + 1
		}
	}
	pr.next = max(pr.match+1, min(next, pr.next), 1)
	n.sendAppend(m.From)
}

func (n *Node) maybeCommit() bool {
	matches := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		if p == n.id {
			matches = append(matches, n.log.lastIndex())
		} else {
			matches = append(matches, n.prs[p].match)
		}
	}
	slices.Sort(matches)
	idx := matches[len(matches)-n.quorum()]
	// Only entries from the current term commit by counting replicas (§5.4.2).
	if t, _ := n.log.term(idx); idx > n.commit && t == n.term {
		n.commit = idx
		return true
	}
	return false
}

func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
}

func (n *Node) sendAppend(to ID) {
	pr := n.prs[to]
	prevIndex := pr.next - 1
	prevTerm, ok := n.log.term(prevIndex)
	if !ok {
		// The entries the follower needs were compacted: send the snapshot.
		if n.lastSnapshot == nil {
			panic("raft: compacted log without a snapshot")
		}
		n.send(Message{Type: MsgSnap, To: to, Snapshot: n.lastSnapshot})
		pr.next = n.lastSnapshot.Index + 1
		return
	}
	hi := min(n.log.lastIndex(), prevIndex+uint64(n.cfg.MaxEntriesPerMsg))
	var ents []Entry
	if hi > prevIndex {
		ents = slices.Clone(n.log.slice(prevIndex+1, hi))
	}
	n.send(Message{Type: MsgApp, To: to, PrevIndex: prevIndex, PrevTerm: prevTerm, Entries: ents, Commit: n.commit})
	// Optimistically pipeline: assume these will be accepted.
	if len(ents) > 0 {
		pr.next = hi + 1
	}
}

func (n *Node) appendEntries(es ...Entry) {
	for i := range es {
		es[i].Term, es[i].Index = n.term, n.log.lastIndex()+1+uint64(i)
	}
	n.log.append(es...)
	if len(n.peers) == 1 {
		n.maybeCommit()
	}
}

// Propose appends data to the log if this node is the leader and returns the
// entry's index and term. Being appended is not being committed: the entry
// can still be lost if leadership changes before a quorum stores it.
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	if data == nil {
		data = []byte{}
	}
	n.appendEntries(Entry{Data: data})
	n.broadcastAppend()
	return n.log.lastIndex(), n.term, nil
}

// Compact records a snapshot of the state machine as of index (which must be
// applied) and drops the log prefix it covers. The driver persists the
// snapshot itself.
func (n *Node) Compact(index uint64, data []byte) (*Snapshot, error) {
	if index > n.applied {
		return nil, fmt.Errorf("raft: compact %d beyond applied %d", index, n.applied)
	}
	if index <= n.log.snapIndex {
		return n.lastSnapshot, nil
	}
	t, _ := n.log.term(index)
	s := &Snapshot{Index: index, Term: t, Peers: slices.Clone(n.peers), Data: data}
	n.log.compact(index, t)
	n.lastSnapshot = s
	return s, nil
}

// HasReady reports whether Ready would return work.
func (n *Node) HasReady() bool {
	return n.hardState() != n.prevHS || n.pendingSnap != nil || n.log.unstable <= n.log.lastIndex() ||
		len(n.msgs) > 0 || n.commit > n.applied
}

// Ready returns pending work. Call Advance after handling it.
func (n *Node) Ready() Ready {
	rd := Ready{HardState: n.hardState(), Snapshot: n.pendingSnap, Messages: n.msgs}
	rd.HardStateChanged = rd.HardState != n.prevHS
	if n.log.unstable <= n.log.lastIndex() {
		rd.Entries = slices.Clone(n.log.slice(max(n.log.unstable, n.log.snapIndex+1), n.log.lastIndex()))
	}
	if n.commit > n.applied {
		rd.CommittedEntries = slices.Clone(n.log.slice(n.applied+1, n.commit))
	}
	return rd
}

// Advance acknowledges that rd was handled.
func (n *Node) Advance(rd Ready) {
	n.prevHS = rd.HardState
	if len(rd.Entries) > 0 {
		n.log.unstable = max(n.log.unstable, rd.Entries[len(rd.Entries)-1].Index+1)
	}
	if len(rd.CommittedEntries) > 0 {
		n.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	if rd.Snapshot != nil && n.pendingSnap == rd.Snapshot {
		n.pendingSnap = nil
	}
	n.msgs = n.msgs[len(rd.Messages):]
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	s := Status{ID: n.id, Role: n.role, Term: n.term, Vote: n.vote, Leader: n.leader, Commit: n.commit,
		Applied: n.applied, LastIndex: n.log.lastIndex(), SnapIndex: n.log.snapIndex}
	if n.role == Leader {
		s.Match = map[ID]uint64{}
		for id, pr := range n.prs {
			s.Match[id] = pr.match
		}
		s.Match[n.id] = n.log.lastIndex()
	}
	return s
}

// Entries returns a copy of the live log (for invariant checks).
func (n *Node) Entries() []Entry { return slices.Clone(n.log.entries) }
