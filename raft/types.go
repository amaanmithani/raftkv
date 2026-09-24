package raft

import (
	"errors"
	"fmt"
)

// ID identifies a node. 0 means "none".
type ID uint64

// None is the zero ID.
const None ID = 0

// Entry is one log entry. Data == nil marks the no-op a new leader appends to
// commit entries from earlier terms.
type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

// Snapshot is a compacted prefix of the log: the state machine as of Index.
type Snapshot struct {
	Index uint64
	Term  uint64
	Peers []ID
	Data  []byte
}

// HardState must be persisted before any message in the same Ready is sent.
type HardState struct {
	Term   uint64
	Vote   ID
	Commit uint64
}

// MsgType enumerates messages.
type MsgType uint8

// Message types. Heartbeats are MsgApp with no entries.
const (
	MsgPreVote MsgType = iota + 1
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgSnap
)

var msgNames = map[MsgType]string{MsgPreVote: "PreVote", MsgPreVoteResp: "PreVoteResp", MsgVote: "Vote",
	MsgVoteResp: "VoteResp", MsgApp: "App", MsgAppResp: "AppResp", MsgSnap: "Snap"}

func (t MsgType) String() string {
	if s, ok := msgNames[t]; ok {
		return s
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

// Message is exchanged between nodes. Fields are used per type.
type Message struct {
	Type MsgType
	From ID
	To   ID
	Term uint64

	// Votes.
	LastLogIndex uint64
	LastLogTerm  uint64
	Granted      bool

	// Append.
	PrevIndex uint64
	PrevTerm  uint64
	Entries   []Entry
	Commit    uint64

	// Append response. On rejection, ConflictTerm/ConflictIndex let the
	// leader skip back a whole term at a time (the "fast backup").
	Success       bool
	MatchIndex    uint64
	ConflictTerm  uint64
	ConflictIndex uint64

	// Snapshot install.
	Snapshot *Snapshot
}

// Role is a node's current role.
type Role uint8

// Roles.
const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[r]
}

// ErrNotLeader is returned by Propose on a non-leader.
var ErrNotLeader = errors.New("raft: not the leader")

// Ready is a batch of work for the driver. The driver must, in order:
// persist HardState (if HardStateChanged), Snapshot (if non-nil; a snapshot
// in Ready is one received from the leader, and ALL previously stored log
// entries must be discarded with it: they belong to a history the snapshot
// replaces) and Entries (truncating any stored entries at index >=
// Entries[0].Index first); then send Messages; then apply the Snapshot and
// CommittedEntries to the state machine; then call Advance.
type Ready struct {
	HardState        HardState
	HardStateChanged bool
	Snapshot         *Snapshot
	Entries          []Entry
	Messages         []Message
	CommittedEntries []Entry
}

// Empty reports whether the batch has no work.
func (r Ready) Empty() bool {
	return !r.HardStateChanged && r.Snapshot == nil && len(r.Entries) == 0 && len(r.Messages) == 0 &&
		len(r.CommittedEntries) == 0
}

// Status is a read-only view for tests, metrics and the visualizer.
type Status struct {
	ID        ID
	Role      Role
	Term      uint64
	Vote      ID
	Leader    ID
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	SnapIndex uint64
	Match     map[ID]uint64 // leader only
}
