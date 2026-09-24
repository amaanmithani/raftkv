// Package kv is the replicated state machine: a string map with Get, Put and
// Append, plus client sessions so a retried command is applied exactly once.
package kv

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Op is a command kind.
type Op string

// Operations.
const (
	Get    Op = "get"
	Put    Op = "put"
	Append Op = "append"
)

// Command is what clients propose. (ClientID, Seq) identifies a request
// across retries; Seq must increase per client.
type Command struct {
	Op       Op     `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID uint64 `json:"cid"`
	Seq      uint64 `json:"seq"`
}

// Encode serialises a command for the log.
func (c Command) Encode() []byte {
	b, _ := json.Marshal(c)
	return b
}

// Decode parses a log entry.
func Decode(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

// Result is what a command returned.
type Result struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type session struct {
	Seq    uint64 `json:"seq"`
	Result Result `json:"result"`
}

// Store is the state machine. Not safe for concurrent use.
type Store struct {
	data     map[string]string
	sessions map[uint64]session
}

// New returns an empty store.
func New() *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]session{}}
}

// Apply executes a committed command. A command whose (ClientID, Seq) was
// already applied returns the recorded result without re-executing: that is
// what makes client retries safe. Commands with Seq below the client's last
// are stale duplicates and return an error.
func (s *Store) Apply(c Command) (Result, error) {
	if c.ClientID != 0 {
		if sess, ok := s.sessions[c.ClientID]; ok {
			switch {
			case c.Seq == sess.Seq:
				return sess.Result, nil
			case c.Seq < sess.Seq:
				return Result{}, fmt.Errorf("kv: stale request seq %d < %d for client %d", c.Seq, sess.Seq, c.ClientID)
			}
		}
	}
	var r Result
	switch c.Op {
	case Get:
		r.Value, r.Found = s.data[c.Key]
	case Put:
		s.data[c.Key] = c.Value
		r = Result{Value: c.Value, Found: true}
	case Append:
		s.data[c.Key] += c.Value
		r = Result{Value: s.data[c.Key], Found: true}
	default:
		return Result{}, fmt.Errorf("kv: unknown op %q", c.Op)
	}
	if c.ClientID != 0 {
		s.sessions[c.ClientID] = session{Seq: c.Seq, Result: r}
	}
	return r, nil
}

// ApplyEntry decodes and applies a log entry. Empty entries (leader no-ops)
// are ignored.
func (s *Store) ApplyEntry(data []byte) (Command, Result, error) {
	if len(data) == 0 {
		return Command{}, Result{}, nil
	}
	c, err := Decode(data)
	if err != nil {
		return Command{}, Result{}, fmt.Errorf("kv: decode: %w", err)
	}
	r, err := s.Apply(c)
	return c, r, err
}

// Get reads a key directly (not linearizable on its own; tests only).
func (s *Store) Get(key string) (string, bool) {
	v, ok := s.data[key]
	return v, ok
}

// Len returns the number of keys.
func (s *Store) Len() int { return len(s.data) }

type snapshot struct {
	Data     map[string]string  `json:"data"`
	Sessions map[uint64]session `json:"sessions"`
}

// Snapshot serialises the full state, sessions included: a restored replica
// must still reject duplicates the old one had seen.
func (s *Store) Snapshot() []byte {
	b, _ := json.Marshal(snapshot{Data: s.data, Sessions: s.sessions})
	return b
}

// Restore replaces the state with a snapshot.
func (s *Store) Restore(b []byte) error {
	var sn snapshot
	if err := json.Unmarshal(b, &sn); err != nil {
		return fmt.Errorf("kv: restore: %w", err)
	}
	if sn.Data == nil {
		sn.Data = map[string]string{}
	}
	if sn.Sessions == nil {
		sn.Sessions = map[uint64]session{}
	}
	s.data, s.sessions = sn.Data, sn.Sessions
	return nil
}

// Keys returns sorted keys (for tests and the visualizer).
func (s *Store) Keys() []string {
	out := make([]string, 0, len(s.data))
	for k := range s.data {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
