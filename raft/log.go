package raft

import "fmt"

// raftLog holds the in-memory log after the last snapshot. Entry i (1-based
// Raft index) lives at entries[i-snapIndex-1].
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry
	// unstable is the first index not yet handed to the driver for
	// persistence (lastIndex+1 when everything is stable).
	unstable uint64
}

func newLog(snap *Snapshot, entries []Entry) *raftLog {
	l := &raftLog{}
	if snap != nil {
		l.snapIndex, l.snapTerm = snap.Index, snap.Term
	}
	for _, e := range entries {
		if e.Index <= l.snapIndex {
			continue
		}
		if e.Index != l.lastIndex()+1 {
			panic(fmt.Sprintf("raft: restored log has a gap at %d (last %d)", e.Index, l.lastIndex()))
		}
		l.entries = append(l.entries, e)
	}
	l.unstable = l.lastIndex() + 1
	return l
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 { t, _ := l.term(l.lastIndex()); return t }

// term returns the term of the entry at i. ok is false if i was compacted
// away (i < snapIndex) or doesn't exist yet.
func (l *raftLog) term(i uint64) (uint64, bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	default:
		return l.entries[i-l.snapIndex-1].Term, true
	}
}

// slice returns entries [lo, hi] inclusive (bounds must be in the live log).
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo > hi {
		return nil
	}
	return l.entries[lo-l.snapIndex-1 : hi-l.snapIndex]
}

// append adds entries whose first index is lastIndex+1.
func (l *raftLog) append(es ...Entry) {
	if len(es) == 0 {
		return
	}
	if es[0].Index != l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: append at %d, last is %d", es[0].Index, l.lastIndex()))
	}
	l.entries = append(l.entries, es...)
	if es[0].Index < l.unstable {
		l.unstable = es[0].Index
	}
}

// truncateFrom removes entries at index >= i.
func (l *raftLog) truncateFrom(i uint64) {
	l.entries = l.entries[:i-l.snapIndex-1]
	if i < l.unstable {
		l.unstable = i
	}
}

// firstIndexOfTerm returns the first index at or after lo whose term is t,
// scanning back from hi. Used to compute the conflict hint.
func (l *raftLog) firstIndexOfTerm(t, hi uint64) uint64 {
	i := hi
	for i > l.snapIndex+1 {
		if pt, _ := l.term(i - 1); pt != t {
			break
		}
		i--
	}
	return i
}

// lastIndexOfTerm returns the last index with term t, or 0.
func (l *raftLog) lastIndexOfTerm(t uint64) uint64 {
	for i := l.lastIndex(); i > l.snapIndex; i-- {
		tt, _ := l.term(i)
		if tt == t {
			return i
		}
		if tt < t {
			return 0
		}
	}
	return 0
}

// isUpToDate implements §5.4.1: is a candidate's log at least as up to date?
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	lt := l.lastTerm()
	return lastTerm > lt || (lastTerm == lt && lastIndex >= l.lastIndex())
}

// compact drops entries up to and including i, which becomes the snapshot point.
func (l *raftLog) compact(i, term uint64) {
	if i <= l.snapIndex {
		return
	}
	if i >= l.lastIndex() {
		l.entries = nil
	} else {
		l.entries = append([]Entry(nil), l.entries[i-l.snapIndex:]...)
	}
	l.snapIndex, l.snapTerm = i, term
	if l.unstable < i+1 {
		l.unstable = i + 1
	}
}

// restore replaces the whole log with a snapshot point.
func (l *raftLog) restore(s *Snapshot) {
	l.entries = nil
	l.snapIndex, l.snapTerm = s.Index, s.Term
	l.unstable = s.Index + 1
}
