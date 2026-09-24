// Package node runs a raft.Node as a real server: file-backed storage, an
// HTTP transport between peers, and an HTTP key-value API for clients.
package node

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/amaanmithani/raftkv/raft"
)

// Storage persists hard state, the latest snapshot and the log.
//
// Layout in Dir:
//
//	snapshot.json  latest snapshot, replaced atomically
//	wal            append-only records. A record carries a batch of entries
//	               (replay truncates the log at the batch's first index, so
//	               overwriting a conflicting suffix needs no in-place edits)
//	               and/or the hard state (last one wins). One Ready batch is
//	               one record and one fsync: group commit.
//
// Every WAL record is length-prefixed and CRC32-checked; a torn final record
// (crash mid-write) is detected and discarded on open.
type Storage struct {
	dir  string
	sync bool
	wal  *os.File
	st   raft.InitialState
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Open loads (or creates) storage in dir. sync controls fsync after writes;
// turning it off trades durability for speed (benchmarks only).
func Open(dir string, sync bool) (*Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Storage{dir: dir, sync: sync}
	var snap raft.Snapshot
	if err := readJSON(filepath.Join(dir, "snapshot.json"), &snap); err != nil {
		return nil, err
	}
	if snap.Index > 0 {
		s.st.Snapshot = &snap
	}
	entries, hs, good, err := replayWAL(filepath.Join(dir, "wal"))
	if err != nil {
		return nil, err
	}
	s.st.HardState = hs
	var snapIdx uint64
	if s.st.Snapshot != nil {
		snapIdx = s.st.Snapshot.Index
	}
	for _, e := range entries {
		if e.Index > snapIdx {
			s.st.Entries = append(s.st.Entries, e)
		}
	}
	_, statErr := os.Stat(filepath.Join(dir, "wal"))
	f, err := os.OpenFile(filepath.Join(dir, "wal"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if errors.Is(statErr, os.ErrNotExist) {
		// A new file's directory entry must be durable before anything in
		// the file (e.g. a vote) is relied on.
		if err := syncDir(dir, sync); err != nil {
			return nil, err
		}
	}
	// Drop a torn tail so new records follow the last good one.
	if err := f.Truncate(good); err != nil {
		return nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		return nil, err
	}
	s.wal = f
	return s, nil
}

// Initial returns the state to start raft.New with.
func (s *Storage) Initial() raft.InitialState { return s.st }

// Save persists a Ready batch's durable parts. It must complete before the
// batch's messages are sent. A snapshot here was received from the leader and
// replaces the whole log: stored entries are discarded with it.
func (s *Storage) Save(hs raft.HardState, hsChanged bool, snap *raft.Snapshot, entries []raft.Entry) error {
	if snap != nil {
		if err := s.saveSnapshot(snap, false); err != nil {
			return err
		}
	}
	if len(entries) == 0 && !hsChanged {
		return nil
	}
	rec := walRecord{Entries: entries}
	if hsChanged {
		rec.HS = &hs
	}
	return s.appendRecord(rec)
}

// walRecord is one WAL record.
type walRecord struct {
	Entries []raft.Entry    `json:"e,omitempty"`
	HS      *raft.HardState `json:"h,omitempty"`
}

func (s *Storage) appendRecord(rec walRecord) error {
	payload, err := json.Marshal(rec) // never empty: at least "{}"
	if err != nil {
		return err
	}
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, crcTable))
	if _, err := s.wal.Write(append(hdr, payload...)); err != nil {
		return err
	}
	if s.sync {
		return s.wal.Sync()
	}
	return nil
}

// SaveSnapshot persists a locally taken snapshot (log compaction) and drops
// the WAL entries it covers.
func (s *Storage) SaveSnapshot(snap *raft.Snapshot) error { return s.saveSnapshot(snap, true) }

// saveSnapshot writes the snapshot, then rewrites the WAL. keepSuffix keeps
// entries after the snapshot (compaction); an installed snapshot replaces the
// whole log, since entries after it may come from a divergent history.
func (s *Storage) saveSnapshot(snap *raft.Snapshot, keepSuffix bool) error {
	if err := writeJSON(filepath.Join(s.dir, "snapshot.json"), snap, s.sync); err != nil {
		return err
	}
	entries, hs, _, err := replayWAL(filepath.Join(s.dir, "wal"))
	if err != nil {
		return err
	}
	var keep []raft.Entry
	for _, e := range entries {
		if keepSuffix && e.Index > snap.Index {
			keep = append(keep, e)
		}
	}
	// Rewrite the WAL atomically: write a new file, then rename over.
	tmp := filepath.Join(s.dir, "wal.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	old := s.wal
	s.wal = f
	restore := func(err error) error {
		f.Close()
		s.wal = old
		return err
	}
	if len(keep) > 0 || hs != (raft.HardState{}) {
		if err := s.appendRecord(walRecord{Entries: keep, HS: &hs}); err != nil {
			return restore(err)
		}
	}
	if s.sync {
		if err := f.Sync(); err != nil {
			return restore(err)
		}
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, "wal")); err != nil {
		return restore(err)
	}
	old.Close()
	return syncDir(s.dir, s.sync)
}

// Close releases the WAL file.
func (s *Storage) Close() error { return s.wal.Close() }

// replayWAL reads every intact record, applying truncate-then-append
// semantics. It returns the entries, the latest hard state and the byte
// offset of the end of the last intact record.
func replayWAL(path string) ([]raft.Entry, raft.HardState, int64, error) {
	var hs raft.HardState
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, hs, 0, nil
	}
	if err != nil {
		return nil, hs, 0, err
	}
	var log []raft.Entry
	var good int64
	for {
		rest := data[good:]
		if len(rest) == 0 {
			return log, hs, good, nil
		}
		// A record is bad if its header is incomplete, its length is zero or
		// absurd, its payload is short, or its checksum/JSON is wrong.
		bad := len(rest) < 8
		var n uint32
		if !bad {
			n = binary.LittleEndian.Uint32(rest[0:4])
			bad = n == 0 || n > maxRecord || int64(len(rest)) < 8+int64(n)
		}
		var rec walRecord
		if !bad {
			payload := rest[8 : 8+n]
			bad = crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(rest[4:8]) ||
				json.Unmarshal(payload, &rec) != nil
		}
		if bad {
			// A torn final write leaves a short or zero-filled tail. Anything
			// else means records after this one would be silently lost:
			// refuse to start rather than drop acknowledged data.
			if tornTail(rest, n) {
				return log, hs, good, nil
			}
			return nil, hs, 0, fmt.Errorf("wal: corrupt record at offset %d with data after it; refusing to truncate", good)
		}
		if rec.HS != nil {
			hs = *rec.HS
		}
		if batch := rec.Entries; len(batch) > 0 {
			first := batch[0].Index
			cut := len(log)
			for i, e := range log {
				if e.Index >= first {
					cut = i
					break
				}
			}
			log = append(log[:cut], batch...)
		}
		good += int64(8 + n)
	}
}

// maxRecord bounds a single WAL record (a Ready batch).
const maxRecord = 64 << 20

// tornTail reports whether a bad record at the start of rest can be the
// remains of an interrupted final write: the claimed record runs to or past
// the end of the file, or everything left is zero bytes.
func tornTail(rest []byte, n uint32) bool {
	if len(rest) < 8 || int64(len(rest)) <= 8+int64(n) {
		return true
	}
	for _, b := range rest {
		if b != 0 {
			return false
		}
	}
	return true
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// writeJSON replaces path atomically (temp file + fsync + rename + dir fsync).
func writeJSON(path string, v any, sync bool) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path), sync)
}

func syncDir(dir string, sync bool) error {
	if !sync {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
