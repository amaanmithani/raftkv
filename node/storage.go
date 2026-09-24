// Package node runs a raft.Node as a real server: file-backed storage, an
// HTTP transport between peers, and an HTTP key-value API for clients.
package node

import (
	"bufio"
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
	f, err := os.OpenFile(filepath.Join(dir, "wal"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
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
// batch's messages are sent.
func (s *Storage) Save(hs raft.HardState, hsChanged bool, snap *raft.Snapshot, entries []raft.Entry) error {
	if snap != nil {
		if err := s.SaveSnapshot(snap); err != nil {
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
	payload, err := json.Marshal(rec)
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

// SaveSnapshot persists a snapshot and compacts the WAL to entries after it.
func (s *Storage) SaveSnapshot(snap *raft.Snapshot) error {
	if err := writeJSON(filepath.Join(s.dir, "snapshot.json"), snap, s.sync); err != nil {
		return err
	}
	entries, hs, _, err := replayWAL(filepath.Join(s.dir, "wal"))
	if err != nil {
		return err
	}
	var keep []raft.Entry
	for _, e := range entries {
		if e.Index > snap.Index {
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
	if len(keep) > 0 || hs != (raft.HardState{}) {
		if err := s.appendRecord(walRecord{Entries: keep, HS: &hs}); err != nil {
			f.Close()
			s.wal = old
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, "wal")); err != nil {
		return err
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
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, hs, 0, nil
	}
	if err != nil {
		return nil, hs, 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var log []raft.Entry
	var good int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return log, hs, good, nil // clean end or torn header
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil || crc32.Checksum(payload, crcTable) != sum {
			return log, hs, good, nil // torn or corrupt tail
		}
		var rec walRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			return nil, hs, 0, fmt.Errorf("wal: corrupt record at %d: %w", good, err)
		}
		if rec.HS != nil {
			hs = *rec.HS
		}
		batch := rec.Entries
		if len(batch) > 0 {
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
