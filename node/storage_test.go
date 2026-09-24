package node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/amaanmithani/raftkv/raft"
)

func ents(term uint64, from, to uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Term: term, Index: i, Data: []byte{byte(i)}})
	}
	return out
}

func TestStorageRoundTripAndTruncation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.Save(raft.HardState{Term: 2, Vote: 1, Commit: 3}, true, nil, ents(1, 1, 5)))
	must(s.Save(raft.HardState{}, false, nil, ents(2, 4, 6))) // overwrite 4..5, add 6
	must(s.Close())
	r, err := Open(dir, true)
	must(err)
	st := r.Initial()
	if st.HardState != (raft.HardState{Term: 2, Vote: 1, Commit: 3}) || len(st.Entries) != 6 ||
		st.Entries[3].Term != 2 || st.Entries[5].Index != 6 || st.Entries[2].Term != 1 {
		t.Fatalf("replay: %+v", st)
	}
	must(r.Close())
}

func TestStorageTornTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, false)
	_ = s.Save(raft.HardState{}, false, nil, ents(1, 1, 3))
	_ = s.Close()
	f, _ := os.OpenFile(filepath.Join(dir, "wal"), os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.Write([]byte{200, 0, 0, 0, 1, 2}) // half a header + garbage
	_ = f.Close()
	r, err := Open(dir, false)
	if err != nil || len(r.Initial().Entries) != 3 {
		t.Fatalf("torn tail: %v %+v", err, r.Initial())
	}
	// New writes land after the last good record, not after the garbage.
	_ = r.Save(raft.HardState{}, false, nil, ents(1, 4, 4))
	_ = r.Close()
	r2, _ := Open(dir, false)
	if n := len(r2.Initial().Entries); n != 4 {
		t.Fatalf("after torn tail + append: %d entries", n)
	}
	_ = r2.Close()
}

func TestStorageSnapshotCompactsWAL(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, true)
	_ = s.Save(raft.HardState{Term: 1}, true, nil, ents(1, 1, 10))
	before, _ := os.Stat(filepath.Join(dir, "wal"))
	if err := s.SaveSnapshot(&raft.Snapshot{Index: 8, Term: 1, Data: []byte("sm")}); err != nil {
		t.Fatal(err)
	}
	_ = s.Save(raft.HardState{}, false, nil, ents(1, 11, 11))
	after, _ := os.Stat(filepath.Join(dir, "wal"))
	if after.Size() >= before.Size() {
		t.Fatalf("WAL not compacted: %d -> %d", before.Size(), after.Size())
	}
	_ = s.Close()
	r, _ := Open(dir, true)
	st := r.Initial()
	if st.Snapshot == nil || st.Snapshot.Index != 8 || len(st.Entries) != 3 || st.Entries[0].Index != 9 {
		t.Fatalf("restore after compaction: %+v", st)
	}
	_ = r.Close()
}

func TestStorageCorruptRecordIsTreatedAsTornTail(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, false)
	_ = s.Save(raft.HardState{}, false, nil, ents(1, 1, 2))
	_ = s.Save(raft.HardState{}, false, nil, ents(1, 3, 3))
	_ = s.Close()
	b, _ := os.ReadFile(filepath.Join(dir, "wal"))
	b[len(b)-2] ^= 0xFF // flip bits in the last record's payload
	_ = os.WriteFile(filepath.Join(dir, "wal"), b, 0o644)
	r, _ := Open(dir, false)
	if n := len(r.Initial().Entries); n != 2 {
		t.Fatalf("CRC mismatch should drop the last record: %d entries", n)
	}
	_ = r.Close()
}
