package kv

import "testing"

func TestOpsAndSessions(t *testing.T) {
	s := New()
	r, err := s.Apply(Command{Op: Put, Key: "a", Value: "1", ClientID: 7, Seq: 1})
	if err != nil || r.Value != "1" {
		t.Fatal(r, err)
	}
	r, _ = s.Apply(Command{Op: Append, Key: "a", Value: "2", ClientID: 7, Seq: 2})
	if r.Value != "12" {
		t.Fatal(r)
	}
	// Retry of seq 2 must not append again.
	r, _ = s.Apply(Command{Op: Append, Key: "a", Value: "2", ClientID: 7, Seq: 2})
	if v, _ := s.Get("a"); v != "12" || r.Value != "12" {
		t.Fatalf("duplicate applied twice: %q", v)
	}
	if _, err := s.Apply(Command{Op: Append, Key: "a", Value: "x", ClientID: 7, Seq: 1}); err == nil {
		t.Fatal("stale seq accepted")
	}
	if r, _ := s.Apply(Command{Op: Get, Key: "missing"}); r.Found {
		t.Fatal("found missing key")
	}
	if _, err := s.Apply(Command{Op: "delete"}); err == nil {
		t.Fatal("unknown op")
	}
}

func TestSnapshotKeepsSessions(t *testing.T) {
	s := New()
	_, _ = s.Apply(Command{Op: Append, Key: "k", Value: "x", ClientID: 1, Seq: 5})
	r := New()
	if err := r.Restore(s.Snapshot()); err != nil {
		t.Fatal(err)
	}
	_, _ = r.Apply(Command{Op: Append, Key: "k", Value: "x", ClientID: 1, Seq: 5})
	if v, _ := r.Get("k"); v != "x" || r.Len() != 1 || r.Keys()[0] != "k" {
		t.Fatalf("restored replica re-applied a duplicate: %q", v)
	}
	if err := r.Restore([]byte("{bad")); err == nil {
		t.Fatal("bad snapshot")
	}
	if err := r.Restore([]byte("{}")); err != nil || r.Len() != 0 {
		t.Fatal("empty snapshot")
	}
}

func TestApplyEntry(t *testing.T) {
	s := New()
	if c, _, err := s.ApplyEntry(nil); err != nil || c.Op != "" {
		t.Fatal("no-op entry")
	}
	if _, _, err := s.ApplyEntry([]byte("{nope")); err == nil {
		t.Fatal("bad entry")
	}
	c := Command{Op: Put, Key: "z", Value: "1"}
	d, err := Decode(c.Encode())
	if err != nil || d != c {
		t.Fatal("round trip")
	}
}
