package node

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/amaanmithani/raftkv/raft"
)

type testCluster struct {
	t       *testing.T
	peers   map[raft.ID]string
	dirs    map[raft.ID]string
	servers map[raft.ID]*Server
	https   map[raft.ID]*http.Server
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, peers: map[raft.ID]string{}, dirs: map[raft.ID]string{}, servers: map[raft.ID]*Server{},
		https: map[raft.ID]*http.Server{}}
	lns := map[raft.ID]net.Listener{}
	for i := 1; i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		id := raft.ID(i)
		lns[id] = ln
		tc.peers[id] = "http://" + ln.Addr().String()
		tc.dirs[id] = t.TempDir()
	}
	for id, ln := range lns {
		tc.start(id, ln)
	}
	t.Cleanup(func() {
		for id := range tc.servers {
			tc.kill(id)
		}
	})
	return tc
}

func (tc *testCluster) start(id raft.ID, ln net.Listener) {
	tc.t.Helper()
	s, err := New(Config{ID: id, Peers: tc.peers, Dir: tc.dirs[id], Tick: 10 * time.Millisecond, Sync: true,
		SnapshotEvery: 20})
	if err != nil {
		tc.t.Fatal(err)
	}
	s.Start()
	hs := &http.Server{Handler: s.Handler()}
	go func() { _ = hs.Serve(ln) }()
	tc.servers[id], tc.https[id] = s, hs
}

// kill stops a node abruptly (like kill -9 for durability purposes: only what
// storage persisted survives).
func (tc *testCluster) kill(id raft.ID) {
	if s := tc.servers[id]; s != nil {
		_ = tc.https[id].Close()
		s.Stop()
		delete(tc.servers, id)
		delete(tc.https, id)
	}
}

func (tc *testCluster) restart(id raft.ID) {
	addr := strings.TrimPrefix(tc.peers[id], "http://")
	var ln net.Listener
	var err error
	for i := 0; i < 50; i++ { // the port may linger briefly after close
		if ln, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		tc.t.Fatal(err)
	}
	tc.start(id, ln)
}

func (tc *testCluster) leader() raft.ID {
	tc.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, s := range tc.servers {
			if st, err := s.Status(); err == nil && st.Role == raft.Leader {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatal("no leader")
	return raft.None
}

// do sends a KV request to any node, following leader hints and retrying
// with the same client id and seq (exactly-once).
func (tc *testCluster) do(method, key, body string, cid, seq uint64) map[string]any {
	tc.t.Helper()
	path := "/kv/" + key
	if method == "APPEND" {
		method, path = http.MethodPost, path+"/append"
	}
	deadline := time.Now().Add(15 * time.Second)
	target := tc.leader()
	for time.Now().Before(deadline) {
		base, ok := tc.peers[target]
		if _, alive := tc.servers[target]; !ok || !alive {
			target = tc.leader()
			continue
		}
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		req.Header.Set("X-Client-ID", fmt.Sprint(cid))
		req.Header.Set("X-Seq", fmt.Sprint(seq))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			target = tc.leader()
			continue
		}
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return out
		}
		target = tc.leader()
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatalf("%s %s never succeeded", method, key)
	return nil
}

func TestClusterSurvivesLeaderCrashAndRestart(t *testing.T) {
	tc := newTestCluster(t, 3)
	for i := 1; i <= 30; i++ {
		tc.do("APPEND", "log", fmt.Sprint(i, ","), 1, uint64(i))
	}
	old := tc.leader()
	tc.kill(old)
	for i := 31; i <= 40; i++ {
		tc.do("APPEND", "log", fmt.Sprint(i, ","), 1, uint64(i))
	}
	// Retrying an already-applied request must not append twice.
	tc.do("APPEND", "log", "40,", 1, 40)
	var want strings.Builder
	for i := 1; i <= 40; i++ {
		fmt.Fprint(&want, i, ",")
	}
	if got := tc.do(http.MethodGet, "log", "", 2, 1)["value"]; got != want.String() {
		t.Fatalf("after leader crash: %q", got)
	}
	tc.restart(old)
	// The restarted node recovers from its WAL/snapshot and catches up.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := tc.servers[old].Status()
		lst, _ := tc.servers[tc.leader()].Status()
		if st.Applied >= lst.Commit && st.Applied > 0 {
			if v, _ := tc.servers[old].kv.Get("log"); v == want.String() {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("restarted node did not catch up")
}

func TestFollowerRedirectsWithLeaderHint(t *testing.T) {
	tc := newTestCluster(t, 3)
	l := tc.leader()
	var follower raft.ID
	for id := range tc.servers {
		if id != l {
			follower = id
			break
		}
	}
	// Wait until the follower has heard from the leader, or its hint is empty.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if st, _ := tc.servers[follower].Status(); st.Leader == l {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	req, _ := http.NewRequest(http.MethodPut, tc.peers[follower]+"/kv/x", strings.NewReader("1"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMisdirectedRequest || resp.Header.Get("X-Raft-Leader") != tc.peers[l] {
		t.Fatalf("follower should redirect to %s: %d %q", tc.peers[l], resp.StatusCode, resp.Header.Get("X-Raft-Leader"))
	}
	s, _ := http.Get(tc.peers[l] + "/status")
	var st map[string]any
	_ = json.NewDecoder(s.Body).Decode(&st)
	s.Body.Close()
	if st["role"] != "leader" {
		t.Fatalf("status: %+v", st)
	}
}

func TestClientIDValidationAndStorageFailureHalts(t *testing.T) {
	tc := newTestCluster(t, 1)
	l := tc.leader()
	base := tc.peers[l]
	req, _ := http.NewRequest(http.MethodPut, base+"/kv/x", strings.NewReader("1"))
	req.Header.Set("X-Client-ID", "5") // seq missing
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("client id without seq: %d", resp.StatusCode)
	}
	// Without a client id the request still works (at-most-once).
	req, _ = http.NewRequest(http.MethodPut, base+"/kv/x", strings.NewReader("1"))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("anonymous put: %d", resp.StatusCode)
	}
	// Break storage: the next write must fail and the node must halt,
	// never acknowledging anything it couldn't persist.
	_ = tc.servers[l].store.wal.Close()
	req, _ = http.NewRequest(http.MethodPut, base+"/kv/y", strings.NewReader("2"))
	resp, err = http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("acknowledged a write after a storage failure")
		}
	}
	if !tc.servers[l].failed.Load() {
		t.Fatal("node should have halted on the storage error")
	}
}

func TestPeerTokenRequired(t *testing.T) {
	s, err := New(Config{ID: 1, Peers: map[raft.ID]string{1: "http://127.0.0.1:1"}, Dir: t.TempDir(), PeerToken: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	for token, want := range map[string]int{"": 401, "wrong": 401} {
		r, _ := http.NewRequest(http.MethodPost, "/raft", strings.NewReader(""))
		if token != "" {
			r.Header.Set("X-Raft-Token", token)
		}
		rec := &recorder{header: http.Header{}}
		h.ServeHTTP(rec, r)
		if rec.code != want {
			t.Fatalf("token %q: %d", token, rec.code)
		}
	}
	_ = s.store.Close()
}

type recorder struct {
	header http.Header
	code   int
}

func (r *recorder) Header() http.Header         { return r.header }
func (r *recorder) Write(b []byte) (int, error) { return len(b), nil }
func (r *recorder) WriteHeader(c int)           { r.code = c }
