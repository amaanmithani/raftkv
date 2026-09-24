package node

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amaanmithani/raftkv/kv"
	"github.com/amaanmithani/raftkv/raft"
)

// Config configures a server.
type Config struct {
	ID            raft.ID
	Peers         map[raft.ID]string // id -> base URL (http://host:port), including self
	Dir           string
	Tick          time.Duration // default 50ms
	ElectionTick  int           // in ticks, default 10
	HeartbeatTick int           // default 2
	SnapshotEvery uint64        // default 1000 applied entries
	Sync          bool          // fsync WAL and state (default true via NewConfig)
	Logger        *slog.Logger
	// RequestTimeout bounds how long a client request waits to commit.
	RequestTimeout time.Duration
	// PeerToken, if set, must be presented by peers on /raft (X-Raft-Token).
	PeerToken string
}

// Server is one raftkv node.
type Server struct {
	cfg     Config
	raft    *raft.Node
	store   *Storage
	kv      *kv.Store
	client  *http.Client
	inbox   chan raft.Message
	props   chan *proposal
	statusC chan chan raft.Status
	stop    chan struct{}
	done    chan struct{}
	senders map[raft.ID]chan raft.Message
	waiters map[uint64]*proposal // log index -> proposal waiting on it
	stopped atomic.Bool
	failed  atomic.Bool // storage error: loop has exited
	wg      sync.WaitGroup
}

type proposal struct {
	cmd   kv.Command
	index uint64
	term  uint64
	res   chan result
}

type result struct {
	val kv.Result
	err error
}

// Errors returned to clients.
var (
	ErrNotLeader = errors.New("not the leader")
	// ErrUnknownOutcome: leadership changed or a snapshot replaced the log
	// before this node saw the request apply. It may or may not have taken
	// effect; only a retry with the same X-Client-ID and X-Seq is safe.
	ErrUnknownOutcome = errors.New("outcome unknown: the request may or may not have been applied; retry only with the same X-Client-ID and X-Seq")
	ErrStopped        = errors.New("server stopped")
)

// New opens storage and restores state, but doesn't start the loop.
func New(cfg Config) (*Server, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 50 * time.Millisecond
	}
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = 2
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 1000
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("peers must include self (%d)", cfg.ID)
	}
	st, err := Open(cfg.Dir, cfg.Sync)
	if err != nil {
		return nil, err
	}
	init := st.Initial()
	store := kv.New()
	if init.Snapshot != nil {
		if err := store.Restore(init.Snapshot.Data); err != nil {
			return nil, err
		}
	}
	var ids []raft.ID
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	r := raft.New(raft.Config{ID: cfg.ID, Peers: ids, ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		PreVote: true, CheckQuorum: true, Rand: rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID)))}, init)
	s := &Server{cfg: cfg, raft: r, store: st, kv: store,
		client: &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 16}},
		inbox:  make(chan raft.Message, 4096), props: make(chan *proposal, 1024), statusC: make(chan chan raft.Status),
		stop: make(chan struct{}), done: make(chan struct{}), senders: map[raft.ID]chan raft.Message{},
		waiters: map[uint64]*proposal{}}
	return s, nil
}

// Start runs the event loop and per-peer senders.
func (s *Server) Start() {
	for id, url := range s.cfg.Peers {
		if id == s.cfg.ID {
			continue
		}
		ch := make(chan raft.Message, 1024)
		s.senders[id] = ch
		s.wg.Add(1)
		go s.sendLoop(url, ch)
	}
	go s.loop()
}

// Stop halts the server. In-flight client requests get ErrStopped.
func (s *Server) Stop() {
	if s.stopped.Swap(true) {
		return
	}
	close(s.stop)
	<-s.done
	for _, ch := range s.senders {
		close(ch)
	}
	s.wg.Wait()
	s.client.CloseIdleConnections()
	_ = s.store.Close()
}

// loop owns the raft node: every raft call happens on this goroutine.
func (s *Server) loop() {
	defer close(s.done)
	t := time.NewTicker(s.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			for _, p := range s.waiters {
				p.res <- result{err: ErrStopped}
			}
			return
		case <-t.C:
			s.raft.Tick()
		case m := <-s.inbox:
			s.raft.Step(m)
		case p := <-s.props:
			s.propose(p)
		case c := <-s.statusC:
			c <- s.raft.Status()
			continue
		}
		// Group commit: absorb everything already queued before persisting,
		// so concurrent proposals share one WAL record and one fsync.
	drain:
		for i := 0; i < 1024; i++ {
			select {
			case m := <-s.inbox:
				s.raft.Step(m)
			case p := <-s.props:
				s.propose(p)
			default:
				break drain
			}
		}
		if err := s.handleReady(); err != nil {
			// Never retry a failed write or fsync (the page cache may lie
			// about what reached disk): fail everything and halt the node.
			s.cfg.Logger.Error("fatal storage error; node halted", "err", err)
			for _, p := range s.waiters {
				p.res <- result{err: ErrStopped}
			}
			s.waiters = map[uint64]*proposal{}
			s.failed.Store(true)
			return
		}
	}
}

func (s *Server) propose(p *proposal) {
	idx, term, err := s.raft.Propose(p.cmd.Encode())
	if err != nil {
		p.res <- result{err: ErrNotLeader}
		return
	}
	p.index, p.term = idx, term
	s.waiters[idx] = p
}

func (s *Server) handleReady() error {
	for s.raft.HasReady() {
		rd := s.raft.Ready()
		if err := s.store.Save(rd.HardState, rd.HardStateChanged, rd.Snapshot, rd.Entries); err != nil {
			return err
		}
		for _, m := range rd.Messages {
			if ch := s.senders[m.To]; ch != nil {
				select {
				case ch <- m:
				default: // peer is slow or down; raft will retransmit
				}
			}
		}
		if rd.Snapshot != nil {
			if err := s.kv.Restore(rd.Snapshot.Data); err != nil {
				return err
			}
			for idx, p := range s.waiters {
				if idx <= rd.Snapshot.Index {
					p.res <- result{err: ErrUnknownOutcome}
					delete(s.waiters, idx)
				}
			}
		}
		for _, e := range rd.CommittedEntries {
			_, res, err := s.kv.ApplyEntry(e.Data)
			if p, ok := s.waiters[e.Index]; ok {
				delete(s.waiters, e.Index)
				if p.term != e.Term {
					p.res <- result{err: ErrUnknownOutcome}
				} else {
					p.res <- result{val: res, err: err}
				}
			}
		}
		s.raft.Advance(rd)
		if st := s.raft.Status(); st.Applied-st.SnapIndex >= s.cfg.SnapshotEvery {
			snap, err := s.raft.Compact(st.Applied, s.kv.Snapshot())
			if err != nil {
				return err
			}
			if err := s.store.SaveSnapshot(snap); err != nil {
				return err
			}
		}
	}
	// A proposal whose index was overwritten will never be applied at its
	// term; fail waiters that can no longer succeed once we're not leader.
	if st := s.raft.Status(); st.Role != raft.Leader {
		for idx, p := range s.waiters {
			if idx <= st.Commit {
				continue
			}
			p.res <- result{err: ErrUnknownOutcome}
			delete(s.waiters, idx)
		}
	}
	return nil
}

func (s *Server) sendLoop(base string, ch chan raft.Message) {
	defer s.wg.Done()
	for m := range ch {
		batch := []raft.Message{m}
	drain:
		for len(batch) < 256 {
			select {
			case more, ok := <-ch:
				if !ok {
					break drain
				}
				batch = append(batch, more)
			default:
				break drain
			}
		}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(batch); err != nil {
			continue
		}
		req, err := http.NewRequest(http.MethodPost, base+"/raft", &buf)
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		if s.cfg.PeerToken != "" {
			req.Header.Set("X-Raft-Token", s.cfg.PeerToken)
		}
		resp, err := s.client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}

// Status returns the node's raft status (from the loop goroutine).
func (s *Server) Status() (raft.Status, error) {
	c := make(chan raft.Status, 1)
	select {
	case s.statusC <- c:
		return <-c, nil
	case <-s.done:
		return raft.Status{}, ErrStopped
	}
}

// Do proposes a command and waits until it is applied (or fails).
func (s *Server) Do(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	p := &proposal{cmd: cmd, res: make(chan result, 1)}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	select {
	case s.props <- p:
	case <-s.stop:
		return kv.Result{}, ErrStopped
	case <-s.done:
		return kv.Result{}, ErrStopped
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	}
	select {
	case r := <-p.res:
		return r.val, r.err
	case <-s.done: // loop exited; a queued proposal will never be answered
		select {
		case r := <-p.res:
			return r.val, r.err
		default:
			return kv.Result{}, ErrStopped
		}
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	}
}

// Handler serves the peer transport (/raft), the client API (/kv/{key}) and
// /status.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft", func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.PeerToken != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Raft-Token")), []byte(s.cfg.PeerToken)) != 1 {
			http.Error(w, "bad peer token", http.StatusUnauthorized)
			return
		}
		var batch []raft.Message
		if err := gob.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<20)).Decode(&batch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, m := range batch {
			if m.To != s.cfg.ID {
				continue
			}
			select {
			case s.inbox <- m:
			default: // overloaded: drop, raft retransmits
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		st, err := s.Status()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"id": st.ID, "role": st.Role.String(), "term": st.Term,
			"leader": st.Leader, "commit": st.Commit, "applied": st.Applied, "last_index": st.LastIndex,
			"snapshot_index": st.SnapIndex})
	})
	kvHandler := func(op kv.Op) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			cmd := kv.Command{Op: op, Key: r.PathValue("key")}
			if op != kv.Get {
				b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				cmd.Value = string(b)
			}
			// Exactly-once retries need a stable (client id, seq) from the
			// client. Without them a request is at-most-once per attempt and
			// is not recorded in the session table.
			if h := r.Header.Get("X-Client-ID"); h != "" {
				cid, err1 := strconv.ParseUint(h, 10, 64)
				seq, err2 := strconv.ParseUint(r.Header.Get("X-Seq"), 10, 64)
				if err1 != nil || err2 != nil || cid == 0 || seq == 0 {
					respondJSON(w, http.StatusBadRequest, map[string]string{"error": "X-Client-ID and X-Seq must be positive integers"})
					return
				}
				cmd.ClientID, cmd.Seq = cid, seq
			}
			res, err := s.Do(r.Context(), cmd)
			switch {
			case errors.Is(err, ErrNotLeader):
				st, _ := s.Status()
				hint := ""
				if st.Leader != raft.None {
					hint = s.cfg.Peers[st.Leader]
				}
				w.Header().Set("X-Raft-Leader", hint)
				respondJSON(w, http.StatusMisdirectedRequest, map[string]string{"error": "not the leader", "leader": hint})
			case errors.Is(err, ErrUnknownOutcome), errors.Is(err, context.DeadlineExceeded):
				respondJSON(w, http.StatusServiceUnavailable, map[string]string{"error": ErrUnknownOutcome.Error()})
			case err != nil:
				respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			default:
				respondJSON(w, http.StatusOK, map[string]any{"value": res.Value, "found": res.Found})
			}
		}
	}
	mux.HandleFunc("GET /kv/{key}", kvHandler(kv.Get))
	mux.HandleFunc("PUT /kv/{key}", kvHandler(kv.Put))
	mux.HandleFunc("POST /kv/{key}/append", kvHandler(kv.Append))
	return mux
}

func respondJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
