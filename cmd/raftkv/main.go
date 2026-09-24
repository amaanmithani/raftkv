// Command raftkv runs one node of a raftkv cluster.
//
//	raftkv -id 1 -peers 1=http://127.0.0.1:7001,2=http://127.0.0.1:7002,3=http://127.0.0.1:7003 -data ./data/1
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/amaanmithani/raftkv/node"
	"github.com/amaanmithani/raftkv/raft"
)

func main() {
	id := flag.Uint64("id", 0, "this node's id (must appear in -peers)")
	peers := flag.String("peers", "", "comma-separated id=url list, including this node")
	data := flag.String("data", "", "data directory (default ./data/<id>)")
	tick := flag.Duration("tick", 50*time.Millisecond, "raft tick interval")
	nosync := flag.Bool("nosync", false, "skip fsync (benchmarks only: loses durability)")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(raft.ID(*id), *peers, *data, *tick, !*nosync, logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func parsePeers(s string) (map[raft.ID]string, error) {
	out := map[raft.ID]string{}
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		n, err := strconv.ParseUint(k, 10, 64)
		if !ok || err != nil || n == 0 {
			return nil, fmt.Errorf("bad peer %q: want id=url", part)
		}
		if _, err := url.Parse(v); err != nil {
			return nil, fmt.Errorf("bad peer url %q: %w", v, err)
		}
		out[raft.ID(n)] = strings.TrimRight(v, "/")
	}
	return out, nil
}

func run(id raft.ID, peerList, data string, tick time.Duration, sync bool, logger *slog.Logger) error {
	peers, err := parsePeers(peerList)
	if err != nil {
		return err
	}
	self, ok := peers[id]
	if !ok {
		return fmt.Errorf("-id %d is not in -peers", id)
	}
	if data == "" {
		data = fmt.Sprintf("data/%d", id)
	}
	u, err := url.Parse(self)
	if err != nil {
		return err
	}
	s, err := node.New(node.Config{ID: id, Peers: peers, Dir: data, Tick: tick, Sync: sync, Logger: logger})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", u.Host)
	if err != nil {
		return err
	}
	s.Start()
	srv := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	logger.Info("raftkv node up", "id", id, "addr", u.Host, "data", data, "fsync", sync)
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	s.Stop()
	return nil
}
