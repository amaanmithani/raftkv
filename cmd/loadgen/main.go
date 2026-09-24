// Command loadgen drives a raftkv cluster with concurrent clients and reports
// throughput and latency as JSON. Each client issues sequential requests with
// its own client id and increasing seq, following leader redirects.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	peers := flag.String("peers", "", "comma-separated base URLs")
	clients := flag.Int("clients", 32, "concurrent clients")
	dur := flag.Duration("duration", 20*time.Second, "measurement duration")
	warm := flag.Duration("warmup", 3*time.Second, "warm-up before measuring")
	keys := flag.Int("keys", 1000, "key space")
	valSize := flag.Int("value-bytes", 64, "value size")
	readPct := flag.Int("read-pct", 0, "percentage of GETs (reads go through the log)")
	flag.Parse()
	urls := strings.Split(*peers, ",")
	tr := &http.Transport{MaxIdleConnsPerHost: 2 * *clients, MaxIdleConns: 4 * *clients}
	hc := &http.Client{Timeout: 10 * time.Second, Transport: tr}

	var leader atomic.Value
	leader.Store(urls[0])
	var measuring atomic.Bool
	var mu sync.Mutex
	var lats []float64
	var errors, redirects atomic.Int64
	var ekMu sync.Mutex
	errKinds := map[string]int{}
	kind := func(k string) {
		ekMu.Lock()
		errKinds[k]++
		ekMu.Unlock()
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	val := strings.Repeat("x", *valSize)
	// Client ids must be unique per run: a later run reusing ids with seq
	// restarting at 1 would be (correctly) rejected by the store's sessions
	// as stale duplicates.
	idBase := uint64(time.Now().UnixNano()) &^ 0xFFFFF
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(cid)))
			var local []float64
			seq := uint64(0)
			for {
				select {
				case <-stop:
					mu.Lock()
					lats = append(lats, local...)
					mu.Unlock()
					return
				default:
				}
				seq++
				key := fmt.Sprint("k", rng.Intn(*keys))
				method, body := http.MethodPut, val
				if rng.Intn(100) < *readPct {
					method, body = http.MethodGet, ""
				}
				start := time.Now()
				for attempt := 0; attempt < 20; attempt++ {
					base := leader.Load().(string)
					req, _ := http.NewRequest(method, base+"/kv/"+key, strings.NewReader(body))
					req.Header.Set("X-Client-ID", fmt.Sprint(idBase+uint64(cid)+1))
					req.Header.Set("X-Seq", fmt.Sprint(seq))
					resp, err := hc.Do(req)
					if err != nil {
						errors.Add(1)
						msg := err.Error()
						if i := strings.LastIndex(msg, ": "); i >= 0 {
							msg = msg[i+2:]
						}
						kind(msg)
						leader.Store(urls[rng.Intn(len(urls))])
						continue
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						if measuring.Load() {
							local = append(local, float64(time.Since(start).Microseconds())/1000)
						}
						break
					}
					if resp.StatusCode == http.StatusMisdirectedRequest {
						redirects.Add(1)
						if h := resp.Header.Get("X-Raft-Leader"); h != "" {
							leader.Store(h)
						} else {
							leader.Store(urls[rng.Intn(len(urls))])
						}
						continue
					}
					errors.Add(1)
					kind(fmt.Sprint("http ", resp.StatusCode))
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(c)
	}
	time.Sleep(*warm)
	measuring.Store(true)
	t0 := time.Now()
	time.Sleep(*dur)
	measuring.Store(false)
	elapsed := time.Since(t0)
	close(stop)
	wg.Wait()
	sort.Float64s(lats)
	pct := func(p float64) float64 {
		if len(lats) == 0 {
			return 0
		}
		return lats[min(len(lats)-1, int(p*float64(len(lats))))]
	}
	out := map[string]any{"clients": *clients, "ops": len(lats), "seconds": elapsed.Seconds(),
		"ops_per_sec": float64(len(lats)) / elapsed.Seconds(), "p50_ms": pct(0.5), "p99_ms": pct(0.99),
		"max_ms": pct(1), "errors": errors.Load(), "error_kinds": errKinds, "redirects": redirects.Load(), "read_pct": *readPct, "value_bytes": *valSize}
	_ = json.NewEncoder(os.Stdout).Encode(out)
}
