// Command linearize runs many seeded fault schedules in parallel and writes a
// summary to results/linearizability.json. Any failing seed is printed so it
// can be replayed exactly with -seed.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/amaanmithani/raftkv/sim"
)

func main() {
	n := flag.Int("schedules", 1000, "number of seeded schedules")
	first := flag.Int64("seed", 1, "first seed")
	out := flag.String("out", "results/linearizability.json", "output file ('' to skip)")
	flag.Parse()

	start := time.Now()
	results := make([]sim.ScheduleResult, *n)
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.NumCPU())
	for i := 0; i < *n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = sim.RunSchedule(*first+int64(i), sim.DefaultSchedule)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var bad []sim.ScheduleResult
	var ops, completed, crashes, partitions, faults, terms, sent, dropped int
	for _, r := range results {
		if !r.Linearizable || r.Invariant != "" {
			bad = append(bad, r)
		}
		ops += r.Operations
		completed += r.Completed
		crashes += r.Crashes
		partitions += r.Partitions
		faults += r.Faults
		terms += r.Elections
		sent += r.Sent
		dropped += r.Dropped
	}
	sort.Slice(bad, func(a, b int) bool { return bad[a].Seed < bad[b].Seed })
	for _, r := range bad {
		fmt.Println("FAIL", r)
	}
	summary := map[string]any{
		"what": "linearizability of the client-observed history under randomized faults",
		"method": fmt.Sprintf("%d seeded schedules (seeds %d..%d), each: %d nodes, %d clients x %d ops on 3 keys, %d steps of nemesis "+
			"(random splits, leader isolation, crashes with and without immediate restart, leader crashes, 0-40%% message loss, "+
			"1-5 tick delays with reordering), then healing. PreVote, CheckQuorum, append batch size, snapshot interval and fault "+
			"frequency are randomized per seed. Raft safety invariants checked during the run; history checked with Porcupine",
			*n, *first, *first+int64(*n)-1, sim.DefaultSchedule.Nodes, sim.DefaultSchedule.Clients, sim.DefaultSchedule.OpsPerClient,
			sim.DefaultSchedule.FaultSteps),
		"schedules": *n, "violations": len(bad), "operations_checked": ops, "operations_completed": completed,
		"fault_events": faults, "crashes": crashes, "partitions": partitions, "leader_terms": terms,
		"messages_sent": sent, "messages_dropped": dropped,
		"wall_seconds": elapsed.Round(time.Second).Seconds(), "cpus": runtime.NumCPU(),
		"date": time.Now().Format("2006-01-02"),
	}
	if len(bad) > 0 {
		summary["failing_seeds"] = bad
	}
	fmt.Printf("%d schedules, %d violations, %d ops checked, %s\n", *n, len(bad), ops, elapsed.Round(time.Second))
	if *out != "" {
		b, _ := json.MarshalIndent(summary, "", "  ")
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if len(bad) > 0 {
		os.Exit(1)
	}
}
