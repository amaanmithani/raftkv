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
	checkTimeout := flag.Duration("check-timeout", 30*time.Second, "Porcupine search limit per schedule")
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
			sc := sim.DefaultSchedule
			sc.CheckTimeout = *checkTimeout
			results[i] = sim.RunSchedule(*first+int64(i), sc)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var bad []sim.ScheduleResult
	inconclusive := 0
	var ops, completed, crashes, partitions, faults, terms, sent, dropped, under, afterHeal int
	for _, r := range results {
		switch {
		case r.Invariant != "" || (!r.Linearizable && !r.Inconclusive):
			bad = append(bad, r) // proven: an invariant broke or the history is not linearizable
		case r.Inconclusive:
			inconclusive++
		}
		ops += r.Operations
		completed += r.Completed
		crashes += r.Crashes
		partitions += r.Partitions
		faults += r.Faults
		terms += r.Elections
		sent += r.Sent
		dropped += r.Dropped
		under += r.OpsUnderFault
		afterHeal += r.OpsAfterHeal
	}
	sort.Slice(bad, func(a, b int) bool { return bad[a].Seed < bad[b].Seed })
	for _, r := range bad {
		fmt.Println("FAIL", r)
	}
	summary := map[string]any{
		"what": "linearizability of the client-observed history under randomized faults",
		"method": fmt.Sprintf("%d seeded schedules (seeds %d..%d), each: %d nodes, %d clients x %d ops on 3 keys, %d steps of nemesis "+
			"(random splits, leader isolation, crashes with and without immediate restart, leader crashes, 0-40%% message loss, "+
			"1-4 tick delays with reordering, message duplication), then healing. PreVote, CheckQuorum, append batch size, "+
			"snapshot interval (down to every entry), duplication rate and fault frequency are randomized per seed. Raft safety invariants checked during the run; history checked with Porcupine",
			*n, *first, *first+int64(*n)-1, sim.DefaultSchedule.Nodes, sim.DefaultSchedule.Clients, sim.DefaultSchedule.OpsPerClient,
			sim.DefaultSchedule.FaultSteps),
		"schedules": *n, "violations": len(bad), "inconclusive_checks": inconclusive, "operations_checked": ops, "operations_completed": completed,
		"fault_events": faults, "crashes": crashes, "partitions": partitions, "leader_terms": terms,
		"messages_sent": sent, "messages_dropped": dropped,
		"ops_overlapping_a_fault": under, "ops_completed_after_healing": afterHeal,
		"note": "reads and writes all go through the Raft log; client replies are delivered in-process (never lost), so " +
			"uncertainty comes only from client timeouts and retries",
		"wall_seconds": elapsed.Round(time.Second).Seconds(), "cpus": runtime.NumCPU(),
		"date": time.Now().Format("2006-01-02"),
	}
	if len(bad) > 0 {
		summary["failing_seeds"] = bad
	}
	fmt.Printf("%d schedules, %d violations, %d inconclusive, %d ops checked, %s\n", *n, len(bad), inconclusive, ops, elapsed.Round(time.Second))
	if *out != "" {
		b, _ := json.MarshalIndent(summary, "", "  ")
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	switch {
	case len(bad) > 0:
		os.Exit(1)
	case inconclusive > 0:
		fmt.Println("INCONCLUSIVE: some histories could not be checked in time; raise -check-timeout")
		os.Exit(2)
	}
}
