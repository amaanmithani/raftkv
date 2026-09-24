package sim

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/amaanmithani/raftkv/raft"
)

// ScheduleResult summarises one randomized fault schedule.
type ScheduleResult struct {
	Seed         int64   `json:"seed"`
	Variant      Variant `json:"variant"`
	Linearizable bool    `json:"linearizable"`
	Inconclusive bool    `json:"inconclusive,omitempty"` // checker timed out: neither proven nor refuted
	Invariant    string  `json:"invariant_violation,omitempty"`
	Operations   int     `json:"operations"`
	Completed    int     `json:"completed"`
	Faults       int     `json:"faults"`
	Crashes      int     `json:"crashes"`
	Partitions   int     `json:"partitions"`
	Elections    int     `json:"leader_terms"`
	Steps        int     `json:"steps"`
	Sent         int     `json:"messages_sent"`
	Dropped      int     `json:"messages_dropped"`
	// OpsUnderFault counts completed operations whose call-to-return span
	// overlapped a tick with an active fault (a partition, a crashed node, or
	// message loss/duplication). OpsAfterHeal counts those that returned only
	// after the healing phase began.
	OpsUnderFault int `json:"ops_under_fault"`
	OpsAfterHeal  int `json:"ops_after_heal"`
}

// ScheduleConfig shapes a randomized run.
type ScheduleConfig struct {
	Nodes        int
	Clients      int
	OpsPerClient int
	FaultSteps   int // steps with faults injected
	SettleSteps  int // max steps after healing for clients to finish
	CheckEvery   int
	// CheckTimeout bounds the Porcupine search per schedule (default 30s).
	CheckTimeout time.Duration
}

// DefaultSchedule is the configuration used for the committed results.
var DefaultSchedule = ScheduleConfig{Nodes: 5, Clients: 6, OpsPerClient: 30, FaultSteps: 800, SettleSteps: 1500, CheckEvery: 5}

// Variant records the per-seed randomized cluster settings.
type Variant struct {
	PreVote          bool    `json:"prevote"`
	CheckQuorum      bool    `json:"check_quorum"`
	MaxDelay         int     `json:"max_delay"`
	MaxEntriesPerMsg int     `json:"max_entries_per_msg"`
	SnapshotEvery    int     `json:"snapshot_every"`
	FaultGap         int     `json:"max_steps_between_faults"`
	DupRate          float64 `json:"duplicate_rate"`
}

// RunSchedule runs one seeded schedule: a workload under an adversarial
// nemesis (partitions, leader isolation, crashes with and without immediate
// restart, message loss and reordering), then a healing phase, then a
// Porcupine linearizability check of the client-observed history. Cluster
// settings are randomized per seed, so the optional defences (PreVote,
// CheckQuorum) don't mask bugs in the core algorithm.
func RunSchedule(seed int64, sc ScheduleConfig) ScheduleResult {
	vr := rand.New(rand.NewSource(seed * 7919))
	v := Variant{PreVote: vr.Intn(2) == 0, CheckQuorum: vr.Intn(2) == 0, MaxDelay: 1 + vr.Intn(4),
		MaxEntriesPerMsg: 1 + vr.Intn(8), SnapshotEvery: 10 + vr.Intn(50), FaultGap: 5 + vr.Intn(30),
		DupRate: []float64{0, 0, 0.1, 0.3}[vr.Intn(4)]}
	if vr.Intn(3) == 0 {
		v.SnapshotEvery = 1 + vr.Intn(6) // aggressive compaction: snapshots installed constantly
	}
	c := New(Config{N: sc.Nodes, Seed: seed, PreVote: v.PreVote, CheckQuorum: v.CheckQuorum,
		SnapshotEvery: uint64(v.SnapshotEvery), MaxDelay: v.MaxDelay, MaxEntriesPerMsg: v.MaxEntriesPerMsg, DupRate: v.DupRate,
		CheckEvery: sc.CheckEvery})
	w := NewWorkload(c, sc.Clients, sc.OpsPerClient, []string{"a", "b", "c"})
	res := ScheduleResult{Seed: seed, Variant: v}
	rng := c.rng
	restartAt := map[raft.ID]int{}
	faultTick := map[int]bool{}
	dropping := false
	step := func() bool {
		active := dropping || len(restartAt) > 0 || c.Partitioned()
		for _, id := range c.IDs {
			if !c.Nodes[id].Alive {
				active = true
			}
		}
		if active {
			faultTick[c.Now+1] = true
		}
		// Iterate in ID order: map order is random and restarts consume the
		// seeded RNG, so any other order would break exact replay.
		for _, id := range c.IDs {
			if at, ok := restartAt[id]; ok && c.Now >= at {
				c.Restart(id)
				delete(restartAt, id)
			}
		}
		w.Tick()
		if err := c.Step(); err != nil {
			res.Invariant = err.Error()
			return false
		}
		return true
	}
	alive := func() int {
		k := 0
		for _, id := range c.IDs {
			if c.Nodes[id].Alive {
				k++
			}
		}
		return k
	}
	next := 0
	for i := 0; i < sc.FaultSteps; i++ {
		if i >= next {
			next = i + 1 + rng.Intn(v.FaultGap)
			res.Faults++
			switch rng.Intn(7) {
			case 0: // random two-way split
				var a, b []raft.ID
				for _, id := range c.IDs {
					if rng.Intn(2) == 0 {
						a = append(a, id)
					} else {
						b = append(b, id)
					}
				}
				c.Partition(a, b)
				res.Partitions++
			case 1: // isolate the leader (alone or with one follower): its log diverges
				if l := c.Leader(); l != raft.None {
					minority := []raft.ID{l}
					var rest []raft.ID
					for _, id := range c.IDs {
						if id == l {
							continue
						}
						if len(minority) < 2 && rng.Intn(2) == 0 {
							minority = append(minority, id)
						} else {
							rest = append(rest, id)
						}
					}
					c.Partition(minority, rest)
					res.Partitions++
				}
			case 2:
				c.Heal()
			case 3: // crash, and maybe come straight back (a crash mid-election)
				if id := c.IDs[rng.Intn(len(c.IDs))]; alive() > 1 && c.Nodes[id].Alive {
					c.Crash(id)
					res.Crashes++
					if rng.Intn(2) == 0 {
						restartAt[id] = c.Now + 1 + rng.Intn(10)
					}
				}
			case 4: // crash the leader right now
				if l := c.Leader(); l != raft.None && alive() > 1 {
					c.Crash(l)
					res.Crashes++
					restartAt[l] = c.Now + 1 + rng.Intn(20)
				}
			case 5:
				for _, id := range c.IDs {
					if !c.Nodes[id].Alive && rng.Intn(2) == 0 {
						c.Restart(id)
						delete(restartAt, id)
					}
				}
			case 6:
				rate := []float64{0, 0.05, 0.2, 0.4}[rng.Intn(4)]
				c.SetDropRate(rate)
				dropping = rate > 0
			}
		}
		if !step() {
			return finish(res, c, w, sc.CheckTimeout)
		}
	}
	// Heal everything and let clients finish.
	healAt := c.Now
	stats := func(r ScheduleResult) ScheduleResult {
		r.OpsUnderFault, r.OpsAfterHeal = w.faultStats(faultTick, healAt)
		return r
	}
	dropping = v.DupRate > 0
	c.Heal()
	c.SetDropRate(0)
	for _, id := range c.IDs {
		c.Restart(id)
	}
	restartAt = map[raft.ID]int{}
	for i := 0; i < sc.SettleSteps && !w.Idle(); i++ {
		if !step() {
			return stats(finish(res, c, w, sc.CheckTimeout))
		}
	}
	return stats(finish(res, c, w, sc.CheckTimeout))
}

func finish(res ScheduleResult, c *Cluster, w *Workload, timeout time.Duration) ScheduleResult {
	res.Steps, res.Sent, res.Dropped, res.Elections = c.Now, c.Sent, c.Dropped, len(c.leaders)
	h := w.History()
	res.Operations, res.Completed = len(h), w.Completed
	if res.Invariant != "" {
		return res
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	out := porcupine.CheckOperationsTimeout(KVModel, h, timeout)
	res.Linearizable = out == porcupine.Ok
	res.Inconclusive = out == porcupine.Unknown
	return res
}

// String renders a one-line summary.
func (r ScheduleResult) String() string {
	return fmt.Sprintf("seed %d: linearizable=%v ops=%d completed=%d faults=%d crashes=%d partitions=%d terms=%d %s",
		r.Seed, r.Linearizable, r.Operations, r.Completed, r.Faults, r.Crashes, r.Partitions, r.Elections, r.Invariant)
}
