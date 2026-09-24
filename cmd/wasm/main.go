//go:build js && wasm

// Command wasm exposes the deterministic simulator to the browser. The page
// calls raftkv.* functions and renders the JSON state they return.
package main

import (
	"encoding/json"
	"fmt"
	"syscall/js"

	"github.com/amaanmithani/raftkv/kv"
	"github.com/amaanmithani/raftkv/raft"
	"github.com/amaanmithani/raftkv/sim"
)

var (
	c      *sim.Cluster
	cutOff = map[raft.ID]bool{} // nodes isolated by the user
	seq    uint64
)

type nodeView struct {
	ID        uint64            `json:"id"`
	Alive     bool              `json:"alive"`
	Isolated  bool              `json:"isolated"`
	Role      string            `json:"role"`
	Term      uint64            `json:"term"`
	Vote      uint64            `json:"vote"`
	Leader    uint64            `json:"leader"`
	Commit    uint64            `json:"commit"`
	Applied   uint64            `json:"applied"`
	LastIndex uint64            `json:"lastIndex"`
	SnapIndex uint64            `json:"snapIndex"`
	Log       []logCell         `json:"log"`
	KV        map[string]string `json:"kv"`
}

type logCell struct {
	Index uint64 `json:"i"`
	Term  uint64 `json:"t"`
	Text  string `json:"x"`
}

type msgView struct {
	From uint64  `json:"from"`
	To   uint64  `json:"to"`
	Type string  `json:"type"`
	Prog float64 `json:"p"` // 0 at send, 1 on arrival
	Ok   bool    `json:"ok"`
	N    int     `json:"n"` // entries carried
}

func state() string {
	nodes := []nodeView{}
	for _, id := range c.IDs {
		n := c.Nodes[id]
		v := nodeView{ID: uint64(id), Alive: n.Alive, Isolated: cutOff[id]}
		if n.Alive {
			st := n.Raft.Status()
			v.Role, v.Term, v.Vote, v.Leader = st.Role.String(), st.Term, uint64(st.Vote), uint64(st.Leader)
			v.Commit, v.Applied, v.LastIndex, v.SnapIndex = st.Commit, st.Applied, st.LastIndex, st.SnapIndex
			es := n.Raft.Entries()
			if len(es) > 24 {
				es = es[len(es)-24:]
			}
			for _, e := range es {
				txt := "no-op"
				if cmd, err := kv.Decode(e.Data); err == nil && len(e.Data) > 0 {
					txt = fmt.Sprintf("%s %s=%s", cmd.Op, cmd.Key, cmd.Value)
				}
				v.Log = append(v.Log, logCell{Index: e.Index, Term: e.Term, Text: txt})
			}
			v.KV = map[string]string{}
			for _, k := range n.KV.Keys() {
				v.KV[k], _ = n.KV.Get(k)
			}
		}
		nodes = append(nodes, v)
	}
	msgs := []msgView{}
	for _, f := range c.InFlight() {
		span := float64(f.DueAt - f.SentAt)
		p := 0.0
		if span > 0 {
			p = float64(c.Now-f.SentAt) / span
		}
		ok := f.Msg.Success || f.Msg.Granted
		msgs = append(msgs, msgView{From: uint64(f.Msg.From), To: uint64(f.Msg.To), Type: f.Msg.Type.String(), Prog: p,
			Ok: ok, N: len(f.Msg.Entries)})
	}
	b, _ := json.Marshal(map[string]any{"now": c.Now, "nodes": nodes, "msgs": msgs, "committed": c.Committed()})
	return string(b)
}

func applyPartition() {
	var rest []raft.ID
	var alone [][]raft.ID
	for _, id := range c.IDs {
		if cutOff[id] {
			alone = append(alone, []raft.ID{id})
		} else {
			rest = append(rest, id)
		}
	}
	if len(alone) == 0 {
		c.Heal()
		return
	}
	c.Partition(append([][]raft.ID{rest}, alone...)...)
}

func newCluster(n, seed int) {
	c = sim.New(sim.Config{N: n, Seed: int64(seed), PreVote: true, CheckQuorum: true, ElectionTick: 10,
		HeartbeatTick: 2, MaxDelay: 3, SnapshotEvery: 40})
	cutOff = map[raft.ID]bool{}
}

func main() {
	api := js.Global().Get("Object").New()
	api.Set("reset", js.FuncOf(func(_ js.Value, a []js.Value) any {
		newCluster(a[0].Int(), a[1].Int())
		return state()
	}))
	api.Set("step", js.FuncOf(func(_ js.Value, _ []js.Value) any {
		if err := c.Step(); err != nil {
			return js.ValueOf(map[string]any{"error": err.Error()})
		}
		return state()
	}))
	api.Set("toggleCrash", js.FuncOf(func(_ js.Value, a []js.Value) any {
		id := raft.ID(a[0].Int())
		if c.Nodes[id].Alive {
			c.Crash(id)
		} else {
			c.Restart(id)
		}
		return state()
	}))
	api.Set("toggleIsolate", js.FuncOf(func(_ js.Value, a []js.Value) any {
		id := raft.ID(a[0].Int())
		cutOff[id] = !cutOff[id]
		applyPartition()
		return state()
	}))
	api.Set("put", js.FuncOf(func(_ js.Value, a []js.Value) any {
		l := c.Leader()
		if l == raft.None {
			return "no leader right now"
		}
		seq++
		cmd := kv.Command{Op: kv.Put, Key: a[0].String(), Value: a[1].String(), ClientID: 1, Seq: seq}
		if _, _, err := c.Nodes[l].Raft.Propose(cmd.Encode()); err != nil {
			return err.Error()
		}
		return ""
	}))
	js.Global().Set("raftkv", api)
	newCluster(5, 1)
	select {}
}
