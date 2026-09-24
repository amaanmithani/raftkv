package sim

import (
	"fmt"
	"math"

	"github.com/anishathalye/porcupine"
	"github.com/amaanmithani/raftkv/kv"
	"github.com/amaanmithani/raftkv/raft"
)

// KVInput and KVOutput are the Porcupine operation payloads.
type KVInput struct {
	Op    kv.Op
	Key   string
	Value string
}

// KVOutput is what a completed operation returned.
type KVOutput struct {
	Value   string
	Unknown bool // never completed: may or may not have taken effect
}

// KVModel is the sequential specification of the store, partitioned by key.
var KVModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(KVInput).Key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		s, in, out := state.(string), input.(KVInput), output.(KVOutput)
		switch in.Op {
		case kv.Get:
			return out.Unknown || out.Value == s, s
		case kv.Put:
			return true, in.Value
		default: // append
			return true, s + in.Value
		}
	},
	Equal: func(a, b any) bool { return a == b },
	DescribeOperation: func(input, output any) string {
		in, out := input.(KVInput), output.(KVOutput)
		if in.Op == kv.Get {
			return fmt.Sprintf("get(%s) -> %q", in.Key, out.Value)
		}
		return fmt.Sprintf("%s(%s, %q)", in.Op, in.Key, in.Value)
	},
}

type pending struct {
	cmd      kv.Command
	start    int
	node     raft.ID
	index    uint64
	term     uint64
	deadline int
}

type client struct {
	id      uint64
	seq     uint64
	target  raft.ID
	pending *pending
	left    int
}

// Workload drives clients against a cluster and records a Porcupine history.
// Clients retry a request with the same (client, seq) on another node after a
// timeout or a lost proposal; the store's sessions make retries exactly-once.
type Workload struct {
	c       *Cluster
	clients []*client
	keys    []string
	timeout int
	history []porcupine.Operation
	// Completed counts operations that returned.
	Completed int
}

// NewWorkload attaches nClients clients, each issuing opsPerClient operations
// over a small key space (contention is the point).
func NewWorkload(c *Cluster, nClients, opsPerClient int, keys []string) *Workload {
	w := &Workload{c: c, keys: keys, timeout: 4 * c.cfg.ElectionTick}
	for i := 0; i < nClients; i++ {
		w.clients = append(w.clients, &client{id: uint64(i + 1), left: opsPerClient})
	}
	c.OnApply = w.onApply
	return w
}

func (w *Workload) randomNode() raft.ID { return w.c.IDs[w.c.rng.Intn(len(w.c.IDs))] }

// tryPropose submits the client's pending command to its target node.
func (w *Workload) tryPropose(cl *client) {
	p := cl.pending
	n := w.c.Nodes[cl.target]
	if !n.Alive {
		cl.target = w.randomNode()
		return
	}
	idx, term, err := n.Raft.Propose(p.cmd.Encode())
	if err != nil {
		if l := n.Raft.Status().Leader; l != raft.None {
			cl.target = l
		} else {
			cl.target = w.randomNode()
		}
		return
	}
	p.node, p.index, p.term, p.deadline = cl.target, idx, term, w.c.Now+w.timeout
}

// Tick issues new operations and retries stalled ones. Call once per step.
func (w *Workload) Tick() {
	for _, cl := range w.clients {
		if cl.pending == nil {
			if cl.left == 0 || w.c.rng.Intn(3) != 0 {
				continue
			}
			cl.left--
			cl.seq++
			op := []kv.Op{kv.Get, kv.Put, kv.Append}[w.c.rng.Intn(3)]
			cmd := kv.Command{Op: op, Key: w.keys[w.c.rng.Intn(len(w.keys))], ClientID: cl.id, Seq: cl.seq}
			if op != kv.Get {
				cmd.Value = fmt.Sprintf("%d.%d ", cl.id, cl.seq)
			}
			cl.pending = &pending{cmd: cmd, start: w.c.Now}
			if cl.target == raft.None {
				cl.target = w.randomNode()
			}
		}
		p := cl.pending
		if p.index == 0 || w.c.Now >= p.deadline || !w.c.Nodes[p.node].Alive {
			if p.index != 0 { // timed out or node died: retry elsewhere, same seq
				p.index = 0
				cl.target = w.randomNode()
			}
			w.tryPropose(cl)
		}
	}
}

func (w *Workload) onApply(node raft.ID, e raft.Entry, cmd kv.Command, res kv.Result, err error) {
	for _, cl := range w.clients {
		p := cl.pending
		if p == nil || p.index == 0 || p.node != node || p.index != e.Index {
			continue
		}
		if e.Term != p.term {
			p.index = 0 // our proposal was overwritten: retry
			continue
		}
		if err != nil {
			panic(fmt.Sprintf("client %d seq %d: %v", cl.id, p.cmd.Seq, err))
		}
		w.history = append(w.history, porcupine.Operation{ClientId: int(cl.id),
			Input: KVInput{Op: p.cmd.Op, Key: p.cmd.Key, Value: p.cmd.Value}, Call: int64(p.start),
			Output: KVOutput{Value: res.Value}, Return: int64(w.c.Now)})
		w.Completed++
		cl.pending = nil
	}
}

// Idle reports whether every client has finished.
func (w *Workload) Idle() bool {
	for _, cl := range w.clients {
		if cl.pending != nil || cl.left > 0 {
			return false
		}
	}
	return true
}

// History returns completed operations plus writes still pending, which are
// recorded as possibly-applied (they may have committed without the client
// hearing back). Pending reads impose no constraint and are dropped.
func (w *Workload) History() []porcupine.Operation {
	h := append([]porcupine.Operation(nil), w.history...)
	for _, cl := range w.clients {
		if p := cl.pending; p != nil && p.cmd.Op != kv.Get {
			h = append(h, porcupine.Operation{ClientId: int(cl.id),
				Input: KVInput{Op: p.cmd.Op, Key: p.cmd.Key, Value: p.cmd.Value}, Call: int64(p.start),
				Output: KVOutput{Unknown: true}, Return: math.MaxInt64})
		}
	}
	return h
}
