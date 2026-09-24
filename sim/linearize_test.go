package sim

import (
	"testing"

	"github.com/anishathalye/porcupine"
	"github.com/amaanmithani/raftkv/kv"
)

func TestSchedulesAreLinearizable(t *testing.T) {
	n := int64(25)
	if testing.Short() {
		n = 5
	}
	for seed := int64(1); seed <= n; seed++ {
		r := RunSchedule(seed, DefaultSchedule)
		if !r.Linearizable || r.Invariant != "" || r.Inconclusive {
			t.Fatalf("%s", r)
		}
		if r.Completed == 0 {
			t.Fatalf("seed %d: no operation completed", seed)
		}
	}
}

// The checker must actually catch violations, or a green run means nothing.
func TestCheckerCatchesStaleRead(t *testing.T) {
	h := []porcupine.Operation{
		{ClientId: 1, Input: KVInput{Op: kv.Put, Key: "a", Value: "1"}, Call: 0, Output: KVOutput{}, Return: 10},
		{ClientId: 2, Input: KVInput{Op: kv.Get, Key: "a"}, Call: 20, Output: KVOutput{Value: ""}, Return: 30},
	}
	if porcupine.CheckOperations(KVModel, h) {
		t.Fatal("a read after a completed write returned the old value, and the checker accepted it")
	}
	h[1].Output = KVOutput{Value: "1"}
	if !porcupine.CheckOperations(KVModel, h) {
		t.Fatal("valid history rejected")
	}
	if KVModel.DescribeOperation(KVInput{Op: kv.Get, Key: "a"}, KVOutput{Value: "x"}) == "" {
		t.Fatal("describe")
	}
}

func TestScheduleReplaysExactly(t *testing.T) {
	a, b := RunSchedule(42, DefaultSchedule), RunSchedule(42, DefaultSchedule)
	if a.Sent != b.Sent || a.Dropped != b.Dropped || a.Steps != b.Steps || a.Elections != b.Elections || a.Completed != b.Completed {
		t.Fatalf("seed 42 diverged:\n%+v\n%+v", a, b)
	}
}
