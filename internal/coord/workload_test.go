package coord

import (
	"log/slog"
	"os"
	"runtime"
	"testing"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestConcurrencyLimitFromCaps(t *testing.T) {
	cases := []struct {
		name string
		caps map[string]string
		want int
	}{
		{"prefers compute.concurrency", map[string]string{"compute.concurrency": "4", "cpu.cores": "16"}, 4},
		{"falls back to cpu.cores", map[string]string{"cpu.cores": "16"}, 16},
		{"ignores zero concurrency, uses cores", map[string]string{"compute.concurrency": "0", "cpu.cores": "8"}, 8},
		{"ignores non-numeric, uses cores", map[string]string{"compute.concurrency": "x", "cpu.cores": "8"}, 8},
		{"none present", map[string]string{"arch": "amd64"}, 0},
		{"nil caps", nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := concurrencyLimitFromCaps(c.caps); got != c.want {
				t.Errorf("concurrencyLimitFromCaps(%v) = %d, want %d", c.caps, got, c.want)
			}
		})
	}
}

func TestSetWorkerLimitFromCaps(t *testing.T) {
	c := New(nil, nil, TaskDefaults{}, quietLogger())

	// Heterogeneous node: a 24-core worker advertises its capacity.
	c.SetWorkerLimitFromCaps("big", map[string]string{"cpu.cores": "24"})
	if _, limit := c.workerLoad.Load("big"); limit != 24 {
		t.Errorf("limit for big = %d, want 24", limit)
	}

	// Explicit concurrency overrides core count.
	c.SetWorkerLimitFromCaps("capped", map[string]string{"compute.concurrency": "2", "cpu.cores": "16"})
	if _, limit := c.workerLoad.Load("capped"); limit != 2 {
		t.Errorf("limit for capped = %d, want 2", limit)
	}

	// Unknown node falls back to local CPU count (not another node's limit).
	if _, limit := c.workerLoad.Load("unknown"); limit != runtime.GOMAXPROCS(0) {
		t.Errorf("limit for unknown = %d, want GOMAXPROCS %d", limit, runtime.GOMAXPROCS(0))
	}

	// No usable caps: limit stays at the fallback, not forced to something bogus.
	c.SetWorkerLimitFromCaps("bare", map[string]string{"arch": "amd64"})
	if _, limit := c.workerLoad.Load("bare"); limit != runtime.GOMAXPROCS(0) {
		t.Errorf("limit for bare = %d, want GOMAXPROCS %d", limit, runtime.GOMAXPROCS(0))
	}
}

func TestOverloadedWorkers_CapacityAware(t *testing.T) {
	wl := NewWorkerLoad()
	// Two-node cluster: a small node oversubscribed, a big node with headroom.
	wl.SetLimit("small", 2)
	wl.SetLimit("big", 16)
	for i := 0; i < 5; i++ { // small: 5 running > limit 2 → overloaded
		wl.TaskStarted("small")
	}
	for i := 0; i < 10; i++ { // big: 10 running < limit 16 → not overloaded
		wl.TaskStarted("big")
	}

	got := wl.OverloadedWorkers()
	if len(got) != 1 || got[0] != "small" {
		t.Fatalf("OverloadedWorkers = %v, want [small]", got)
	}

	// The big node running MORE tasks than the small node is still not flagged:
	// capacity-aware, not raw-count. (Old median heuristic would mis-rank these.)
	if containsStr(got, "big") {
		t.Error("big node should not be overloaded — it is within its limit")
	}
}

func TestOverloadedWorkers_NoneWhenWithinLimits(t *testing.T) {
	wl := NewWorkerLoad()
	wl.SetLimit("a", 4)
	wl.SetLimit("b", 4)
	wl.TaskStarted("a")
	wl.TaskStarted("a")
	wl.TaskStarted("b")
	if got := wl.OverloadedWorkers(); len(got) != 0 {
		t.Errorf("OverloadedWorkers = %v, want none (all within limits)", got)
	}
}

func TestOverloadedWorkers_NeedsTwoWorkers(t *testing.T) {
	wl := NewWorkerLoad()
	wl.SetLimit("solo", 1)
	wl.TaskStarted("solo")
	wl.TaskStarted("solo") // oversubscribed, but no peer to receive
	if got := wl.OverloadedWorkers(); got != nil {
		t.Errorf("OverloadedWorkers = %v, want nil with a single worker", got)
	}
}

func TestHasCapacity(t *testing.T) {
	wl := NewWorkerLoad()
	wl.SetLimit("node", 2)

	if !wl.HasCapacity("node") {
		t.Error("idle node with limit 2 should have capacity")
	}
	wl.TaskStarted("node")
	if !wl.HasCapacity("node") {
		t.Error("node at 1/2 should have capacity")
	}
	wl.TaskStarted("node")
	if wl.HasCapacity("node") {
		t.Error("node at 2/2 should NOT have capacity")
	}
	if wl.HasCapacity("unknown") {
		t.Error("unknown node should report no capacity (treated as unavailable)")
	}
}

func containsStr(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestClearWorker(t *testing.T) {
	c := New(nil, nil, TaskDefaults{}, quietLogger())
	c.SetWorkerLimitFromCaps("gone", map[string]string{"cpu.cores": "12"})
	c.workerLoad.TaskStarted("gone")
	c.workerLoad.AllocResources("gone", 1024, 2, 1)

	c.ClearWorker("gone")

	running, limit := c.workerLoad.Load("gone")
	if running != 0 {
		t.Errorf("running after clear = %d, want 0", running)
	}
	if limit != runtime.GOMAXPROCS(0) {
		t.Errorf("limit after clear = %d, want fallback GOMAXPROCS %d", limit, runtime.GOMAXPROCS(0))
	}
	if alloc := c.workerLoad.Allocated("gone"); alloc.Memory != 0 || alloc.CPUCores != 0 || alloc.GPUs != 0 {
		t.Errorf("alloc after clear = %+v, want zero", alloc)
	}
}
