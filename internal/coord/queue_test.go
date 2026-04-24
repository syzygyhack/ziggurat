package coord

import (
	"testing"

	"github.com/syzygyhack/ziggurat/internal/model"
)

func TestQueue_PriorityOrdering(t *testing.T) {
	q := NewQueue()

	low := &model.Task{ID: "low", Config: model.TaskConfig{Priority: 1}}
	high := &model.Task{ID: "high", Config: model.TaskConfig{Priority: 10}}
	mid := &model.Task{ID: "mid", Config: model.TaskConfig{Priority: 5}}

	q.Push(low)
	q.Push(high)
	q.Push(mid)

	got := q.Pop(nil, nil)
	if got == nil || got.ID != "high" {
		t.Fatalf("expected high priority task, got %v", got)
	}
	got = q.Pop(nil, nil)
	if got == nil || got.ID != "mid" {
		t.Fatalf("expected mid priority task, got %v", got)
	}
	got = q.Pop(nil, nil)
	if got == nil || got.ID != "low" {
		t.Fatalf("expected low priority task, got %v", got)
	}
}

func TestQueue_FIFOWithinPriority(t *testing.T) {
	q := NewQueue()

	first := &model.Task{ID: "first", Config: model.TaskConfig{Priority: 0}}
	second := &model.Task{ID: "second", Config: model.TaskConfig{Priority: 0}}
	third := &model.Task{ID: "third", Config: model.TaskConfig{Priority: 0}}

	q.Push(first)
	q.Push(second)
	q.Push(third)

	got := q.Pop(nil, nil)
	if got == nil || got.ID != "first" {
		t.Fatalf("expected first, got %v", got)
	}
	got = q.Pop(nil, nil)
	if got == nil || got.ID != "second" {
		t.Fatalf("expected second, got %v", got)
	}
}

func TestQueue_TagMatching(t *testing.T) {
	q := NewQueue()

	gpuTask := &model.Task{ID: "gpu", Requires: []string{"gpu"}}
	anyTask := &model.Task{ID: "any"}

	q.Push(gpuTask)
	q.Push(anyTask)

	// Worker without gpu tag should skip gpu task.
	got := q.Pop([]string{"python3"}, nil)
	if got == nil || got.ID != "any" {
		t.Fatalf("expected any task, got %v", got)
	}

	// GPU task should still be in queue.
	if q.Len() != 1 {
		t.Fatalf("expected 1 task in queue, got %d", q.Len())
	}

	// Worker with gpu tag gets it.
	got = q.Pop([]string{"gpu"}, nil)
	if got == nil || got.ID != "gpu" {
		t.Fatalf("expected gpu task, got %v", got)
	}
}

func TestQueue_Remove(t *testing.T) {
	q := NewQueue()

	a := &model.Task{ID: "a"}
	b := &model.Task{ID: "b"}
	q.Push(a)
	q.Push(b)

	q.Remove("a")

	if q.Len() != 1 {
		t.Fatalf("expected 1 task after remove, got %d", q.Len())
	}
	got := q.Pop(nil, nil)
	if got == nil || got.ID != "b" {
		t.Fatalf("expected b, got %v", got)
	}
}

func TestQueue_EmptyPop(t *testing.T) {
	q := NewQueue()
	got := q.Pop(nil, nil)
	if got != nil {
		t.Fatalf("expected nil from empty queue, got %v", got)
	}
}

func TestQueue_ConstraintMatching(t *testing.T) {
	q := NewQueue()

	// Task requires >= 16GB VRAM.
	gpuTask := &model.Task{
		ID:          "gpu-heavy",
		Constraints: []string{"gpu.vram >= 16GB"},
	}
	// Task with no constraints.
	anyTask := &model.Task{ID: "any"}

	q.Push(gpuTask)
	q.Push(anyTask)

	// Worker with only 8GB VRAM should skip the GPU task.
	smallCaps := map[string]string{"gpu.vram": "8589934592"} // 8GB in bytes
	got := q.Pop(nil, smallCaps)
	if got == nil || got.ID != "any" {
		t.Fatalf("expected any task, got %v", got)
	}

	// GPU task should still be in queue.
	if q.Len() != 1 {
		t.Fatalf("expected 1 task in queue, got %d", q.Len())
	}

	// Worker with 32GB VRAM satisfies the constraint.
	bigCaps := map[string]string{"gpu.vram": "34359738368"} // 32GB in bytes
	got = q.Pop(nil, bigCaps)
	if got == nil || got.ID != "gpu-heavy" {
		t.Fatalf("expected gpu-heavy task, got %v", got)
	}
}
