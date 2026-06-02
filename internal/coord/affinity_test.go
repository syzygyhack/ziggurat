package coord

import (
	"context"
	"testing"

	"github.com/syzygyhack/ziggurat/internal/model"
)

// A task pinned (by affinity) to another node that has capacity must NOT be
// dequeued by the local worker — it is left queued for remote dispatch. When
// the preferred node fills up, the local worker reclaims it (soft affinity,
// no starvation).
func TestDequeue_YieldsAffinityToRemoteWithCapacity(t *testing.T) {
	c, _ := testSetup(t)
	const local = "local-node"
	const remote = "remote-x"

	task := &model.Task{Command: []string{"echo", "hi"}}
	task.Config.Affinity = remote
	submitted, err := c.Submit(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}

	// Remote node is known and idle → has capacity.
	c.workerLoad.SetLimit(remote, 2)

	// Local worker should yield: affinity points to a node that can take it.
	if got := c.Dequeue(nil, map[string]string{}, local); got != nil {
		t.Fatalf("expected nil (yield to remote), got task %s", got.ID)
	}
	// Task must remain queued for the dispatcher.
	if c.queue.Len() != 1 {
		t.Fatalf("expected task to remain queued, queue len = %d", c.queue.Len())
	}

	// Fill the remote node to capacity → affinity can no longer be honored.
	c.workerLoad.TaskStarted(remote)
	c.workerLoad.TaskStarted(remote)

	// Local worker now reclaims the task (fallback, no starvation).
	got := c.Dequeue(nil, map[string]string{}, local)
	if got == nil || got.ID != submitted.ID {
		t.Fatalf("expected local fallback to dequeue %s, got %v", submitted.ID, got)
	}
}

// Affinity to the dequeuing worker itself is honored locally.
func TestDequeue_AffinityToSelfRunsLocally(t *testing.T) {
	c, _ := testSetup(t)
	const local = "local-node"

	task := &model.Task{Command: []string{"echo", "hi"}}
	task.Config.Affinity = local
	submitted, _ := c.Submit(context.Background(), task)

	c.workerLoad.SetLimit(local, 4)

	got := c.Dequeue(nil, map[string]string{}, local)
	if got == nil || got.ID != submitted.ID {
		t.Fatalf("expected self-affinity task to run locally, got %v", got)
	}
}

// Affinity to a node that isn't known (departed / never seen) falls back to
// local execution rather than stalling.
func TestDequeue_AffinityToUnknownNodeRunsLocally(t *testing.T) {
	c, _ := testSetup(t)

	task := &model.Task{Command: []string{"echo", "hi"}}
	task.Config.Affinity = "ghost-node"
	submitted, _ := c.Submit(context.Background(), task)

	got := c.Dequeue(nil, map[string]string{}, "local-node")
	if got == nil || got.ID != submitted.ID {
		t.Fatalf("expected unknown-affinity task to run locally, got %v", got)
	}
}

// The dispatcher's PopForRemote claims an affinity-pinned task that the local
// worker yielded, so it can be sent to the preferred node.
func TestPopForRemote_ClaimsAffinityTask(t *testing.T) {
	c, _ := testSetup(t)
	const local = "local-node"
	const remote = "remote-x"

	task := &model.Task{Command: []string{"echo", "hi"}}
	task.Config.Affinity = remote
	submitted, _ := c.Submit(context.Background(), task)
	c.workerLoad.SetLimit(remote, 2)

	prefersRemote := func(t *model.Task) bool {
		aff := t.Config.Affinity
		return aff != "" && aff != local && c.WorkerHasCapacity(aff)
	}

	// The local worker could run this task (no tags/constraints), but affinity
	// pins it remotely, so PopForRemote must claim it.
	got := c.queue.PopForRemote(nil, map[string]string{}, prefersRemote)
	if got == nil || got.ID != submitted.ID {
		t.Fatalf("expected PopForRemote to claim affinity task %s, got %v", submitted.ID, got)
	}
}
