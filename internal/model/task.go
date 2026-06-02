package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// TaskStatus represents the current state of a task.
type TaskStatus int

const (
	TaskQueued     TaskStatus = iota // waiting in queue
	TaskScheduled                    // assigned to worker, not yet running
	TaskRunning                      // process executing
	TaskCompleted                    // exit 0, output uploaded
	TaskFailed                       // retries exhausted or system error
	TaskCancelling                   // SIGTERM sent, waiting for exit
	TaskCancelled                    // cancelled by user or pipeline
	TaskDeadLetter                   // retries exhausted, moved to dead letter queue
)

// IsTerminal returns true if the task is in a terminal state (no further
// state transitions will occur).
func (s TaskStatus) IsTerminal() bool {
	return s == TaskCompleted || s == TaskFailed || s == TaskCancelled || s == TaskDeadLetter
}

func (s TaskStatus) String() string {
	switch s {
	case TaskQueued:
		return "queued"
	case TaskScheduled:
		return "scheduled"
	case TaskRunning:
		return "running"
	case TaskCompleted:
		return "completed"
	case TaskFailed:
		return "failed"
	case TaskCancelling:
		return "cancelling"
	case TaskCancelled:
		return "cancelled"
	case TaskDeadLetter:
		return "dead_letter"
	default:
		return "unknown"
	}
}

// MarshalJSON serializes TaskStatus as a human-readable string.
func (s TaskStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON deserializes TaskStatus from a string (or numeric for backwards compat).
func (s *TaskStatus) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		// Fall back to numeric for data persisted before this change.
		var n int
		if err2 := json.Unmarshal(data, &n); err2 != nil {
			return err
		}
		*s = TaskStatus(n)
		return nil
	}
	switch str {
	case "queued":
		*s = TaskQueued
	case "scheduled":
		*s = TaskScheduled
	case "running":
		*s = TaskRunning
	case "completed":
		*s = TaskCompleted
	case "failed":
		*s = TaskFailed
	case "cancelling":
		*s = TaskCancelling
	case "cancelled":
		*s = TaskCancelled
	case "dead_letter":
		*s = TaskDeadLetter
	default:
		return fmt.Errorf("unknown task status: %s", str)
	}
	return nil
}

// Task represents a unit of work submitted to the cluster.
type Task struct {
	ID          string            `json:"id"`
	Command     []string          `json:"command"`
	Env         map[string]string `json:"env,omitempty"`
	InputRefs   map[string]string `json:"input_refs,omitempty"` // name -> content hash (resolved at submission)
	Artifacts   []string          `json:"artifacts,omitempty"`  // content hashes (resolved at submission)
	Params      map[string]string `json:"params,omitempty"`
	Requires    []string          `json:"requires,omitempty"`
	Constraints []string          `json:"constraints,omitempty"` // capability constraint expressions
	Resources   ResourceReq       `json:"resources,omitempty"`
	Image       string            `json:"image,omitempty"`
	Environment *TaskEnvironment  `json:"environment,omitempty"`
	Config      TaskConfig        `json:"config"`

	// Set by coordinator / worker.
	Status       TaskStatus  `json:"status"`
	Attempt      int         `json:"attempt"`
	Worker       string      `json:"worker,omitempty"`
	RemoteOrigin bool        `json:"remote_origin,omitempty"` // true if accepted via dispatch (no local refcounts)
	OutputRef    string      `json:"output_ref,omitempty"`
	Stdout       string      `json:"stdout,omitempty"`
	Stderr       string      `json:"stderr,omitempty"`
	ExitCode     int         `json:"exit_code"`
	Error        string      `json:"error,omitempty"`
	Metrics      TaskMetrics `json:"metrics"`
	CreatedAt    time.Time   `json:"created_at"`
}

// ResourceReq specifies optional resource requests for scheduler admission.
type ResourceReq struct {
	Memory   int64 `json:"memory,omitempty"`    // bytes, 0 = no requirement
	CPUCores int   `json:"cpu_cores,omitempty"` // logical cores, 0 = no requirement
	GPUs     int   `json:"gpus,omitempty"`      // GPU devices, 0 = no requirement
}

// TaskConfig holds per-task configuration overrides.
type TaskConfig struct {
	Priority      int      `json:"priority"`
	Timeout       Duration `json:"timeout"`
	MaxRetries    int      `json:"max_retries"`
	MaxOutputSize int64    `json:"max_output_size,omitempty"` // 0 = cluster default
	Affinity      string   `json:"affinity,omitempty"`
	KeepWorkspace bool     `json:"keep_workspace,omitempty"`
}

// TaskEnvironment configures a persistent, reusable environment for a task.
// The worker maintains a directory that survives workspace cleanup, identified
// by Name (explicit) or derived from the BLAKE3 hash of Fingerprint file
// contents. When Setup is provided and the fingerprint is stale (or the env
// doesn't exist), the setup command runs before the main task command.
type TaskEnvironment struct {
	Name        string   `json:"name,omitempty"`        // explicit env name; empty = derive from fingerprint
	Setup       []string `json:"setup,omitempty"`       // command to run when env needs (re)creation
	Fingerprint []string `json:"fingerprint,omitempty"` // input/artifact filenames whose content hash determines staleness
}

// TaskMetrics captures timing and size information for a task execution.
type TaskMetrics struct {
	QueuedAt    time.Time `json:"queued_at"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	WallTime    Duration  `json:"wall_time,omitempty"`
	OutputBytes int64     `json:"output_bytes,omitempty"`
}

// Duration wraps time.Duration with human-readable JSON marshaling.
// Accepts strings ("5m", "10s", "1h30m") and nanosecond integers on unmarshal.
// Marshals as a string like "5m0s".
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		parsed, err := time.ParseDuration(str)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", str, err)
		}
		*d = Duration(parsed)
		return nil
	}
	// Fall back to numeric nanoseconds for backwards compat.
	var ns int64
	if err := json.Unmarshal(data, &ns); err != nil {
		return fmt.Errorf("duration must be a string (\"5m\") or nanoseconds integer")
	}
	*d = Duration(ns)
	return nil
}

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}
