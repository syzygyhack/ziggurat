package model

import "time"

// PipelineStatus represents the current state of a pipeline.
type PipelineStatus string

const (
	PipelineRunning   PipelineStatus = "running"
	PipelineCompleted PipelineStatus = "completed"
	PipelineFailed    PipelineStatus = "failed"
	PipelineCancelled PipelineStatus = "cancelled"
)

// Pipeline represents an ordered group of tasks with dependency edges.
type Pipeline struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Stages    []Stage        `json:"stages"`
	Status    PipelineStatus `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
	Error     string         `json:"error,omitempty"`
}

// Stage is a single step in a pipeline.
type Stage struct {
	ID          string            `json:"id"`
	Command     []string          `json:"command"`
	Artifacts   []string          `json:"artifacts,omitempty"`
	InputRefs   map[string]string `json:"input_refs,omitempty"` // can use "$<stage_id>.output" syntax
	Params      map[string]string `json:"params,omitempty"`
	Requires    []string          `json:"requires,omitempty"`
	Constraints []string          `json:"constraints,omitempty"` // capability constraint expressions
	Image       string            `json:"image,omitempty"`
	DependsOn   []string          `json:"depends_on,omitempty"` // stage IDs
	Config      TaskConfig        `json:"config,omitempty"`

	// Set by coordinator.
	TaskID string     `json:"task_id,omitempty"` // resolved task ID once scheduled
	Status TaskStatus `json:"status"`
	Error  string     `json:"error,omitempty"`
}

// IsTerminal returns true if the stage is in a terminal state.
func (s *Stage) IsTerminal() bool {
	return s.Status == TaskCompleted || s.Status == TaskFailed ||
		s.Status == TaskCancelled || s.Status == TaskDeadLetter
}
