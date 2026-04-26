package model

import "testing"

func TestStage_IsTerminal(t *testing.T) {
	tests := []struct {
		status TaskStatus
		want   bool
	}{
		{TaskQueued, false},
		{TaskScheduled, false},
		{TaskRunning, false},
		{TaskCancelling, false},
		{TaskCompleted, true},
		{TaskFailed, true},
		{TaskCancelled, true},
		{TaskDeadLetter, true},
	}
	for _, tt := range tests {
		s := Stage{Status: tt.status}
		if got := s.IsTerminal(); got != tt.want {
			t.Errorf("Stage{Status: %v}.IsTerminal() = %v, want %v", tt.status, got, tt.want)
		}
	}
}
