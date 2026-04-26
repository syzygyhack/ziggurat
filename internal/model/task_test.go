package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTaskStatus_String(t *testing.T) {
	tests := []struct {
		status TaskStatus
		want   string
	}{
		{TaskQueued, "queued"},
		{TaskScheduled, "scheduled"},
		{TaskRunning, "running"},
		{TaskCompleted, "completed"},
		{TaskFailed, "failed"},
		{TaskCancelling, "cancelling"},
		{TaskCancelled, "cancelled"},
		{TaskDeadLetter, "dead_letter"},
		{TaskStatus(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.status.String(); got != tt.want {
			t.Errorf("TaskStatus(%d).String() = %q, want %q", tt.status, got, tt.want)
		}
	}
}

func TestTaskStatus_MarshalJSON(t *testing.T) {
	tests := []struct {
		status TaskStatus
		want   string
	}{
		{TaskQueued, `"queued"`},
		{TaskRunning, `"running"`},
		{TaskCompleted, `"completed"`},
		{TaskFailed, `"failed"`},
		{TaskCancelled, `"cancelled"`},
		{TaskDeadLetter, `"dead_letter"`},
	}
	for _, tt := range tests {
		got, err := json.Marshal(tt.status)
		if err != nil {
			t.Fatalf("Marshal(%v) error: %v", tt.status, err)
		}
		if string(got) != tt.want {
			t.Errorf("Marshal(%v) = %s, want %s", tt.status, got, tt.want)
		}
	}
}

func TestTaskStatus_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    TaskStatus
		wantErr bool
	}{
		{"queued", `"queued"`, TaskQueued, false},
		{"scheduled", `"scheduled"`, TaskScheduled, false},
		{"running", `"running"`, TaskRunning, false},
		{"completed", `"completed"`, TaskCompleted, false},
		{"failed", `"failed"`, TaskFailed, false},
		{"cancelling", `"cancelling"`, TaskCancelling, false},
		{"cancelled", `"cancelled"`, TaskCancelled, false},
		{"dead_letter", `"dead_letter"`, TaskDeadLetter, false},
		{"numeric 0", `0`, TaskQueued, false},
		{"numeric 3", `3`, TaskCompleted, false},
		{"unknown string", `"bogus"`, 0, true},
		{"invalid type", `true`, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got TaskStatus
			err := json.Unmarshal([]byte(tt.input), &got)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Unmarshal(%s) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("Unmarshal(%s) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestTaskStatus_JSONRoundTrip(t *testing.T) {
	for _, s := range []TaskStatus{
		TaskQueued, TaskScheduled, TaskRunning, TaskCompleted,
		TaskFailed, TaskCancelling, TaskCancelled, TaskDeadLetter,
	} {
		data, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("Marshal(%v): %v", s, err)
		}
		var got TaskStatus
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("Unmarshal(%s): %v", data, err)
		}
		if got != s {
			t.Errorf("roundtrip: got %v, want %v", got, s)
		}
	}
}

func TestDuration_MarshalJSON(t *testing.T) {
	tests := []struct {
		dur  Duration
		want string
	}{
		{Duration(5 * time.Minute), `"5m0s"`},
		{Duration(time.Second), `"1s"`},
		{Duration(0), `"0s"`},
		{Duration(90 * time.Second), `"1m30s"`},
	}
	for _, tt := range tests {
		got, err := json.Marshal(tt.dur)
		if err != nil {
			t.Fatalf("Marshal(%v) error: %v", tt.dur, err)
		}
		if string(got) != tt.want {
			t.Errorf("Marshal(%v) = %s, want %s", tt.dur, got, tt.want)
		}
	}
}

func TestDuration_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Duration
		wantErr bool
	}{
		{"string 5m", `"5m"`, Duration(5 * time.Minute), false},
		{"string 10s", `"10s"`, Duration(10 * time.Second), false},
		{"string 1h30m", `"1h30m"`, Duration(90 * time.Minute), false},
		{"nanoseconds", `1000000000`, Duration(time.Second), false},
		{"zero ns", `0`, Duration(0), false},
		{"invalid string", `"not-a-duration"`, 0, true},
		{"invalid type", `true`, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Duration
			err := json.Unmarshal([]byte(tt.input), &got)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Unmarshal(%s) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("Unmarshal(%s) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestDuration_JSONRoundTrip(t *testing.T) {
	durations := []Duration{
		Duration(0),
		Duration(time.Second),
		Duration(5 * time.Minute),
		Duration(2*time.Hour + 30*time.Minute),
	}
	for _, d := range durations {
		data, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("Marshal(%v): %v", d, err)
		}
		var got Duration
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("Unmarshal(%s): %v", data, err)
		}
		if got != d {
			t.Errorf("roundtrip: got %v, want %v", got, d)
		}
	}
}

func TestDuration_Duration(t *testing.T) {
	d := Duration(5 * time.Minute)
	if got := d.Duration(); got != 5*time.Minute {
		t.Errorf("Duration() = %v, want %v", got, 5*time.Minute)
	}
}

func TestTaskStatus_UnmarshalJSON_InStruct(t *testing.T) {
	type wrapper struct {
		Status TaskStatus `json:"status"`
	}

	// String form.
	var w wrapper
	if err := json.Unmarshal([]byte(`{"status":"completed"}`), &w); err != nil {
		t.Fatalf("Unmarshal string: %v", err)
	}
	if w.Status != TaskCompleted {
		t.Errorf("got %v, want %v", w.Status, TaskCompleted)
	}

	// Numeric backwards compat.
	if err := json.Unmarshal([]byte(`{"status":4}`), &w); err != nil {
		t.Fatalf("Unmarshal numeric: %v", err)
	}
	if w.Status != TaskFailed {
		t.Errorf("got %v, want %v", w.Status, TaskFailed)
	}
}
