package transport

import (
	"reflect"
	"testing"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
)

// TestTaskProtoRoundTrip guards against drift between taskToProto and
// protoToTask: every field carried on the DispatchTaskRequest wire must survive
// a round trip unchanged. If a new wire field is added to one direction but not
// the other, this fails.
func TestTaskProtoRoundTrip(t *testing.T) {
	orig := &model.Task{
		ID:            "task-1",
		Command:       []string{"python3", "run.py", "--x"},
		Env:           map[string]string{"K": "V"},
		InputRefs:     map[string]string{"data": "hash-in"},
		Artifacts:     []string{"hash-a"},
		ArtifactNames: []string{"run.py"},
		Params:        map[string]string{"seed": "7"},
		Requires:      []string{"python3"},
		Constraints:   []string{"gpu.vram >= 8GB"},
		Image:         "ghcr.io/example/x:1",
		Resources:     model.ResourceReq{Memory: 1 << 30, CPUCores: 4, GPUs: 1},
		Environment: &model.TaskEnvironment{
			Name:        "venv",
			Setup:       []string{"pip", "install", "-r", "requirements.txt"},
			Fingerprint: []string{"requirements.txt"},
		},
		Config: model.TaskConfig{
			Priority:      5,
			Timeout:       model.Duration(10 * time.Minute),
			MaxRetries:    2,
			MaxOutputSize: 1 << 20,
			Affinity:      "node-a",
			KeepWorkspace: true,
		},
		Attempt: 3,
	}

	got := protoToTask(taskToProto(orig))
	if !reflect.DeepEqual(orig, got) {
		t.Errorf("round trip mismatch:\n orig = %+v\n got  = %+v", orig, got)
	}
}

// TestTaskProtoRoundTrip_Minimal ensures the nil/zero guards (Environment,
// Resources) don't materialize empty structs on the way back.
func TestTaskProtoRoundTrip_Minimal(t *testing.T) {
	orig := &model.Task{Command: []string{"echo", "hi"}}
	got := protoToTask(taskToProto(orig))
	if !reflect.DeepEqual(orig, got) {
		t.Errorf("minimal round trip mismatch:\n orig = %+v\n got  = %+v", orig, got)
	}
}
