package worker

import (
	"strings"
	"testing"

	"github.com/syzygyhack/ziggurat/internal/model"
)

func TestBuildEnv_ZigguratVarsOverrideInherited(t *testing.T) {
	task := &model.Task{
		ID:      "test-123",
		Attempt: 0,
	}

	env := BuildEnv(task, "/ws", "/ws/input", "/ws/output")

	// Count how many times ZIGGURAT_TASK_ID appears.
	count := 0
	for _, e := range env {
		if strings.HasPrefix(e, "ZIGGURAT_TASK_ID=") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("ZIGGURAT_TASK_ID appears %d times in env, want exactly 1", count)
	}
}

func TestBuildEnv_UserCannotOverrideZiggurat(t *testing.T) {
	task := &model.Task{
		ID:      "test-123",
		Attempt: 0,
		Env: map[string]string{
			"ZIGGURAT_WORKSPACE": "/evil",
			"ziggurat_task_id":   "evil-id",
		},
	}

	env := BuildEnv(task, "/ws", "/ws/input", "/ws/output")

	for _, e := range env {
		if e == "ZIGGURAT_WORKSPACE=/evil" || e == "ziggurat_task_id=evil-id" {
			t.Errorf("user-supplied ZIGGURAT_* var leaked into env: %s", e)
		}
	}
}

func TestBuildEnv_ProtectedVarsNotOverridden(t *testing.T) {
	task := &model.Task{
		ID:      "test-123",
		Attempt: 0,
		Env: map[string]string{
			"PATH": "/evil",
			"HOME": "/evil",
		},
	}

	env := BuildEnv(task, "/ws", "/ws/input", "/ws/output")

	for _, e := range env {
		if e == "PATH=/evil" || e == "HOME=/evil" {
			t.Errorf("user-supplied protected var leaked into env: %s", e)
		}
	}
}

func TestBuildEnv_ParamsInjected(t *testing.T) {
	task := &model.Task{
		ID:      "test-123",
		Attempt: 0,
		Params: map[string]string{
			"scale": "mZ",
		},
	}

	env := BuildEnv(task, "/ws", "/ws/input", "/ws/output")

	found := false
	for _, e := range env {
		if e == "ZIGGURAT_PARAM_SCALE=mZ" {
			found = true
		}
	}
	if !found {
		t.Error("expected ZIGGURAT_PARAM_SCALE=mZ in env")
	}
}

func TestBuildEnv_NoDuplicateZigguratVars(t *testing.T) {
	task := &model.Task{
		ID:      "test-123",
		Attempt: 0,
	}

	env := BuildEnv(task, "/ws", "/ws/input", "/ws/output")

	seen := make(map[string]int)
	for _, e := range env {
		if strings.HasPrefix(e, "ZIGGURAT_") {
			key := strings.SplitN(e, "=", 2)[0]
			seen[key]++
		}
	}
	for key, count := range seen {
		if count > 1 {
			t.Errorf("%s appears %d times, want 1", key, count)
		}
	}
}
