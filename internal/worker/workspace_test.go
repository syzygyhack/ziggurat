package worker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnforceWorkspaceLimit_RemovesOldest(t *testing.T) {
	tmpDir := t.TempDir()

	// Create 5 workspace dirs with staggered mtimes.
	for i := 0; i < 5; i++ {
		name := filepath.Join(tmpDir, "task-"+string(rune('a'+i)))
		if err := os.MkdirAll(name, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// Enforce limit of 3.
	EnforceWorkspaceLimit(tmpDir, 3)

	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 workspaces after enforcement, got %d", len(entries))
	}
}

func TestEnforceWorkspaceLimit_NoOpUnderLimit(t *testing.T) {
	tmpDir := t.TempDir()

	os.MkdirAll(filepath.Join(tmpDir, "task-a"), 0o755)
	os.MkdirAll(filepath.Join(tmpDir, "task-b"), 0o755)

	EnforceWorkspaceLimit(tmpDir, 10)

	entries, _ := os.ReadDir(tmpDir)
	if len(entries) != 2 {
		t.Fatalf("expected 2 workspaces (under limit), got %d", len(entries))
	}
}

func TestEnforceWorkspaceLimit_ZeroDisabled(t *testing.T) {
	tmpDir := t.TempDir()

	for i := 0; i < 5; i++ {
		os.MkdirAll(filepath.Join(tmpDir, "task-"+string(rune('a'+i))), 0o755)
	}

	// 0 means disabled — no eviction.
	EnforceWorkspaceLimit(tmpDir, 0)

	entries, _ := os.ReadDir(tmpDir)
	if len(entries) != 5 {
		t.Fatalf("expected 5 workspaces (limit disabled), got %d", len(entries))
	}
}
