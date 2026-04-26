package node

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreateID_CreatesNew(t *testing.T) {
	dir := t.TempDir()

	id, err := LoadOrCreateID(dir)
	if err != nil {
		t.Fatalf("LoadOrCreateID: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty ID")
	}

	// Verify file was created.
	data, err := os.ReadFile(filepath.Join(dir, "node.id"))
	if err != nil {
		t.Fatalf("read node.id: %v", err)
	}
	if got := string(data); got != id+"\n" {
		t.Errorf("file contents = %q, want %q", got, id+"\n")
	}
}

func TestLoadOrCreateID_ReadsExisting(t *testing.T) {
	dir := t.TempDir()

	// Pre-write an ID.
	expected := "existing-uuid-1234"
	os.WriteFile(filepath.Join(dir, "node.id"), []byte(expected+"\n"), 0o644)

	id, err := LoadOrCreateID(dir)
	if err != nil {
		t.Fatalf("LoadOrCreateID: %v", err)
	}
	if id != expected {
		t.Errorf("got %q, want %q", id, expected)
	}
}

func TestLoadOrCreateID_StableAcrossCalls(t *testing.T) {
	dir := t.TempDir()

	id1, err := LoadOrCreateID(dir)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := LoadOrCreateID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Errorf("IDs differ across calls: %q vs %q", id1, id2)
	}
}

func TestLoadOrCreateID_CreatesDataDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data")

	id, err := LoadOrCreateID(dir)
	if err != nil {
		t.Fatalf("LoadOrCreateID: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty ID")
	}

	// Verify nested dirs were created.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Error("data dir was not created")
	}
}

func TestLoadOrCreateID_EmptyFileRegenerates(t *testing.T) {
	dir := t.TempDir()

	// Write an empty file.
	os.WriteFile(filepath.Join(dir, "node.id"), []byte(""), 0o644)

	id, err := LoadOrCreateID(dir)
	if err != nil {
		t.Fatalf("LoadOrCreateID: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty ID for empty file")
	}
}
