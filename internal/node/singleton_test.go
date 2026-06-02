package node

import (
	"path/filepath"
	"testing"
)

func TestSingleInstance_RejectsSecond(t *testing.T) {
	lock := filepath.Join(t.TempDir(), "ziggurat.lock")

	rel1, err := acquireSingleInstanceAt(lock)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	// A second acquire on the same path must be rejected while the first holds.
	if _, err := acquireSingleInstanceAt(lock); err == nil {
		t.Fatal("second acquire succeeded; expected rejection while lock is held")
	}

	// After release, a new acquire must succeed.
	rel1()
	rel2, err := acquireSingleInstanceAt(lock)
	if err != nil {
		t.Fatalf("re-acquire after release failed: %v", err)
	}
	rel2()
}

func TestSingleInstance_DistinctPathsCoexist(t *testing.T) {
	// Different lock paths (the Windows/WSL/two-machine case) must not conflict.
	dir := t.TempDir()
	relA, err := acquireSingleInstanceAt(filepath.Join(dir, "a.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer relA()
	relB, err := acquireSingleInstanceAt(filepath.Join(dir, "b.lock"))
	if err != nil {
		t.Fatalf("distinct lock path should not conflict: %v", err)
	}
	relB()
}
