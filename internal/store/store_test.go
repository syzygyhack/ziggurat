package store

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	s, err := New(DefaultTestConfig(), tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestStore_PutAndGet(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	data := []byte("hello world")
	hash, err := s.Put(ctx, "test/hello", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if hash == "" {
		t.Fatal("expected non-empty hash")
	}

	rc, err := s.Get(ctx, "test/hello")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	if closeErr := rc.Close(); closeErr != nil {
		t.Fatal("integrity check failed:", closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestStore_Deduplication(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	data := []byte("same content")
	hash1, _ := s.Put(ctx, "key1", bytes.NewReader(data))
	hash2, _ := s.Put(ctx, "key2", bytes.NewReader(data))

	if hash1 != hash2 {
		t.Fatalf("same content produced different hashes: %s vs %s", hash1, hash2)
	}
}

func TestStore_NamespaceOverwrite(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	s.Put(ctx, "key", bytes.NewReader([]byte("v1")))
	hash2, _ := s.Put(ctx, "key", bytes.NewReader([]byte("v2")))

	rc, _ := s.Get(ctx, "key")
	got, _ := io.ReadAll(rc)
	rc.Close()

	if string(got) != "v2" {
		t.Fatalf("expected v2 after overwrite, got %q", got)
	}

	// Resolve should return the new hash.
	resolved, _ := s.Resolve("key")
	if resolved != hash2 {
		t.Fatalf("expected resolve to return new hash %s, got %s", hash2, resolved)
	}
}

func TestStore_Delete(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	s.Put(ctx, "key", bytes.NewReader([]byte("data")))
	err := s.Delete(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Get(ctx, "key")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestStore_List(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	s.Put(ctx, "data/a", bytes.NewReader([]byte("a")))
	s.Put(ctx, "data/b", bytes.NewReader([]byte("b")))
	s.Put(ctx, "other/c", bytes.NewReader([]byte("c")))

	keys, err := s.List(ctx, "data/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys with prefix data/, got %d: %v", len(keys), keys)
	}
}

func TestStore_RefCounting(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	hash, _ := s.Put(ctx, "key", bytes.NewReader([]byte("data")))

	// IncrRef should succeed.
	if err := s.IncrRef(hash); err != nil {
		t.Fatal(err)
	}

	// Delete namespace — decrements one ref.
	s.Delete(ctx, "key")

	// Object should still exist (refcount was 2, now 1 after namespace delete).
	rc, err := s.GetByHash(ctx, hash)
	if err != nil {
		t.Fatal("object should still exist after partial deref:", err)
	}
	rc.Close()

	// Final deref.
	s.DecrRef(hash)
}

func TestStore_IncrRefMissingObjectReturnsError(t *testing.T) {
	s := testStore(t)

	err := s.IncrRef("deadbeef" + "00000000000000000000000000000000000000000000000000000000")
	if err == nil {
		t.Fatal("expected error when incrementing ref on nonexistent object")
	}
}

func TestStore_Stats(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	s.Put(ctx, "a", bytes.NewReader([]byte("hello")))
	s.Put(ctx, "b", bytes.NewReader([]byte("world!")))

	stats := s.Stats()
	if stats.Objects != 2 {
		t.Fatalf("expected 2 objects, got %d", stats.Objects)
	}
	if stats.UsedBytes != 11 {
		t.Fatalf("expected 11 bytes, got %d", stats.UsedBytes)
	}
}
