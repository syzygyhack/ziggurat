package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/syzygyhack/ziggurat/internal/config"
)

// testStoreWithEC creates a store with erasure coding enabled and a low
// large-tier threshold so tests don't need huge payloads.
func testStoreWithEC(t *testing.T) *Store {
	t.Helper()
	tmpDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.StorageConfig{
		ReplicationFactor: 1,
		Erasure: config.ErasureConfig{
			Enabled:      true,
			DataShards:   4,
			ParityShards: 2,
		},
		TierThresholds: config.TierConfig{
			Medium: 1 << 10, // 1 KB
			Large:  4 << 10, // 4 KB — low threshold for testing
		},
	}
	s, err := New(cfg, tmpDir, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestStore_ECPutAndGet(t *testing.T) {
	s := testStoreWithEC(t)
	ctx := context.Background()

	// Create data larger than the large threshold (4 KB).
	data := make([]byte, 8*1024) // 8 KB
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	hash, err := s.Put(ctx, "test/ec-object", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if hash == "" {
		t.Fatal("expected non-empty hash")
	}

	// Verify object metadata has erasure params.
	meta, err := s.getMeta(hash)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Erasure == nil {
		t.Fatal("expected erasure params for large object")
	}
	if meta.Erasure.DataShards != 4 || meta.Erasure.ParityShards != 2 {
		t.Fatalf("wrong EC params: %d+%d", meta.Erasure.DataShards, meta.Erasure.ParityShards)
	}
	if len(meta.Erasure.ShardHashes) != 6 {
		t.Fatalf("expected 6 shard hashes, got %d", len(meta.Erasure.ShardHashes))
	}

	// Read back via normal path (blob still on disk).
	rc, err := s.Get(ctx, "test/ec-object")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch via normal Get")
	}
}

func TestStore_ECReconstructFromShards(t *testing.T) {
	s := testStoreWithEC(t)
	ctx := context.Background()

	data := make([]byte, 8*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	hash, err := s.Put(ctx, "test/ec-reconstruct", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Delete the full blob to force EC reconstruction.
	blobFile := blobPath(s.dir, hash)
	if err := os.Remove(blobFile); err != nil {
		t.Fatalf("failed to remove blob for test: %v", err)
	}

	// GetByHash should reconstruct from EC shards.
	rc, err := s.GetByHash(ctx, hash)
	if err != nil {
		t.Fatalf("EC reconstruction failed: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("reconstructed data mismatch")
	}
}

func TestStore_SmallObjectSkipsEC(t *testing.T) {
	s := testStoreWithEC(t)
	ctx := context.Background()

	// Small object (below medium threshold of 1 KB).
	data := []byte("small")
	hash, err := s.Put(ctx, "test/small", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	meta, err := s.getMeta(hash)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Erasure != nil {
		t.Fatal("small objects should not have erasure params")
	}
}

func TestStore_ECCodecAccessor(t *testing.T) {
	s := testStoreWithEC(t)
	if s.ErasureCodec() == nil {
		t.Fatal("expected non-nil erasure codec")
	}

	// Store without EC.
	s2 := testStore(t)
	if s2.ErasureCodec() != nil {
		t.Fatal("expected nil erasure codec for non-EC store")
	}
}
