package store

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/zeebo/blake3"
)

func TestErasureCodec_EncodeDecodeRoundtrip(t *testing.T) {
	codec, err := NewErasureCodec(4, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Generate random data (256 KB — divisible by 4 for clean splits).
	data := make([]byte, 256*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	shards, err := codec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(shards) != 6 {
		t.Fatalf("expected 6 shards, got %d", len(shards))
	}

	// Verify parity.
	ok, err := codec.Verify(shards)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("parity verification failed")
	}

	// Decode with all shards.
	decoded, err := codec.Decode(shards, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, data) {
		t.Fatal("roundtrip mismatch with all shards")
	}
}

func TestErasureCodec_DecodeWithMissing(t *testing.T) {
	codec, err := NewErasureCodec(4, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	shards, err := codec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Remove 2 shards (the parity tolerance limit).
	shards[1] = nil
	shards[4] = nil

	decoded, err := codec.Decode(shards, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, data) {
		t.Fatal("reconstruction failed with 2 missing shards")
	}
}

func TestErasureCodec_TooManyMissing(t *testing.T) {
	codec, err := NewErasureCodec(4, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	shards, err := codec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Remove 3 shards — exceeds tolerance (parity = 2).
	shards[0] = nil
	shards[2] = nil
	shards[5] = nil

	_, err = codec.Decode(shards, int64(len(data)))
	if err == nil {
		t.Fatal("expected error with 3 missing shards, got nil")
	}
}

func TestWriteReadShards(t *testing.T) {
	codec, err := NewErasureCodec(4, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	shards, err := codec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	storeDir := t.TempDir()
	hashHex := testHashHex(data)

	// Write all shards.
	hashes, err := WriteShards(storeDir, hashHex, shards)
	if err != nil {
		t.Fatal(err)
	}
	if len(hashes) != 6 {
		t.Fatalf("expected 6 shard hashes, got %d", len(hashes))
	}

	// Read back and verify.
	readBack := ReadLocalShards(storeDir, hashHex, 6, hashes)
	for i, s := range readBack {
		if s == nil {
			t.Fatalf("shard %d is nil", i)
		}
		if !bytes.Equal(s, shards[i]) {
			t.Fatalf("shard %d mismatch", i)
		}
	}
}

func TestListLocalShardIndices(t *testing.T) {
	storeDir := t.TempDir()
	hashHex := "aabbccddee112233445566778899aabb0011223344556677889900aabbccddeeff"

	// Create shard directory with some shards.
	dir := filepath.Join(storeDir, "ec", hashHex[:2], hashHex)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, idx := range []int{0, 2, 5} {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.shard", idx)), []byte("data"), 0o644)
	}

	indices, err := ListLocalShardIndices(storeDir, hashHex)
	if err != nil {
		t.Fatal(err)
	}
	if len(indices) != 3 || indices[0] != 0 || indices[1] != 2 || indices[2] != 5 {
		t.Fatalf("expected [0, 2, 5], got %v", indices)
	}
}

func TestEncodeAndWrite(t *testing.T) {
	codec, err := NewErasureCodec(4, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	storeDir := t.TempDir()
	contentHash, shardHashes, size, err := EncodeAndWrite(storeDir, bytes.NewReader(data), codec)
	if err != nil {
		t.Fatal(err)
	}

	if size != int64(len(data)) {
		t.Fatalf("expected size %d, got %d", len(data), size)
	}

	// Verify content hash.
	expected := testHash(data)
	if contentHash != expected {
		t.Fatal("content hash mismatch")
	}

	// Reconstruct from shards.
	decoded, err := ReadAndDecode(storeDir, hex.EncodeToString(contentHash[:]), int64(len(data)), codec, shardHashes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, data) {
		t.Fatal("reconstructed data mismatch")
	}
}

func TestDeleteShards(t *testing.T) {
	codec, err := NewErasureCodec(4, 2)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 32*1024)
	rand.Read(data)
	shards, _ := codec.Encode(data)

	storeDir := t.TempDir()
	hashHex := testHashHex(data)
	WriteShards(storeDir, hashHex, shards)

	// Verify shards exist.
	indices, _ := ListLocalShardIndices(storeDir, hashHex)
	if len(indices) != 6 {
		t.Fatalf("expected 6 shards before delete, got %d", len(indices))
	}

	// Delete.
	if err := DeleteShards(storeDir, hashHex); err != nil {
		t.Fatal(err)
	}

	// Verify gone.
	indices, _ = ListLocalShardIndices(storeDir, hashHex)
	if len(indices) != 0 {
		t.Fatalf("expected 0 shards after delete, got %d", len(indices))
	}
}

func testHash(data []byte) [32]byte {
	hasher := blake3.New()
	hasher.Write(data)
	var h [32]byte
	hasher.Sum(h[:0])
	return h
}

func testHashHex(data []byte) string {
	h := testHash(data)
	return hex.EncodeToString(h[:])
}

