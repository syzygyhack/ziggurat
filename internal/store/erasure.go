package store

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/klauspost/reedsolomon"
	"github.com/zeebo/blake3"
)

// ErasureCodec encodes and decodes objects using Reed-Solomon erasure coding.
// Data is split into DataShards pieces; ParityShards parity pieces are computed.
// Any DataShards of the total (DataShards + ParityShards) suffice to reconstruct.
type ErasureCodec struct {
	enc          reedsolomon.Encoder
	DataShards   int
	ParityShards int
}

// NewErasureCodec creates a codec with the given shard counts.
func NewErasureCodec(dataShards, parityShards int) (*ErasureCodec, error) {
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, fmt.Errorf("create reed-solomon encoder: %w", err)
	}
	return &ErasureCodec{
		enc:          enc,
		DataShards:   dataShards,
		ParityShards: parityShards,
	}, nil
}

// TotalShards returns DataShards + ParityShards.
func (c *ErasureCodec) TotalShards() int {
	return c.DataShards + c.ParityShards
}

// Encode splits data into shards and computes parity. Returns all shards
// (data shards first, then parity shards). Each shard includes its own
// BLAKE3 hash for integrity verification.
func (c *ErasureCodec) Encode(data []byte) ([][]byte, error) {
	shards, err := c.enc.Split(data)
	if err != nil {
		return nil, fmt.Errorf("split data: %w", err)
	}
	if err := c.enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("encode parity: %w", err)
	}
	return shards, nil
}

// Decode reconstructs the original data from available shards. Missing shards
// should be nil. Returns the reconstructed data (trimmed to originalSize).
func (c *ErasureCodec) Decode(shards [][]byte, originalSize int64) ([]byte, error) {
	if err := c.enc.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("reconstruct: %w", err)
	}

	var buf bytes.Buffer
	if err := c.enc.Join(&buf, shards, int(originalSize)); err != nil {
		return nil, fmt.Errorf("join shards: %w", err)
	}
	return buf.Bytes(), nil
}

// Verify checks whether the parity shards are consistent with the data shards.
func (c *ErasureCodec) Verify(shards [][]byte) (bool, error) {
	return c.enc.Verify(shards)
}

// --- On-disk shard storage ---

// shardDir returns the directory for erasure-coded shards of a given object hash.
// Layout: <storeDir>/ec/<hash[:2]>/<hash>/
func shardDir(storeDir, hashHex string) string {
	return filepath.Join(storeDir, "ec", hashHex[:2], hashHex)
}

// shardPath returns the path for a specific shard index.
// Layout: <shardDir>/<index>.shard
func shardPath(storeDir, hashHex string, index int) string {
	return filepath.Join(shardDir(storeDir, hashHex), fmt.Sprintf("%d.shard", index))
}

// WriteShards writes all shards to disk under the erasure-coded directory layout.
// Returns per-shard BLAKE3 hashes for integrity tracking.
func WriteShards(storeDir, hashHex string, shards [][]byte) ([][32]byte, error) {
	dir := shardDir(storeDir, hashHex)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create shard dir: %w", err)
	}

	hashes := make([][32]byte, len(shards))
	for i, shard := range shards {
		hasher := blake3.New()
		hasher.Write(shard)
		hasher.Sum(hashes[i][:0])

		path := shardPath(storeDir, hashHex, i)
		if err := os.WriteFile(path, shard, 0o644); err != nil {
			return nil, fmt.Errorf("write shard %d: %w", i, err)
		}
	}
	return hashes, nil
}

// ReadShard reads a single shard from disk and verifies its integrity.
func ReadShard(storeDir, hashHex string, index int, expectedHash [32]byte) ([]byte, error) {
	path := shardPath(storeDir, hashHex, index)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read shard %d: %w", index, err)
	}

	var actual [32]byte
	hasher := blake3.New()
	hasher.Write(data)
	hasher.Sum(actual[:0])

	if actual != expectedHash {
		return nil, fmt.Errorf("shard %d integrity check failed: expected %s, got %s",
			index, hex.EncodeToString(expectedHash[:])[:12], hex.EncodeToString(actual[:])[:12])
	}
	return data, nil
}

// ReadLocalShards reads all locally available shards for an object.
// Returns a slice where missing/corrupt shards are nil.
func ReadLocalShards(storeDir, hashHex string, totalShards int, shardHashes [][32]byte) [][]byte {
	shards := make([][]byte, totalShards)
	for i := 0; i < totalShards; i++ {
		if i < len(shardHashes) {
			data, err := ReadShard(storeDir, hashHex, i, shardHashes[i])
			if err == nil {
				shards[i] = data
			}
		}
	}
	return shards
}

// DeleteShards removes all shard files and the shard directory for an object.
func DeleteShards(storeDir, hashHex string) error {
	return os.RemoveAll(shardDir(storeDir, hashHex))
}

// ListLocalShardIndices returns the shard indices present on disk for an object.
func ListLocalShardIndices(storeDir, hashHex string) ([]int, error) {
	dir := shardDir(storeDir, hashHex)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var indices []int
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".shard") {
			idx, err := strconv.Atoi(strings.TrimSuffix(name, ".shard"))
			if err == nil {
				indices = append(indices, idx)
			}
		}
	}
	sort.Ints(indices)
	return indices, nil
}

// WriteSingleShard writes a single shard to disk (used when receiving a shard
// from a remote node during replication).
func WriteSingleShard(storeDir, hashHex string, index int, data []byte) error {
	dir := shardDir(storeDir, hashHex)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create shard dir: %w", err)
	}
	path := shardPath(storeDir, hashHex, index)
	return os.WriteFile(path, data, 0o644)
}

// EncodeAndWrite is a convenience that reads all data, encodes with the codec,
// and writes shards to disk. Returns the content hash and per-shard hashes.
func EncodeAndWrite(storeDir string, r io.Reader, codec *ErasureCodec) ([32]byte, [][32]byte, int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return [32]byte{}, nil, 0, fmt.Errorf("read data: %w", err)
	}

	// Compute content hash over the full original data.
	var contentHash [32]byte
	hasher := blake3.New()
	hasher.Write(data)
	hasher.Sum(contentHash[:0])

	hashHex := hex.EncodeToString(contentHash[:])
	size := int64(len(data))

	shards, err := codec.Encode(data)
	if err != nil {
		return [32]byte{}, nil, 0, err
	}

	shardHashes, err := WriteShards(storeDir, hashHex, shards)
	if err != nil {
		return [32]byte{}, nil, 0, err
	}

	return contentHash, shardHashes, size, nil
}

// ReadAndDecode reads local shards and reconstructs the original data.
func ReadAndDecode(storeDir, hashHex string, originalSize int64, codec *ErasureCodec, shardHashes [][32]byte) ([]byte, error) {
	shards := ReadLocalShards(storeDir, hashHex, codec.TotalShards(), shardHashes)

	// Count available shards.
	available := 0
	for _, s := range shards {
		if s != nil {
			available++
		}
	}
	if available < codec.DataShards {
		return nil, fmt.Errorf("need %d shards but only %d available locally", codec.DataShards, available)
	}

	return codec.Decode(shards, originalSize)
}
