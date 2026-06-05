package store

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/zeebo/blake3"
)

// hashBytes returns the BLAKE3-256 digest of b.
func hashBytes(b []byte) [32]byte {
	h := blake3.New()
	h.Write(b)
	var out [32]byte
	h.Sum(out[:0])
	return out
}

// ValidateHashHex checks that a hash string is exactly 64 hex characters
// (a valid hex-encoded BLAKE3 digest). Case is normalised before validation.
// This must be called on any hash received from an untrusted source before it
// is used in file paths, to prevent directory-traversal attacks.
func ValidateHashHex(hashHex string) error {
	if len(hashHex) != 64 {
		return fmt.Errorf("invalid hash length %d, expected 64", len(hashHex))
	}
	// Validate without allocation: lowercase hex characters intersect with
	// the uppercase range only for 'A'-'F' → 'a'-'f'.
	for _, c := range hashHex {
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			continue
		}
		return fmt.Errorf("invalid hex character %q", c)
	}
	return nil
}

// NormalizeHashHex converts a hex hash to lowercase. The caller must ensure
// the input has already passed ValidateHashHex.
func NormalizeHashHex(hashHex string) string {
	// Fast path: if already lowercase, return as-is to avoid allocation.
	hasUpper := false
	for _, c := range hashHex {
		if c >= 'A' && c <= 'F' {
			hasUpper = true
			break
		}
	}
	if !hasUpper {
		return hashHex
	}
	b := []byte(hashHex)
	for i, c := range b {
		if c >= 'A' && c <= 'F' {
			b[i] = c + 32 // 'a' - 'A'
		}
	}
	return string(b)
}

// WriteBlob writes data from r to disk under a content-addressed path.
// Returns the BLAKE3 hash, total bytes written, and whether a new file was
// created. When created is false the blob already existed on disk
// (deduplication) and the caller must NOT delete it on failure — doing so
// would destroy a valid blob that other metadata references.
func WriteBlob(storeDir string, r io.Reader) (hash [32]byte, size int64, created bool, err error) {
	// Write to a temp file while computing the hash.
	tmp, err := os.CreateTemp(storeDir, ".blob-*")
	if err != nil {
		return [32]byte{}, 0, false, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		// Clean up temp file on failure.
		if tmpPath != "" {
			os.Remove(tmpPath)
		}
	}()

	hasher := blake3.New()
	w := io.MultiWriter(tmp, hasher)

	n, err := io.Copy(w, r)
	if err != nil {
		tmp.Close()
		return [32]byte{}, 0, false, fmt.Errorf("write blob: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return [32]byte{}, 0, false, fmt.Errorf("close temp file: %w", err)
	}

	hasher.Sum(hash[:0])

	// Move to final content-addressed path.
	dest := blobPath(storeDir, hex.EncodeToString(hash[:]))
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return [32]byte{}, 0, false, fmt.Errorf("create shard dir: %w", err)
	}

	// If the file already exists (deduplication), just remove the temp.
	if _, err := os.Stat(dest); err == nil {
		os.Remove(tmpPath)
		tmpPath = "" // prevent deferred cleanup
		return hash, n, false, nil
	}

	if err := os.Rename(tmpPath, dest); err != nil {
		return [32]byte{}, 0, false, fmt.Errorf("rename blob: %w", err)
	}
	tmpPath = "" // rename succeeded, don't clean up

	return hash, n, true, nil
}

// ReadBlob opens a blob by hash for reading, verifying integrity.
func ReadBlob(storeDir string, hashHex string) (io.ReadCloser, error) {
	path := blobPath(storeDir, hashHex)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open blob %s: %w", hashHex[:12], err)
	}
	return &verifyingReader{f: f, hasher: blake3.New(), expected: hashHex}, nil
}

// DeleteBlob removes a blob from disk.
func DeleteBlob(storeDir string, hashHex string) error {
	path := blobPath(storeDir, hashHex)
	return os.Remove(path)
}

// blobPath returns the disk path for a blob: <storeDir>/<2-char prefix>/<full hash>.
func blobPath(storeDir, hashHex string) string {
	return filepath.Join(storeDir, hashHex[:2], hashHex)
}

// verifyingReader wraps a file reader and checks the BLAKE3 hash on Close.
type verifyingReader struct {
	f        *os.File
	hasher   *blake3.Hasher
	expected string
}

func (vr *verifyingReader) Read(p []byte) (int, error) {
	n, err := vr.f.Read(p)
	if n > 0 {
		vr.hasher.Write(p[:n])
	}
	return n, err
}

func (vr *verifyingReader) Close() error {
	defer vr.f.Close()

	// Read any remaining data to complete hash.
	io.Copy(io.Discard, vr)

	var hash [32]byte
	vr.hasher.Sum(hash[:0])
	actual := hex.EncodeToString(hash[:])

	if actual != vr.expected {
		return fmt.Errorf("integrity check failed: expected %s, got %s", vr.expected[:12], actual[:12])
	}
	return nil
}
