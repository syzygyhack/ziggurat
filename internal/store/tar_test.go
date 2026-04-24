package store

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateDeterministicTar_RoundTrip(t *testing.T) {
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("aaa"), 0o644)
	os.WriteFile(filepath.Join(src, "b.txt"), []byte("bbb"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "c.txt"), []byte("ccc"), 0o644)

	var buf bytes.Buffer
	if err := CreateDeterministicTar(src, &buf); err != nil {
		t.Fatal(err)
	}

	dest := t.TempDir()
	if err := ExtractTar(bytes.NewReader(buf.Bytes()), dest); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		path    string
		content string
	}{
		{"a.txt", "aaa"},
		{"b.txt", "bbb"},
		{filepath.Join("sub", "c.txt"), "ccc"},
	} {
		data, err := os.ReadFile(filepath.Join(dest, tc.path))
		if err != nil {
			t.Fatalf("missing file %s: %v", tc.path, err)
		}
		if string(data) != tc.content {
			t.Fatalf("%s: got %q, want %q", tc.path, data, tc.content)
		}
	}
}

func TestCreateDeterministicTar_Determinism(t *testing.T) {
	mkDir := func() string {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "z.txt"), []byte("last"), 0o644)
		os.WriteFile(filepath.Join(dir, "a.txt"), []byte("first"), 0o644)
		os.MkdirAll(filepath.Join(dir, "mid"), 0o755)
		os.WriteFile(filepath.Join(dir, "mid", "m.txt"), []byte("middle"), 0o644)
		return dir
	}

	var buf1, buf2 bytes.Buffer
	if err := CreateDeterministicTar(mkDir(), &buf1); err != nil {
		t.Fatal(err)
	}
	if err := CreateDeterministicTar(mkDir(), &buf2); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Fatal("identical directories produced different tar archives")
	}
}

func TestCreateDeterministicTar_EmptyDir(t *testing.T) {
	src := t.TempDir()

	var buf bytes.Buffer
	if err := CreateDeterministicTar(src, &buf); err != nil {
		t.Fatal(err)
	}

	if buf.Len() == 0 {
		t.Fatal("empty directory should still produce a valid tar")
	}

	dest := t.TempDir()
	if err := ExtractTar(bytes.NewReader(buf.Bytes()), dest); err != nil {
		t.Fatal(err)
	}
}

func TestExtractTar_PathTraversal(t *testing.T) {
	// Build a tar archive with a "../escape.txt" entry.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := &tar.Header{
		Name: "../escape.txt",
		Mode: 0o644,
		Size: 4,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	tw.Write([]byte("evil"))
	tw.Close()

	dest := t.TempDir()
	err := ExtractTar(bytes.NewReader(buf.Bytes()), dest)
	if err == nil {
		t.Fatal("expected error for path traversal, got nil")
	}

	// Ensure no file was written outside dest.
	if _, statErr := os.Stat(filepath.Join(dest, "..", "escape.txt")); statErr == nil {
		t.Fatal("path traversal: file written outside dest")
	}
}
