package util

import "testing"

func TestFileExists(t *testing.T) {
	// Test with a file that should never exist.
	if FileExists("/nonexistent/file/path/that/cannot/exist") {
		t.Error("FileExists on nonexistent path should be false")
	}
}

func TestFileExists_TempFile(t *testing.T) {
	// Create a temp file and verify it exists.
	f := t.TempDir()
	if !FileExists(f) {
		t.Errorf("FileExists(%q) should be true for existing directory", f)
	}
}
