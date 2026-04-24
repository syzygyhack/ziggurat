package node

import (
	"runtime"
	"testing"
)

func TestDetectCapabilities_BasicFields(t *testing.T) {
	caps := DetectCapabilities("")

	if caps["os"] != runtime.GOOS {
		t.Errorf("os = %q, want %q", caps["os"], runtime.GOOS)
	}
	if caps["arch"] != runtime.GOARCH {
		t.Errorf("arch = %q, want %q", caps["arch"], runtime.GOARCH)
	}
	if caps["cpu.cores"] == "" {
		t.Error("cpu.cores should be set")
	}
	if caps["hostname"] == "" {
		t.Error("hostname should be set")
	}
	if caps["mem.total"] == "" {
		t.Error("mem.total should be set")
	}
}

func TestMergeCapabilities(t *testing.T) {
	detected := map[string]string{
		"os":        "linux",
		"arch":      "amd64",
		"cpu.cores": "16",
	}
	configured := map[string]string{
		"cpu.cores":      "8",  // override
		"python.version": "3.12", // new
	}

	merged := MergeCapabilities(detected, configured)

	if merged["os"] != "linux" {
		t.Error("detected key should be preserved")
	}
	if merged["cpu.cores"] != "8" {
		t.Errorf("configured should override: got %q, want %q", merged["cpu.cores"], "8")
	}
	if merged["python.version"] != "3.12" {
		t.Error("configured key should be added")
	}
}

func TestMergeCapabilities_NilConfigured(t *testing.T) {
	detected := map[string]string{"os": "linux"}
	merged := MergeCapabilities(detected, nil)

	if merged["os"] != "linux" {
		t.Error("merge with nil configured should return detected")
	}
}
