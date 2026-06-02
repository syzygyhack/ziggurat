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

func TestParseRuntimeVersion(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want string
	}{
		{"python", "Python 3.12.1\n", "3.12.1"},
		{"python2 stderr", "Python 2.7.18\n", "2.7.18"},
		{"node", "v20.11.0\n", "20.11.0"},
		{"go", "go version go1.24.0 linux/amd64\n", "1.24.0"},
		{"java stderr", "openjdk version \"17.0.2\" 2022-01-18\n", "17.0.2"},
		{"ruby", "ruby 3.2.2 (2023-03-30 revision e51014f9c0) [x86_64-linux]", "3.2.2"},
		{"rust", "rustc 1.75.0 (82e1608df 2023-12-21)", "1.75.0"},
		{"two-segment", "tool 1.5", "1.5"},
		{"prerelease suffix", "Python 3.13.0rc1", "3.13.0"},
		{"none", "no version available", ""},
		{"empty", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseRuntimeVersion(c.out); got != c.want {
				t.Errorf("parseRuntimeVersion(%q) = %q, want %q", c.out, got, c.want)
			}
		})
	}
}

func TestDetectRuntimes_FindsGo(t *testing.T) {
	// The Go toolchain is always on PATH during `go test`, so go.version must
	// be detected and look like a dotted version.
	caps := make(map[string]string)
	detectRuntimes(caps)

	v, ok := caps["go.version"]
	if !ok {
		t.Fatal("expected go.version to be detected")
	}
	if !versionPattern.MatchString(v) {
		t.Errorf("go.version = %q, not a dotted version", v)
	}
}
