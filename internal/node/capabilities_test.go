package node

import (
	"runtime"
	"strconv"
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
		"cpu.cores":      "8",    // override
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

func TestParseNvidiaCSV(t *testing.T) {
	out := "NVIDIA GeForce RTX 4090 D, 49152, 591.86\nNVIDIA GeForce RTX 4090 D, 49152, 591.86\n"
	devices, driver := parseNvidiaCSV(out)
	if len(devices) != 2 {
		t.Fatalf("expected 2 devices, got %d", len(devices))
	}
	if driver != "591.86" {
		t.Errorf("driver = %q, want 591.86", driver)
	}
	if devices[0].vendor != "nvidia" || devices[0].model != "NVIDIA GeForce RTX 4090 D" {
		t.Errorf("device0 = %+v", devices[0])
	}
	if devices[0].vram != 49152*1024*1024 {
		t.Errorf("vram = %d, want %d", devices[0].vram, 49152*1024*1024)
	}
	if d, _ := parseNvidiaCSV(""); d != nil {
		t.Error("empty output should yield no devices")
	}
}

func TestAggregateGPU(t *testing.T) {
	caps := map[string]string{}
	aggregateGPU(caps, []gpuDevice{
		{vendor: "nvidia", model: "RTX 4090", vram: 24 << 30},
		{vendor: "nvidia", model: "RTX 3080", vram: 10 << 30},
	})
	if caps["gpu.count"] != "2" {
		t.Errorf("gpu.count = %q, want 2", caps["gpu.count"])
	}
	// gpu.vram is the sum; gpu.vram.max is the largest single device.
	if caps["gpu.vram"] != strconv.FormatInt(34<<30, 10) {
		t.Errorf("gpu.vram = %q, want %d", caps["gpu.vram"], int64(34)<<30)
	}
	if caps["gpu.vram.max"] != strconv.FormatInt(24<<30, 10) {
		t.Errorf("gpu.vram.max = %q, want %d", caps["gpu.vram.max"], int64(24)<<30)
	}
	if caps["gpu.0.model"] != "RTX 4090" || caps["gpu.1.model"] != "RTX 3080" {
		t.Errorf("per-device models wrong: %v", caps)
	}
	if caps["gpu.vendor"] != "nvidia" {
		t.Errorf("gpu.vendor = %q, want nvidia", caps["gpu.vendor"])
	}
}

func TestAggregateGPU_NoDevices(t *testing.T) {
	caps := map[string]string{}
	aggregateGPU(caps, nil)
	if len(caps) != 0 {
		t.Errorf("expected no caps for zero devices, got %v", caps)
	}
}

func TestParseROCmCSV(t *testing.T) {
	// device,Card series,VRAM Total Memory (B)
	out := "device,Card series,VRAM Total Memory (B)\ncard0,Instinct MI210,68702699520\n"
	devices := parseROCmCSV(out)
	if len(devices) != 1 {
		t.Fatalf("expected 1 AMD device, got %d", len(devices))
	}
	if devices[0].vendor != "amd" {
		t.Errorf("vendor = %q, want amd", devices[0].vendor)
	}
	if devices[0].vram != 68702699520 {
		t.Errorf("vram = %d, want 68702699520", devices[0].vram)
	}
}
