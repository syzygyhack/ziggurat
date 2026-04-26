package node

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// DetectCapabilities probes the local system and returns a map of
// auto-detected capability key-value pairs. dataDir is used to check
// available disk space; pass empty string to skip disk detection.
func DetectCapabilities(dataDir string) map[string]string {
	caps := make(map[string]string)

	// Always available.
	caps["os"] = runtime.GOOS
	caps["arch"] = runtime.GOARCH
	caps["cpu.cores"] = strconv.Itoa(runtime.NumCPU())

	if h, err := os.Hostname(); err == nil {
		caps["hostname"] = h
	}

	// Memory (platform-specific, best-effort).
	if mem := detectTotalMemory(); mem > 0 {
		caps["mem.total"] = strconv.FormatInt(mem, 10)
	}

	// Disk (platform-specific, best-effort).
	if dataDir != "" {
		if avail := detectDiskAvail(dataDir); avail > 0 {
			caps["disk.avail"] = strconv.FormatInt(avail, 10)
		}
	}

	// Storage class detection (best-effort).
	if dataDir != "" {
		if sc := detectStorageClass(dataDir); sc != "" {
			caps["storage.class"] = sc
		}
	}

	// GPU detection via nvidia-smi (best-effort).
	detectNvidiaGPU(caps)

	return caps
}

// MergeCapabilities merges auto-detected and user-configured capabilities.
// Configured values override auto-detected ones for the same key.
func MergeCapabilities(detected, configured map[string]string) map[string]string {
	merged := make(map[string]string, len(detected)+len(configured))
	for k, v := range detected {
		merged[k] = v
	}
	for k, v := range configured {
		merged[k] = v
	}
	return merged
}

// detectNvidiaGPU probes nvidia-smi for GPU information.
func detectNvidiaGPU(caps map[string]string) {
	smi, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return // No nvidia-smi, skip GPU detection.
	}

	// Query: count, model, total memory, driver version.
	out, err := exec.Command(smi,
		"--query-gpu=count,name,memory.total,driver_version",
		"--format=csv,noheader,nounits",
	).Output()
	if err != nil {
		return
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 0 {
		return
	}

	// Parse first line for count and driver (same across GPUs).
	// Format: "count, name, memory.total [MiB], driver_version"
	// nvidia-smi repeats "count" on every line; it's the total GPU count.
	var models []string
	var totalVRAM int64
	gpuCount := 0

	for _, line := range lines {
		fields := strings.SplitN(line, ", ", 4)
		if len(fields) < 4 {
			continue
		}
		gpuCount++
		models = append(models, strings.TrimSpace(fields[1]))

		// memory.total is in MiB.
		if mib, err := strconv.ParseInt(strings.TrimSpace(fields[2]), 10, 64); err == nil {
			totalVRAM += mib * 1024 * 1024 // MiB to bytes
		}

		// Driver is the same for all GPUs; take the last.
		caps["gpu.driver"] = strings.TrimSpace(fields[3])
	}

	if gpuCount > 0 {
		caps["gpu.count"] = strconv.Itoa(gpuCount)
		caps["gpu.vram"] = strconv.FormatInt(totalVRAM, 10)

		// Deduplicate model names.
		unique := dedup(models)
		caps["gpu.model"] = strings.Join(unique, ", ")
	}

	// CUDA version from nvcc.
	if nvcc, err := exec.LookPath("nvcc"); err == nil {
		if out, err := exec.Command(nvcc, "--version").Output(); err == nil {
			if v := parseCUDAVersion(string(out)); v != "" {
				caps["gpu.cuda"] = v
			}
		}
	}
}

// parseCUDAVersion extracts the version from nvcc --version output.
// Example line: "Cuda compilation tools, release 12.4, V12.4.131"
func parseCUDAVersion(output string) string {
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "release") {
			// Find "release X.Y"
			idx := strings.Index(line, "release ")
			if idx < 0 {
				continue
			}
			rest := line[idx+len("release "):]
			if comma := strings.Index(rest, ","); comma > 0 {
				return strings.TrimSpace(rest[:comma])
			}
			return strings.TrimSpace(rest)
		}
	}
	return ""
}

func dedup(ss []string) []string {
	seen := make(map[string]bool, len(ss))
	var result []string
	for _, s := range ss {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	return result
}

// RefreshDiskAvail updates the disk.avail capability in-place.
func RefreshDiskAvail(caps map[string]string, dataDir string) {
	if dataDir == "" {
		return
	}
	if avail := detectDiskAvail(dataDir); avail > 0 {
		caps["disk.avail"] = fmt.Sprintf("%d", avail)
	}
}
