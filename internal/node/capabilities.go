package node

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/syzygyhack/ziggurat/internal/util"
)

// DetectCapabilities probes the local system and returns a map of
// auto-detected capability key-value pairs. dataDir is used to check
// available disk space; pass empty string to skip disk detection.
func DetectCapabilities(dataDir string) map[string]string {
	caps := make(map[string]string)

	// Always available.
	caps["os"] = runtime.GOOS
	caps["arch"] = runtime.GOARCH
	caps["cpu.cores"] = strconv.Itoa(detectCPUCores())

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

	// Container runtime detection, so OCI image tasks route only to nodes that
	// can actually run them (rather than failing at execution).
	if rt := detectContainerRuntime(); rt != "" {
		caps["container.runtime"] = rt
	}

	// GPU detection across vendors (NVIDIA full; AMD/Intel best-effort).
	detectGPU(caps)

	// Language runtime detection (best-effort): python.version, node.version, etc.
	detectRuntimes(caps)

	return caps
}

// versionPattern extracts the first dot-separated numeric version (e.g. the
// "3.12.1" in "Python 3.12.1" or the "1.24.0" in "go version go1.24.0 ...").
var versionPattern = regexp.MustCompile(`\d+\.\d+(?:\.\d+)*`)

// parseRuntimeVersion pulls a dotted version string out of a tool's --version
// output. Returns "" if none is found.
func parseRuntimeVersion(output string) string {
	return versionPattern.FindString(output)
}

// runtimeProbe describes how to detect a language runtime and the capability
// key under which its version is advertised.
type runtimeProbe struct {
	capKey string   // e.g. "python.version"
	bins   []string // candidate executables, first found wins
	args   []string // version arguments
}

// runtimeProbes lists the language runtimes detected at startup. Versions are
// advertised as "<runtime>.version" capabilities so tasks can require them
// with, e.g., --constraint "python.version >= 3.10".
var runtimeProbes = []runtimeProbe{
	{"python.version", []string{"python3", "python"}, []string{"--version"}},
	{"node.version", []string{"node"}, []string{"--version"}},
	{"go.version", []string{"go"}, []string{"version"}},
	{"java.version", []string{"java"}, []string{"-version"}},
	{"ruby.version", []string{"ruby"}, []string{"--version"}},
	{"rust.version", []string{"rustc"}, []string{"--version"}},
}

// detectRuntimes probes for installed language runtimes concurrently and records
// each one's version as a "<runtime>.version" capability. Best-effort: a runtime
// that is absent, hangs, or prints no parseable version is simply skipped.
// Operator-configured node.capabilities still override these via MergeCapabilities.
func detectRuntimes(caps map[string]string) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, p := range runtimeProbes {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v := probeRuntimeVersion(p.bins, p.args); v != "" {
				mu.Lock()
				caps[p.capKey] = v
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}

// probeRuntimeVersion runs the first available binary's version command and
// returns the parsed version. Output is captured combined because some tools
// (notably `java -version`) print to stderr. A short timeout guards against a
// misbehaving tool stalling node startup.
func probeRuntimeVersion(bins, args []string) string {
	for _, bin := range bins {
		if _, err := exec.LookPath(bin); err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		out, _ := exec.CommandContext(ctx, bin, args...).CombinedOutput()
		cancel()
		if v := parseRuntimeVersion(string(out)); v != "" {
			return v
		}
	}
	return ""
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

// detectContainerRuntime returns the available OCI runtime ("podman" preferred,
// then "docker"), or "" if none is installed. Mirrors the worker's selection.
func detectContainerRuntime() string {
	for _, bin := range []string{"podman", "docker"} {
		if _, err := exec.LookPath(bin); err == nil {
			return bin
		}
	}
	return ""
}

// gpuDevice describes a single detected GPU.
type gpuDevice struct {
	vendor string // nvidia | amd | intel
	model  string
	vram   int64 // bytes; 0 if unknown
}

// gpuProbeCmd runs a GPU tool with a short timeout (these can hang on a wedged
// driver) and returns combined-free stdout.
func gpuProbeCmd(name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, name, args...).Output()
}

// detectGPU aggregates GPUs across vendors and records both aggregate caps
// (gpu.count, gpu.vram total, gpu.vram.max single-largest, gpu.model,
// gpu.vendor) and per-device caps (gpu.<i>.model, gpu.<i>.vram). NVIDIA is
// detected in full; AMD and Intel are best-effort (count + vendor, VRAM when
// the vendor tool reports it). Each detector no-ops if its tool is absent.
func detectGPU(caps map[string]string) {
	var devices []gpuDevice
	devices = append(devices, detectNvidiaDevices(caps)...)
	devices = append(devices, detectAMDDevices()...)
	devices = append(devices, detectIntelDevices()...)
	aggregateGPU(caps, devices)
}

// aggregateGPU writes aggregate and per-device GPU capabilities.
func aggregateGPU(caps map[string]string, devices []gpuDevice) {
	if len(devices) == 0 {
		return
	}
	var totalVRAM, maxVRAM int64
	var models, vendors []string
	for i, d := range devices {
		caps[fmt.Sprintf("gpu.%d.model", i)] = d.model
		if d.vram > 0 {
			caps[fmt.Sprintf("gpu.%d.vram", i)] = strconv.FormatInt(d.vram, 10)
		}
		totalVRAM += d.vram
		if d.vram > maxVRAM {
			maxVRAM = d.vram
		}
		models = append(models, d.model)
		vendors = append(vendors, d.vendor)
	}
	caps["gpu.count"] = strconv.Itoa(len(devices))
	caps["gpu.vram"] = strconv.FormatInt(totalVRAM, 10)
	if maxVRAM > 0 {
		// Largest single-device VRAM — use this (not the summed gpu.vram) to
		// require a GPU big enough for one job, e.g. gpu.vram.max >= 16GB.
		caps["gpu.vram.max"] = strconv.FormatInt(maxVRAM, 10)
	}
	caps["gpu.model"] = strings.Join(dedup(models), ", ")
	caps["gpu.vendor"] = strings.Join(dedup(vendors), ", ")
}

// detectNvidiaDevices probes nvidia-smi (per device) and also records driver
// and CUDA version capabilities.
func detectNvidiaDevices(caps map[string]string) []gpuDevice {
	smi, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return nil
	}
	out, err := gpuProbeCmd(smi,
		"--query-gpu=name,memory.total,driver_version",
		"--format=csv,noheader,nounits",
	)
	if err != nil {
		return nil
	}
	devices, driver := parseNvidiaCSV(string(out))
	if len(devices) == 0 {
		return nil
	}
	if driver != "" {
		caps["gpu.driver"] = driver
	}
	if nvcc, err := exec.LookPath("nvcc"); err == nil {
		if out, err := gpuProbeCmd(nvcc, "--version"); err == nil {
			if v := util.ParseCUDAVersion(string(out)); v != "" {
				caps["gpu.cuda"] = v
			}
		}
	}
	return devices
}

// parseNvidiaCSV parses "name, memory.total[MiB], driver_version" lines from
// nvidia-smi --format=csv,noheader,nounits into devices. Returns the devices
// and the (shared) driver version.
func parseNvidiaCSV(output string) (devices []gpuDevice, driver string) {
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.SplitN(line, ", ", 3)
		if len(fields) < 3 {
			continue
		}
		var vram int64
		if mib, err := strconv.ParseInt(strings.TrimSpace(fields[1]), 10, 64); err == nil {
			vram = mib * 1024 * 1024 // MiB -> bytes
		}
		devices = append(devices, gpuDevice{
			vendor: "nvidia",
			model:  strings.TrimSpace(fields[0]),
			vram:   vram,
		})
		driver = strings.TrimSpace(fields[2])
	}
	return devices, driver
}

// detectAMDDevices probes rocm-smi (best-effort; output format varies by ROCm
// version). Returns one device per reported card with VRAM when parseable.
func detectAMDDevices() []gpuDevice {
	smi, err := exec.LookPath("rocm-smi")
	if err != nil {
		return nil
	}
	out, err := gpuProbeCmd(smi, "--showproductname", "--showmeminfo", "vram", "--csv")
	if err != nil {
		return nil
	}
	return parseROCmCSV(string(out))
}

// parseROCmCSV parses rocm-smi --csv output. It is intentionally lenient: it
// treats each data row whose first column names a card (e.g. "card0") as a
// device, extracts the largest byte-valued column as VRAM, and joins remaining
// text columns as the model. Returns nil if nothing card-like is found.
func parseROCmCSV(output string) []gpuDevice {
	var devices []gpuDevice
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		line = strings.TrimSpace(line)
		cols := strings.Split(line, ",")
		if len(cols) < 2 || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(cols[0])), "card") {
			continue
		}
		var vram int64
		var modelParts []string
		for _, c := range cols[1:] {
			c = strings.TrimSpace(c)
			if c == "" {
				continue
			}
			if n, err := strconv.ParseInt(c, 10, 64); err == nil {
				if n > vram {
					vram = n // largest numeric column ~ VRAM bytes
				}
				continue
			}
			modelParts = append(modelParts, c)
		}
		model := "AMD GPU"
		if len(modelParts) > 0 {
			model = strings.Join(modelParts, " ")
		}
		devices = append(devices, gpuDevice{vendor: "amd", model: model, vram: vram})
	}
	return devices
}

// detectIntelDevices best-effort detects Intel data-center GPUs via xpu-smi.
// VRAM is not reliably available from discovery, so it is left unset.
func detectIntelDevices() []gpuDevice {
	smi, err := exec.LookPath("xpu-smi")
	if err != nil {
		return nil
	}
	out, err := gpuProbeCmd(smi, "discovery", "--dump", "1")
	if err != nil {
		return nil
	}
	var devices []gpuDevice
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.Contains(strings.ToLower(line), "device") && strings.Contains(line, "Intel") {
			devices = append(devices, gpuDevice{vendor: "intel", model: "Intel GPU"})
		}
	}
	return devices
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
