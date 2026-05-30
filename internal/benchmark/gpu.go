package benchmark

import (
	"os/exec"
	"strconv"
	"strings"

	"github.com/syzygyhack/ziggurat/internal/util"
)

// GPUInfo describes detected GPU hardware.
type GPUInfo struct {
	Count   int      `json:"count"`
	Model   string   `json:"model,omitempty"`   // comma-separated if heterogeneous
	VRAMMiB int64    `json:"vram_mib,omitempty"` // total across all GPUs
	Driver  string   `json:"driver,omitempty"`
	CUDA    string   `json:"cuda,omitempty"`
	Devices []GPUDev `json:"devices,omitempty"` // per-device detail
}

// GPUDev describes a single GPU device.
type GPUDev struct {
	Index      int    `json:"index"`
	Name       string `json:"name"`
	VRAMMiB    int64  `json:"vram_mib"`
	TempC      int    `json:"temp_c,omitempty"`       // current temperature
	PowerW     int    `json:"power_w,omitempty"`      // current power draw
	PowerCapW  int    `json:"power_cap_w,omitempty"`  // power limit
	Utilization int   `json:"utilization,omitempty"`  // GPU core utilization %
}

// DetectGPU probes nvidia-smi for GPU information. Returns nil if no
// NVIDIA GPU or nvidia-smi is not found. This is best-effort; parse
// failures for individual fields are silently ignored.
func DetectGPU() *GPUInfo {
	smi, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return nil
	}

	// Query per-device: name, memory, driver, temperature, power, utilization.
	out, err := exec.Command(smi,
		"--query-gpu=index,name,memory.total,driver_version,temperature.gpu,power.draw,power.limit,utilization.gpu",
		"--format=csv,noheader,nounits",
	).Output()
	if err != nil {
		return nil
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 0 {
		return nil
	}

	info := &GPUInfo{}

	for _, line := range lines {
		fields := splitCSV(line)
		if len(fields) < 8 {
			continue
		}

		dev := GPUDev{}
		dev.Index, _ = strconv.Atoi(fields[0])
		dev.Name = fields[1]
		dev.VRAMMiB, _ = strconv.ParseInt(fields[2], 10, 64)

		info.Driver = fields[3]

		dev.TempC, _ = strconv.Atoi(fields[4])
		// power.draw and power.limit may have decimals like "65.50"
		dev.PowerW = parseIntOrFloat(fields[5])
		dev.PowerCapW = parseIntOrFloat(fields[6])
		dev.Utilization, _ = strconv.Atoi(fields[7])

		info.Devices = append(info.Devices, dev)
		info.VRAMMiB += dev.VRAMMiB
		info.Count++
	}

	if info.Count == 0 {
		return nil
	}

	// Build model string from unique names.
	var models []string
	added := map[string]bool{}
	for _, d := range info.Devices {
		if !added[d.Name] {
			added[d.Name] = true
			models = append(models, d.Name)
		}
	}
	info.Model = strings.Join(models, ", ")

	// CUDA version from nvcc if available.
	if nvcc, err := exec.LookPath("nvcc"); err == nil {
		if out, err := exec.Command(nvcc, "--version").Output(); err == nil {
			info.CUDA = util.ParseCUDAVersion(string(out))
		}
	}

	return info
}

// splitCSV splits a comma-separated line and trims whitespace from each field.
func splitCSV(line string) []string {
	parts := strings.Split(line, ", ")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// parseIntOrFloat parses a string like "65" or "65.50" into an int.
func parseIntOrFloat(s string) int {
	s = strings.TrimSpace(s)
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int(f)
	}
	return 0
}

