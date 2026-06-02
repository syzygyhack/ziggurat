//go:build linux

package node

import (
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// detectCPUCores returns the number of CPU cores available to this node. When
// running under a cgroup CPU quota (containers, k8s), the quota is honored so
// the node doesn't advertise more cores than it can actually use; otherwise it
// falls back to the host's logical CPU count.
func detectCPUCores() int {
	phys := runtime.NumCPU()
	if q := cgroupCPUQuota(); q > 0 && q < phys {
		return q
	}
	return phys
}

// cgroupCPUQuota returns the integer CPU limit (rounded up) from the cgroup
// (v2 then v1), or 0 if unlimited or undetectable.
func cgroupCPUQuota() int {
	if data, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil { // cgroup v2
		return parseCgroupV2CPUMax(string(data))
	}
	q, e1 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") // cgroup v1
	p, e2 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if e1 == nil && e2 == nil {
		return parseCgroupV1CPU(string(q), string(p))
	}
	return 0
}

// parseCgroupV2CPUMax parses "cpu.max" contents ("<quota> <period>" or "max").
func parseCgroupV2CPUMax(s string) int {
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) != 2 || fields[0] == "max" {
		return 0
	}
	return cpuQuotaToCores(fields[0], fields[1])
}

// parseCgroupV1CPU parses cfs quota/period values; quota <= 0 means unlimited.
func parseCgroupV1CPU(quota, period string) int {
	return cpuQuotaToCores(strings.TrimSpace(quota), strings.TrimSpace(period))
}

func cpuQuotaToCores(quotaStr, periodStr string) int {
	quota, e1 := strconv.ParseInt(quotaStr, 10, 64)
	period, e2 := strconv.ParseInt(periodStr, 10, 64)
	if e1 != nil || e2 != nil || quota <= 0 || period <= 0 {
		return 0
	}
	cores := (quota + period - 1) / period // round up
	if cores < 1 {
		cores = 1
	}
	return int(cores)
}

// detectTotalMemory returns total memory in bytes available to this node,
// capped by any cgroup memory limit so containerized nodes don't over-report.
func detectTotalMemory() int64 {
	var phys int64
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err == nil {
		phys = int64(info.Totalram) * int64(info.Unit)
	}
	if lim := cgroupMemLimit(); lim > 0 && (phys == 0 || lim < phys) {
		return lim
	}
	return phys
}

// cgroupMemLimit returns the cgroup memory limit in bytes (v2 then v1), or 0 if
// unlimited or undetectable.
func cgroupMemLimit() int64 {
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil { // v2
		return parseCgroupMemLimit(string(data))
	}
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil { // v1
		return parseCgroupMemLimit(string(data))
	}
	return 0
}

// parseCgroupMemLimit parses a memory limit value. "max" and the v1 unlimited
// sentinel (a value near int64/page-size max) are treated as unlimited (0).
func parseCgroupMemLimit(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "max" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 || v >= int64(math.MaxInt64)/2 {
		return 0
	}
	return v
}

// detectDiskAvail returns available disk space in bytes for the given path.
func detectDiskAvail(path string) int64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}
	return int64(stat.Bavail) * int64(stat.Bsize)
}

// detectStorageClass probes the block device type for the given path.
// Reads /sys/block/<dev>/queue/rotational: "0" = SSD/NVMe, "1" = HDD.
// Falls back to "ssd" if detection fails.
func detectStorageClass(path string) string {
	// Resolve the device from the mount.
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return "ssd"
	}

	major := (stat.Dev >> 8) & 0xff
	// Try to find the block device name in /sys/dev/block/<major>:<minor>.
	minor := stat.Dev & 0xff
	devLink := filepath.Join("/sys/dev/block",
		strconv.Itoa(int(major))+":"+strconv.Itoa(int(minor)))
	resolved, err := os.Readlink(devLink)
	if err != nil {
		return "ssd"
	}

	// Walk up to find the parent device (e.g., sda from sda1).
	parts := strings.Split(resolved, "/")
	devName := ""
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] == "block" && i+1 < len(parts) {
			devName = parts[i+1]
			break
		}
	}
	if devName == "" {
		return "ssd"
	}
	// Strip partition number (sda1 -> sda, nvme0n1p1 -> nvme0n1).
	if strings.HasPrefix(devName, "nvme") {
		return "nvme"
	}

	rotational := filepath.Join("/sys/block", devName, "queue", "rotational")
	data, err := os.ReadFile(rotational)
	if err != nil {
		return "ssd"
	}
	if strings.TrimSpace(string(data)) == "1" {
		return "hdd"
	}
	return "ssd"
}
