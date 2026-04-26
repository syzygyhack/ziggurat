//go:build !windows

package node

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// detectTotalMemory returns total physical memory in bytes on Unix systems.
func detectTotalMemory() int64 {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0
	}
	return int64(info.Totalram) * int64(info.Unit)
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

