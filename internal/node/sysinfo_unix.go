//go:build !windows

package node

import (
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
