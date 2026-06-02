//go:build darwin

package node

import (
	"runtime"

	"golang.org/x/sys/unix"
)

// detectCPUCores returns the logical CPU count. macOS has no cgroup-style quota
// mechanism, so this is simply the host's logical CPU count.
func detectCPUCores() int {
	return runtime.NumCPU()
}

// detectTotalMemory returns total physical memory in bytes via sysctl hw.memsize.
func detectTotalMemory() int64 {
	v, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0
	}
	return int64(v)
}

// detectDiskAvail returns available disk space in bytes for the given path.
func detectDiskAvail(path string) int64 {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

// detectStorageClass reports the storage type. macOS internal storage is flash;
// per-device probing isn't readily available without elevated APIs, so default
// to "ssd".
func detectStorageClass(path string) string {
	return "ssd"
}
