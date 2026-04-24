//go:build windows

package node

import (
	"syscall"
	"unsafe"
)

var (
	kernel32              = syscall.NewLazyDLL("kernel32.dll")
	procGetDiskFreeSpaceEx = kernel32.NewProc("GetDiskFreeSpaceExW")
)

// memoryStatusEx matches MEMORYSTATUSEX struct.
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

// detectTotalMemory returns total physical memory in bytes on Windows.
func detectTotalMemory() int64 {
	procGlobalMemoryStatusEx := kernel32.NewProc("GlobalMemoryStatusEx")
	var ms memoryStatusEx
	ms.Length = uint32(unsafe.Sizeof(ms))
	ret, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&ms)))
	if ret == 0 {
		return 0
	}
	return int64(ms.TotalPhys)
}

// detectDiskAvail returns available disk space in bytes for the given path.
func detectDiskAvail(path string) int64 {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0
	}
	var freeBytesAvailable uint64
	ret, _, _ := procGetDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		0,
		0,
	)
	if ret == 0 {
		return 0
	}
	return int64(freeBytesAvailable)
}
