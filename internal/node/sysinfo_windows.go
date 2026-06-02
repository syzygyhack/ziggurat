//go:build windows

package node

import (
	"path/filepath"
	"runtime"
	"syscall"
	"unsafe"
)

// detectCPUCores returns the logical CPU count. Windows has no cgroup-style
// quota mechanism, so this is simply the host's logical CPU count.
func detectCPUCores() int {
	return runtime.NumCPU()
}

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

// detectStorageClass returns the storage type for the volume holding path.
// It queries the volume's seek-penalty descriptor (no admin needed for a
// query-only handle): a device that incurs a seek penalty is rotational
// ("hdd"), otherwise it's flash ("ssd"). Falls back to "ssd" on any error
// (volume undeterminable, query unsupported, etc.).
func detectStorageClass(path string) string {
	vol := filepath.VolumeName(path) // e.g. "C:"
	if vol == "" {
		return "ssd"
	}
	devPtr, err := syscall.UTF16PtrFromString(`\\.\` + vol)
	if err != nil {
		return "ssd"
	}
	// Query-only handle (0 access) avoids requiring administrator rights.
	h, err := syscall.CreateFile(devPtr, 0,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE, nil,
		syscall.OPEN_EXISTING, 0, 0)
	if err != nil {
		return "ssd"
	}
	defer syscall.CloseHandle(h)

	const (
		ioctlStorageQueryProperty        = 0x002D1400
		storageDeviceSeekPenaltyProperty = 7 // StorageDeviceSeekPenaltyProperty
		propertyStandardQuery            = 0 // PropertyStandardQuery
	)
	// STORAGE_PROPERTY_QUERY{ PropertyId, QueryType DWORD; AdditionalParameters BYTE[1] }
	query := struct {
		PropertyID uint32
		QueryType  uint32
		Extra      [1]byte
	}{PropertyID: storageDeviceSeekPenaltyProperty, QueryType: propertyStandardQuery}
	// DEVICE_SEEK_PENALTY_DESCRIPTOR{ Version, Size DWORD; IncursSeekPenalty BOOLEAN }
	var desc struct {
		Version           uint32
		Size              uint32
		IncursSeekPenalty byte
		_                 [3]byte // pad to 4-byte boundary
	}
	var returned uint32
	err = syscall.DeviceIoControl(h, ioctlStorageQueryProperty,
		(*byte)(unsafe.Pointer(&query)), uint32(unsafe.Sizeof(query)),
		(*byte)(unsafe.Pointer(&desc)), uint32(unsafe.Sizeof(desc)),
		&returned, nil)
	if err != nil {
		return "ssd"
	}
	if desc.IncursSeekPenalty != 0 {
		return "hdd"
	}
	return "ssd"
}
