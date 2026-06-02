//go:build windows

package node

import (
	"syscall"
)

// acquireSingleInstanceAt opens the lock file with no sharing and
// delete-on-close: a second process's CreateFile fails with a sharing
// violation, and the lock file is removed when this process's handle closes
// (including on crash, since Windows closes handles on termination).
func acquireSingleInstanceAt(path string) (func(), error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	const (
		fileAttributeTemporary = 0x00000100
		fileFlagDeleteOnClose  = 0x04000000
	)
	h, err := syscall.CreateFile(p,
		syscall.GENERIC_WRITE,
		0, // no sharing — second open fails with ERROR_SHARING_VIOLATION
		nil,
		syscall.CREATE_ALWAYS,
		fileAttributeTemporary|fileFlagDeleteOnClose,
		0)
	if err != nil {
		return nil, errAnotherNodeRunning(path)
	}
	return func() { syscall.CloseHandle(h) }, nil
}
