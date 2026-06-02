//go:build !windows

package node

import (
	"fmt"
	"os"
	"syscall"
)

// acquireSingleInstanceAt takes an exclusive, non-blocking advisory lock (flock)
// on the given path. The lock is tied to the open file descriptor, so the OS
// releases it automatically when the process exits or crashes — no stale-lock
// recovery needed.
func acquireSingleInstanceAt(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open single-instance lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errAnotherNodeRunning(path)
	}
	// Record our PID for diagnostics (best-effort).
	_ = f.Truncate(0)
	_, _ = f.WriteString(fmt.Sprintf("%d\n", os.Getpid()))
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		_ = os.Remove(path)
	}, nil
}
