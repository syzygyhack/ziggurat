package node

import (
	"fmt"
	"os"
	"path/filepath"
)

// singleInstanceLockPath is the machine-wide path guarding against more than one
// ziggurat node per OS instance. It lives under the OS temp dir, which is a
// distinct filesystem between Windows and WSL and between separate machines — so
// running a node on each of those remains a valid local multi-node dev setup.
func singleInstanceLockPath() string {
	return filepath.Join(os.TempDir(), "ziggurat.lock")
}

// errAnotherNodeRunning is returned when the single-instance lock is already held.
func errAnotherNodeRunning(path string) error {
	return fmt.Errorf("another ziggurat node is already running on this machine "+
		"(lock %s is held); only one node per machine is supported — for local "+
		"multi-node development, use Windows+WSL or separate machines", path)
}

// AcquireSingleInstance takes a machine-wide exclusive lock so that only one
// ziggurat node runs per OS instance. It returns a release function, or an
// error if another node already holds the lock. The lock is bound to the
// process and released automatically if it exits or crashes.
func AcquireSingleInstance() (release func(), err error) {
	return acquireSingleInstanceAt(singleInstanceLockPath())
}
