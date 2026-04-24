//go:build !windows

package cmd

import (
	"os"
	"syscall"
)

// shutdownSignals returns signals that trigger a graceful node shutdown.
// On Unix, both SIGINT (Ctrl+C) and SIGTERM (kill, systemd, docker) apply.
func shutdownSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}
