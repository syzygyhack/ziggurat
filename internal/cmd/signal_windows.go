//go:build windows

package cmd

import "os"

// shutdownSignals returns signals that trigger a graceful node shutdown.
// On Windows, only Ctrl+C (os.Interrupt) is reliably delivered.
func shutdownSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
