//go:build windows

package worker

import (
	"os/exec"
	"syscall"
)

// SetProcessGroup configures the command to run in a new process group.
// On Windows, this uses CREATE_NEW_PROCESS_GROUP.
func SetProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}
}

// SendTermSignal attempts graceful termination. Windows has no universal
// SIGTERM equivalent, so this is best-effort. The exec flow will follow
// up with KillProcess after the grace period if the process doesn't exit.
func SendTermSignal(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	// GenerateConsoleCtrlEvent could be used here for console apps,
	// but it requires the process to share a console. For broad
	// compatibility, we skip to let the grace timer expire and
	// KillProcess handle it.
}

// KillProcess forcefully terminates the process via TerminateProcess.
func KillProcess(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	cmd.Process.Kill()
}
