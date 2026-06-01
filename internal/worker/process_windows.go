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
// signal mechanism, so we terminate immediately. The exec flow will follow
// up with KillProcess after the grace period as a safety net.
func SendTermSignal(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	cmd.Process.Kill()
}

// KillProcess forcefully terminates the process via TerminateProcess.
func KillProcess(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	cmd.Process.Kill()
}
