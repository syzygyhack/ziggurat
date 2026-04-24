//go:build !windows

package worker

import (
	"os/exec"
	"syscall"
)

// SetProcessGroup configures the command to run in its own process group.
func SetProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// SendTermSignal sends SIGTERM to the process group for graceful shutdown.
func SendTermSignal(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		cmd.Process.Signal(syscall.SIGTERM)
	} else {
		syscall.Kill(-pgid, syscall.SIGTERM)
	}
}

// KillProcess sends SIGKILL to the process group for immediate termination.
func KillProcess(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		cmd.Process.Kill()
	} else {
		syscall.Kill(-pgid, syscall.SIGKILL)
	}
}
