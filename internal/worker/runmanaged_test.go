package worker

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

// TestRunManaged covers the shared start/wait/cancel/grace/kill skeleton used by
// both the host and container execution paths.
func TestRunManaged(t *testing.T) {
	if _, err := exec.LookPath("sleep"); err != nil {
		t.Skip("sleep not available")
	}

	t.Run("normal exit", func(t *testing.T) {
		cmd := exec.Command("sleep", "0.05")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cancelled, killed := false, false
		ec, forced, err := runManaged(context.Background(), cmd, time.Second,
			func() { cancelled = true }, func() { killed = true })
		if err != nil || ec != 0 || forced || cancelled || killed {
			t.Fatalf("ec=%d forced=%v cancelled=%v killed=%v err=%v", ec, forced, cancelled, killed, err)
		}
	})

	t.Run("cancel within grace", func(t *testing.T) {
		cmd := exec.Command("sleep", "30")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // already cancelled
		killed := false
		_, forced, _ := runManaged(ctx, cmd, 2*time.Second,
			func() { _ = cmd.Process.Kill() }, // onCancel stops it promptly
			func() { killed = true })
		if !forced {
			t.Error("expected forced=true")
		}
		if killed {
			t.Error("onKill should not fire when onCancel stops the process within grace")
		}
	})

	t.Run("kill after grace", func(t *testing.T) {
		cmd := exec.Command("sleep", "30")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		killed := false
		_, forced, _ := runManaged(ctx, cmd, 100*time.Millisecond,
			func() {}, // onCancel does nothing, so the grace timer elapses
			func() { killed = true; _ = cmd.Process.Kill() })
		if !forced {
			t.Error("expected forced=true")
		}
		if !killed {
			t.Error("expected onKill to fire after the grace period")
		}
	})
}
