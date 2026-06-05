package worker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"
)

// detectRuntime finds a container runtime on the host. Podman is preferred
// (rootless, daemonless); Docker is the fallback. Returns the binary name
// or an empty string if no runtime is available.
func detectRuntime() string {
	for _, bin := range []string{"podman", "docker"} {
		if _, err := exec.LookPath(bin); err == nil {
			return bin
		}
	}
	return ""
}

// containerResult holds the outcome of a containerized command execution.
type containerResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Error    string
}

// runContainer executes a command inside an OCI container. Workspace is
// bind-mounted at /workspace, which is also the working directory. Env vars
// are passed via a temp --env-file to avoid shell escaping issues.
func runContainer(ctx context.Context, runtime, image, workspace string, command []string, env []string, logBroadcaster *LogBroadcaster, taskID string, log *slog.Logger) containerResult {
	// Validate runtime is available (should already be checked by caller).
	if _, err := exec.LookPath(runtime); err != nil {
		return containerResult{ExitCode: -1, Error: fmt.Sprintf("%s not found: %v", runtime, err)}
	}

	// Verify image exists locally or pull it.
	if err := ensureImage(ctx, runtime, image, log); err != nil {
		return containerResult{ExitCode: -1, Error: fmt.Sprintf("pull image %s: %v", image, err)}
	}

	// Write env vars to a temp file for --env-file. This avoids quoting
	// issues and keeps the command line clean.
	envFile, err := writeEnvFile(env)
	if err != nil {
		return containerResult{ExitCode: -1, Error: fmt.Sprintf("write env file: %v", err)}
	}
	defer os.Remove(envFile)

	containerName := "ziggurat-" + taskID[:12]

	// Build container run args.
	// --rm: auto-remove container on exit
	// --name: predictable name for stop/kill
	// -v: bind-mount workspace
	// -w: working directory inside container
	// --env-file: pass environment
	args := []string{
		"run", "--rm",
		"--name", containerName,
		"-v", workspace + ":/workspace",
		"-w", "/workspace",
		"--env-file", envFile,
	}
	// --pull=missing: only pull if image not already present locally.
	// Podman 4.x defaults to --pull=never; Docker default varies by version.
	args = append(args, "--pull", "missing")
	args = append(args, image)
	args = append(args, command...)

	cmd := exec.CommandContext(ctx, runtime, args...)
	cmd.Dir = workspace
	stdoutBuf, stderrBuf := captureOutput(cmd, logBroadcaster, taskID)

	if err := cmd.Start(); err != nil {
		return containerResult{ExitCode: -1, Error: fmt.Sprintf("container start: %v", err)}
	}

	// Cancel via `runtime stop`, escalating to `runtime kill` after a grace
	// period. Each control command gets its own bounded context.
	exitCode, forcedShutdown, runErr := runManaged(ctx, cmd, 5*time.Second,
		func() {
			log.Debug("stopping container", "name", containerName, "runtime", runtime)
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopCancel()
			exec.CommandContext(stopCtx, runtime, "stop", containerName).Run() // best-effort
		},
		func() {
			killCtx, killCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer killCancel()
			exec.CommandContext(killCtx, runtime, "kill", containerName).Run()
		})
	if runErr != nil {
		return containerResult{
			ExitCode: -1,
			Stdout:   stdoutBuf.String(),
			Stderr:   stderrBuf.String(),
			Error:    fmt.Sprintf("container exec error: %v", runErr),
		}
	}

	if forcedShutdown && exitCode == 0 && ctx.Err() == context.DeadlineExceeded {
		return containerResult{
			ExitCode: -1,
			Stdout:   stdoutBuf.String(),
			Stderr:   stderrBuf.String(),
			Error:    "container timeout exceeded",
		}
	}

	return containerResult{
		ExitCode: exitCode,
		Stdout:   stdoutBuf.String(),
		Stderr:   stderrBuf.String(),
	}
}

// ensureImage checks if the image exists locally and pulls it if needed.
func ensureImage(ctx context.Context, runtime, image string, log *slog.Logger) error {
	// Check if image exists locally.
	checkCmd := exec.CommandContext(ctx, runtime, "image", "inspect", image)
	if checkCmd.Run() == nil {
		return nil // image exists locally
	}

	// Pull the image.
	log.Info("pulling container image", "runtime", runtime, "image", image)
	pullCmd := exec.CommandContext(ctx, runtime, "pull", image)
	var pullStderr bytes.Buffer
	pullCmd.Stderr = &pullStderr
	if err := pullCmd.Run(); err != nil {
		return fmt.Errorf("%s: %v", strings.TrimSpace(pullStderr.String()), err)
	}
	return nil
}

// writeEnvFile writes key=value pairs to a temp file in a format suitable
// for --env-file (one KEY=value per line). Rejects values containing
// newlines to prevent env-var injection via the line-oriented file format.
func writeEnvFile(env []string) (string, error) {
	f, err := os.CreateTemp("", "ziggurat-env-*")
	if err != nil {
		return "", err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, e := range env {
		if strings.Contains(e, "\n") || strings.Contains(e, "\r") {
			return "", fmt.Errorf("env value contains newline (injection rejected): %q", e)
		}
		if _, err := fmt.Fprintln(w, e); err != nil {
			return "", err
		}
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	return f.Name(), nil
}
