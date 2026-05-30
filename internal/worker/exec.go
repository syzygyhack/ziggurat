package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/syzygyhack/ziggurat/internal/config"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
)

// ExecResult captures the outcome of a task execution.
type ExecResult struct {
	ExitCode    int
	Stdout      string
	Stderr      string
	OutputRef   string
	OutputBytes int64
	WallTime    time.Duration
	Error       string
}

// Execute runs a task in an isolated workspace. This is the core execution
// engine: workspace setup -> env resolve -> input fetch -> artifact fetch -> subprocess -> output upload.
// gpuDevices holds allocated GPU indices; when non-empty, CUDA_VISIBLE_DEVICES is set.
// logBroadcaster is optional; when non-nil, stdout/stderr are streamed live to subscribers.
func Execute(ctx context.Context, task *model.Task, s *store.Store, cfg config.ComputeConfig, dataDir string, gpuDevices []int, logBroadcaster *LogBroadcaster, log *slog.Logger) (result *ExecResult) {
	start := time.Now()

	// 1. Create workspace.
	wsDir := cfg.WorkspaceDir
	if wsDir == "" {
		wsDir = filepath.Join(os.TempDir(), "ziggurat")
	}
	workspace := filepath.Join(wsDir, task.ID)
	inputDir := filepath.Join(workspace, "input")
	outputDir := filepath.Join(workspace, "output")

	for _, dir := range []string{workspace, inputDir, outputDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return &ExecResult{ExitCode: -1, Error: fmt.Sprintf("create workspace: %v", err)}
		}
	}

	// Close the log stream when execution finishes so SSE subscribers get
	// the "done" event. Must be deferred early (before any step that can
	// fail) — if input fetch or env resolution fails, an SSE subscriber
	// would otherwise leak waiting on a channel that will never close.
	if logBroadcaster != nil {
		defer logBroadcaster.Close(task.ID)
	}

	// Workspace cleanup deferred: retain if keep_workspace is set, or if the
	// task fails/is cancelled (per spec). Successful tasks always clean up.
	// After retention, enforce max_retained_workspaces via FIFO eviction.
	defer func() {
		retained := false
		if task.Config.KeepWorkspace {
			retained = true
		} else if result != nil && result.ExitCode != 0 {
			retained = true
		} else {
			os.RemoveAll(workspace)
		}

		if retained {
			EnforceWorkspaceLimit(wsDir, cfg.MaxRetainedWorkspaces)
		}
	}()

	// 2. Fetch inputs.
	for name, hashHex := range task.InputRefs {
		dest := filepath.Join(inputDir, name)
		if err := fetchObject(ctx, s, hashHex, dest); err != nil {
			return &ExecResult{ExitCode: -1, Error: fmt.Sprintf("fetch input %s: %v", name, err)}
		}
	}

	// 3. Fetch artifacts into workspace root.
	for _, hashHex := range task.Artifacts {
		if err := fetchObject(ctx, s, hashHex, workspace); err != nil {
			return &ExecResult{ExitCode: -1, Error: fmt.Sprintf("fetch artifact: %v", err)}
		}
	}

	// 4. Resolve persistent environment (if configured).
	envPath, envErr := ResolveEnv(ctx, task, dataDir, workspace, log)
	if envErr != nil {
		return &ExecResult{ExitCode: -1, Error: fmt.Sprintf("resolve env: %v", envErr)}
	}

	// 5. Build environment.
	env := BuildEnv(task, workspace, inputDir, outputDir)
	if envPath != "" {
		env = applyEnvPath(env, envPath)
	}
	if len(gpuDevices) > 0 {
		env = append(env, "CUDA_VISIBLE_DEVICES="+FormatDevices(gpuDevices))
	}

	// 6. Execute command.
	if len(task.Command) == 0 {
		return &ExecResult{ExitCode: -1, Error: "empty command"}
	}

	var exitCode int
	var stdoutStr, stderrStr string
	var errMsg string
	var forcedShutdown bool

	if task.Image != "" {
		// Container execution path.
		runtime := detectRuntime()
		if runtime == "" {
			return &ExecResult{ExitCode: -1, Error: "container execution requested but no container runtime (podman/docker) found"}
		}

		cr := runContainer(ctx, runtime, task.Image, workspace, task.Command, env, logBroadcaster, task.ID, log)
		exitCode = cr.ExitCode
		stdoutStr = cr.Stdout
		stderrStr = cr.Stderr
		errMsg = cr.Error
		// forcedShutdown detection: runContainer handles its own timeout/cancel,
		// so check context state after return for timeout detection.
		if ctx.Err() == context.DeadlineExceeded {
			forcedShutdown = true
		}
	} else {
		// Host execution path.
		cmd := exec.Command(task.Command[0], task.Command[1:]...)
		cmd.Dir = workspace
		cmd.Env = env

		var stdoutBuf, stderrBuf bytes.Buffer
		if logBroadcaster != nil {
			cmd.Stdout = io.MultiWriter(&stdoutBuf, logBroadcaster.Writer(task.ID, "stdout"))
			cmd.Stderr = io.MultiWriter(&stderrBuf, logBroadcaster.Writer(task.ID, "stderr"))
		} else {
			cmd.Stdout = &stdoutBuf
			cmd.Stderr = &stderrBuf
		}

		// Start process in its own process group for clean cancellation.
		SetProcessGroup(cmd)

		if err := cmd.Start(); err != nil {
			return &ExecResult{
				ExitCode: -1,
				WallTime: time.Since(start),
				Error:    fmt.Sprintf("start process: %v", err),
			}
		}

		// Wait for process exit or context cancellation (timeout / user cancel).
		waitCh := make(chan error, 1)
		go func() {
			waitCh <- cmd.Wait()
		}()

		var cmdErr error
		select {
		case cmdErr = <-waitCh:
			// Process exited on its own.
		case <-ctx.Done():
			// Context cancelled — graceful shutdown: SIGTERM → grace → SIGKILL.
			forcedShutdown = true
			SendTermSignal(cmd)
			select {
			case cmdErr = <-waitCh:
				// Process exited within grace period.
			case <-time.After(cfg.CancelGrace):
				KillProcess(cmd)
				cmdErr = <-waitCh
			}
		}

		if cmdErr != nil {
			if exitErr, ok := cmdErr.(*exec.ExitError); ok {
				exitCode = exitErr.ExitCode()
			} else {
				return &ExecResult{
					ExitCode: -1,
					Stdout:   truncate(stdoutBuf.String(), 64*1024),
					Stderr:   truncate(stderrBuf.String(), 64*1024),
					WallTime: time.Since(start),
					Error:    fmt.Sprintf("exec error: %v", cmdErr),
				}
			}
		}
		stdoutStr = stdoutBuf.String()
		stderrStr = stderrBuf.String()
		errMsg = ""
	}

	wallTime := time.Since(start)

	// If error was set during execution, return it now.
	if errMsg != "" {
		return &ExecResult{
			ExitCode: -1,
			Stdout:   truncate(stdoutStr, 64*1024),
			Stderr:   truncate(stderrStr, 64*1024),
			WallTime: wallTime,
			Error:    errMsg,
		}
	}

	result = &ExecResult{
		ExitCode: exitCode,
		Stdout:   truncate(stdoutStr, 64*1024),
		Stderr:   truncate(stderrStr, 64*1024),
		WallTime: wallTime,
	}

	// If we entered the forced-shutdown path (timeout or cancellation)
	// and the process exited 0, check why the context was cancelled.
	// Timeouts must be reported as failures even if the process exits
	// cleanly after SIGTERM.
	if forcedShutdown && exitCode == 0 && ctx.Err() == context.DeadlineExceeded {
		result.ExitCode = -1
		result.Error = fmt.Sprintf("timeout exceeded (wall: %s)", wallTime.Truncate(time.Millisecond))
		return result
	}

	// 7. Check output size limit.
	if exitCode == 0 {
		maxOutput := task.Config.MaxOutputSize
		if maxOutput == 0 {
			maxOutput = cfg.MaxOutputSize
		}
		outputSize, err := dirSize(outputDir)
		if err != nil {
			result.Error = fmt.Sprintf("measure output: %v", err)
			result.ExitCode = -1
			return result
		}
		if maxOutput > 0 && outputSize > maxOutput {
			result.Error = fmt.Sprintf("output size exceeded (actual: %d, limit: %d)", outputSize, maxOutput)
			result.ExitCode = -1
			return result
		}
		result.OutputBytes = outputSize

		// 8. Upload output as deterministic tar.
		if outputSize > 0 {
			ref, err := uploadOutput(ctx, s, outputDir, task.ID)
			if err != nil {
				result.Error = fmt.Sprintf("upload output: %v", err)
				result.ExitCode = -1
				return result
			}
			result.OutputRef = ref
		}
	}

	return result
}

func fetchObject(ctx context.Context, s *store.Store, hashHex, dest string) error {
	rc, err := s.GetByHash(ctx, hashHex)
	if err != nil {
		return err
	}

	// Stream to a temp file so we never hold the full object in memory.
	// After integrity verification (rc.Close), we try tar extraction from
	// the temp file, falling back to using it as a raw file.
	tmpFile, err := os.CreateTemp("", "ziggurat-fetch-*")
	if err != nil {
		rc.Close()
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath) // clean up temp file in all paths

	if _, err := io.Copy(tmpFile, rc); err != nil {
		tmpFile.Close()
		rc.Close()
		return fmt.Errorf("stream object: %w", err)
	}
	tmpFile.Close()

	if err := rc.Close(); err != nil {
		return fmt.Errorf("integrity check failed for %s: %w", hashHex[:12], err)
	}

	// Check if dest already exists as a directory (e.g. workspace root for
	// artifacts). We must never remove a pre-existing directory.
	destExisted := false
	if info, err := os.Stat(dest); err == nil && info.IsDir() {
		destExisted = true
	}

	// Try extracting as a tar archive into dest directory.
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	tf, err := os.Open(tmpPath)
	if err != nil {
		return err
	}
	tarErr := store.ExtractTar(tf, dest)
	tf.Close()
	if tarErr == nil {
		return nil
	}

	// Not a valid tar — write as a raw file by renaming the temp file.
	if destExisted {
		// dest was a pre-existing directory (workspace root for artifacts).
		// Cannot remove it; place raw content as a file named by hash prefix.
		return copyFile(tmpPath, filepath.Join(dest, hashHex[:12]), 0o755)
	}
	os.RemoveAll(dest)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	return copyFile(tmpPath, dest, 0o644)
}

// copyFile copies src to dst with the given permissions. Used instead of
// os.Rename because src (temp file) may be on a different filesystem.
func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func uploadOutput(ctx context.Context, s *store.Store, outputDir, taskID string) (string, error) {
	pr, pw := io.Pipe()

	errCh := make(chan error, 1)
	go func() {
		tarErr := store.CreateDeterministicTar(outputDir, pw)
		// CloseWithError BEFORE send so the reader is always unblocked,
		// even if the main goroutine exits on s.Put error.
		pw.CloseWithError(tarErr)
		errCh <- tarErr
	}()

	nsKey := fmt.Sprintf("output/%s", taskID)
	hash, err := s.Put(ctx, nsKey, pr)
	if err != nil {
		pr.Close() // unblock writer goroutine so errCh drain won't hang
		<-errCh     // wait for goroutine to finish to avoid leak
		return "", err
	}

	if err := <-errCh; err != nil {
		return "", fmt.Errorf("create tar: %w", err)
	}

	return hash, nil
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		size += info.Size()
		return nil
	})
	return size, err
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "\n... (truncated)"
}

// BuildEnv constructs the environment for a task process.
// ZIGGURAT_* vars are authoritative: inherited copies from the parent process
// are stripped, and user-supplied Env entries with the ZIGGURAT_ prefix or
// protected keys (PATH, HOME, USER, SHELL) are silently dropped.
func BuildEnv(task *model.Task, workspace, inputDir, outputDir string) []string {
	// Build the authoritative ZIGGURAT_* set.
	zigguratVars := map[string]string{
		"ZIGGURAT_WORKSPACE": workspace,
		"ZIGGURAT_INPUT":     inputDir,
		"ZIGGURAT_OUTPUT":    outputDir,
		"ZIGGURAT_TASK_ID":   task.ID,
		"ZIGGURAT_ATTEMPT":   fmt.Sprintf("%d", task.Attempt),
	}
	for k, v := range task.Params {
		zigguratVars["ZIGGURAT_PARAM_"+strings.ToUpper(k)] = v
	}

	// Inherit parent env, stripping any existing ZIGGURAT_* vars so
	// the worker-set values are the only copies.
	var env []string
	for _, e := range os.Environ() {
		if key, _, ok := strings.Cut(e, "="); ok {
			if strings.HasPrefix(strings.ToUpper(key), "ZIGGURAT_") {
				continue
			}
		}
		env = append(env, e)
	}

	// Add user-supplied env, dropping ZIGGURAT_* and protected keys.
	// Includes both Unix (HOME, USER, SHELL) and Windows (USERPROFILE,
	// SYSTEMROOT, COMSPEC, HOMEDRIVE, HOMEPATH) critical variables.
	protected := map[string]bool{
		"PATH": true, "HOME": true, "USER": true, "SHELL": true,
		"USERPROFILE": true, "SYSTEMROOT": true, "COMSPEC": true,
		"HOMEDRIVE": true, "HOMEPATH": true,
	}
	for k, v := range task.Env {
		upper := strings.ToUpper(k)
		if strings.HasPrefix(upper, "ZIGGURAT_") || protected[upper] {
			continue
		}
		env = append(env, k+"="+v)
	}

	// Append authoritative ZIGGURAT_* vars.
	for k, v := range zigguratVars {
		env = append(env, k+"="+v)
	}

	return env
}

// applyEnvPath injects persistent-environment variables into the env slice:
// ZIGGURAT_ENV, ZIGGURAT_ENV_NAME, VIRTUAL_ENV, and prepends <envPath>/bin
// to PATH so venv/node_modules binaries resolve first.
func applyEnvPath(env []string, envPath string) []string {
	binDir := filepath.Join(envPath, "bin")
	envName := filepath.Base(envPath)

	var result []string
	for _, e := range env {
		key, val, ok := strings.Cut(e, "=")
		if !ok {
			result = append(result, e)
			continue
		}
		if strings.ToUpper(key) == "PATH" {
			// Prepend env bin dir to PATH.
			result = append(result, key+"="+binDir+string(os.PathListSeparator)+val)
			continue
		}
		result = append(result, e)
	}

	result = append(result,
		"ZIGGURAT_ENV="+envPath,
		"ZIGGURAT_ENV_NAME="+envName,
		"VIRTUAL_ENV="+envPath,
	)
	return result
}
