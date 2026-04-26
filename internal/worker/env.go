package worker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/zeebo/blake3"
)

const (
	envFingerprintFile = ".ziggurat-fingerprint"
	envLastUsedFile    = ".ziggurat-last-used"
	envLockFile        = ".ziggurat-lock"
)

// EnvDir returns the root directory for persistent task environments.
func EnvDir(dataDir string) string {
	return filepath.Join(dataDir, "envs")
}

// ResolveEnv prepares a persistent environment for a task. It returns the
// absolute path to the environment directory and any error. If the task has
// no Environment set, it returns ("", nil).
//
// The resolution process:
//  1. Determine env name from Name, Fingerprint hash, or task ID.
//  2. Create the env directory if it doesn't exist.
//  3. Compare the current fingerprint against the stored one.
//  4. If stale or new, acquire a lock and run the Setup command.
//  5. Touch the last-used timestamp.
func ResolveEnv(ctx context.Context, task *model.Task, dataDir, workspace string, log *slog.Logger) (string, error) {
	env := task.Environment
	if env == nil {
		return "", nil
	}

	name, err := envName(env, task.ID, workspace)
	if err != nil {
		return "", fmt.Errorf("resolve env name: %w", err)
	}

	envRoot := EnvDir(dataDir)
	envPath := filepath.Join(envRoot, name)
	if err := os.MkdirAll(envPath, 0o755); err != nil {
		return "", fmt.Errorf("create env dir: %w", err)
	}

	// Compute current fingerprint from referenced files.
	currentFP, err := computeFingerprint(env.Fingerprint, workspace)
	if err != nil {
		return "", fmt.Errorf("compute fingerprint: %w", err)
	}

	// Check if setup is needed.
	needsSetup := false
	if len(env.Setup) > 0 {
		storedFP, _ := os.ReadFile(filepath.Join(envPath, envFingerprintFile))
		if currentFP == "" {
			// No fingerprint files — setup runs only if env is brand new.
			if !fileExists(filepath.Join(envPath, envFingerprintFile)) {
				needsSetup = true
			}
		} else if string(storedFP) != currentFP {
			needsSetup = true
		}
	}

	if needsSetup {
		lockPath := filepath.Join(envPath, envLockFile)
		unlock, err := acquireLock(ctx, lockPath)
		if err != nil {
			return "", fmt.Errorf("lock env: %w", err)
		}
		defer unlock()

		// Re-check after acquiring lock — another task may have set up while we waited.
		storedFP, _ := os.ReadFile(filepath.Join(envPath, envFingerprintFile))
		recheck := false
		if currentFP == "" {
			recheck = !fileExists(filepath.Join(envPath, envFingerprintFile))
		} else {
			recheck = string(storedFP) != currentFP
		}

		if recheck {
			log.Info("running env setup", "env", name, "command", env.Setup)
			if err := runSetup(ctx, env.Setup, envPath, workspace); err != nil {
				return "", fmt.Errorf("env setup failed: %w", err)
			}

			// Write fingerprint (or empty marker if no fingerprint files).
			fp := currentFP
			if fp == "" {
				fp = "initialized"
			}
			if err := os.WriteFile(filepath.Join(envPath, envFingerprintFile), []byte(fp), 0o644); err != nil {
				return "", fmt.Errorf("write fingerprint: %w", err)
			}
		}
	}

	// Touch last-used timestamp.
	now := time.Now().Format(time.RFC3339)
	os.WriteFile(filepath.Join(envPath, envLastUsedFile), []byte(now), 0o644)

	return envPath, nil
}

// envName determines the environment name from the TaskEnvironment config.
func envName(env *model.TaskEnvironment, taskID, workspace string) (string, error) {
	if env.Name != "" {
		return sanitizeName(env.Name), nil
	}
	if len(env.Fingerprint) > 0 {
		fp, err := computeFingerprint(env.Fingerprint, workspace)
		if err != nil {
			return "", err
		}
		if fp != "" {
			return fp[:16], nil
		}
	}
	// Fallback: task ID (ephemeral, no reuse).
	return taskID, nil
}

// computeFingerprint hashes the contents of the named files. Files are
// searched in the workspace root, then workspace/input/. Returns "" if
// no files are found or the list is empty.
func computeFingerprint(files []string, workspace string) (string, error) {
	if len(files) == 0 {
		return "", nil
	}

	h := blake3.New()
	for _, name := range files {
		data, err := readFingerprintFile(name, workspace)
		if err != nil {
			return "", err
		}
		h.Write([]byte(name))
		h.Write([]byte{0})
		h.Write(data)
		h.Write([]byte{0})
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// readFingerprintFile searches for a file by name in workspace root, then
// workspace/input/, then as an absolute path.
func readFingerprintFile(name, workspace string) ([]byte, error) {
	candidates := []string{
		filepath.Join(workspace, name),
		filepath.Join(workspace, "input", name),
	}
	if filepath.IsAbs(name) {
		candidates = append(candidates, name)
	}
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err == nil {
			return data, nil
		}
	}
	return nil, fmt.Errorf("fingerprint file %q not found", name)
}

// runSetup executes the setup command in the env directory. The workspace
// is passed via ZIGGURAT_WORKSPACE so the setup script can reference
// input files if needed.
func runSetup(ctx context.Context, command []string, envPath, workspace string) error {
	if len(command) == 0 {
		return nil
	}

	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Dir = envPath
	cmd.Env = append(os.Environ(),
		"ZIGGURAT_ENV="+envPath,
		"ZIGGURAT_WORKSPACE="+workspace,
		"ZIGGURAT_INPUT="+filepath.Join(workspace, "input"),
	)

	var stderr bytes.Buffer
	cmd.Stdout = os.Stderr // setup output visible in worker logs
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// acquireLock creates a simple file-based lock. Returns an unlock function.
// Uses os.OpenFile with O_CREATE|O_EXCL for atomicity. Spins with backoff
// until the lock is acquired or the context is cancelled.
func acquireLock(ctx context.Context, path string) (func(), error) {
	for {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			f.Close()
			return func() { os.Remove(path) }, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		// Check if lock is stale (older than 10 minutes — setup shouldn't take that long).
		info, statErr := os.Stat(path)
		if statErr == nil && time.Since(info.ModTime()) > 10*time.Minute {
			os.Remove(path)
			continue
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("lock wait cancelled: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// PruneEnvs removes environment directories that haven't been used within
// maxAge, then enforces maxCount via FIFO eviction. Returns the number of
// envs removed.
func PruneEnvs(dataDir string, maxAge time.Duration, maxCount int) int {
	envRoot := EnvDir(dataDir)
	entries, err := os.ReadDir(envRoot)
	if err != nil {
		return 0
	}

	type envEntry struct {
		name     string
		lastUsed time.Time
	}

	var envs []envEntry
	now := time.Now()
	removed := 0

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(envRoot, e.Name())
		lu := lastUsedTime(path)

		// Remove if older than maxAge.
		if maxAge > 0 && now.Sub(lu) > maxAge {
			os.RemoveAll(path)
			removed++
			continue
		}
		envs = append(envs, envEntry{name: e.Name(), lastUsed: lu})
	}

	// FIFO eviction beyond maxCount.
	if maxCount > 0 && len(envs) > maxCount {
		sort.Slice(envs, func(i, j int) bool {
			return envs[i].lastUsed.Before(envs[j].lastUsed)
		})
		excess := len(envs) - maxCount
		for i := 0; i < excess; i++ {
			os.RemoveAll(filepath.Join(envRoot, envs[i].name))
			removed++
		}
	}

	return removed
}

// ListEnvs returns information about persistent environments on this node.
func ListEnvs(dataDir string) []EnvInfo {
	envRoot := EnvDir(dataDir)
	entries, err := os.ReadDir(envRoot)
	if err != nil {
		return nil
	}

	var result []EnvInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(envRoot, e.Name())
		info := EnvInfo{
			Name:     e.Name(),
			Path:     path,
			LastUsed: lastUsedTime(path),
		}
		// Read fingerprint if present.
		if fp, err := os.ReadFile(filepath.Join(path, envFingerprintFile)); err == nil {
			info.Fingerprint = string(fp)
		}
		// Compute size.
		info.SizeBytes = dirSizeQuiet(path)
		result = append(result, info)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LastUsed.After(result[j].LastUsed)
	})
	return result
}

// EnvInfo describes a persistent environment on disk.
type EnvInfo struct {
	Name        string    `json:"name"`
	Path        string    `json:"path"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	LastUsed    time.Time `json:"last_used"`
	SizeBytes   int64     `json:"size_bytes"`
}

func lastUsedTime(envPath string) time.Time {
	data, err := os.ReadFile(filepath.Join(envPath, envLastUsedFile))
	if err == nil {
		if t, err := time.Parse(time.RFC3339, strings.TrimSpace(string(data))); err == nil {
			return t
		}
	}
	// Fall back to directory mod time.
	info, err := os.Stat(envPath)
	if err != nil {
		return time.Time{}
	}
	return info.ModTime()
}

func sanitizeName(name string) string {
	// Replace path separators and dangerous characters.
	r := strings.NewReplacer("/", "_", "\\", "_", "..", "_", " ", "_")
	return r.Replace(name)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func dirSizeQuiet(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		size += info.Size()
		return nil
	})
	return size
}
