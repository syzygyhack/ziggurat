package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	runWait           bool
	runInputs         []string
	runArtifacts      []string
	runParams         []string
	runRequires       []string
	runImage          string
	runPriority       int
	runTimeout        time.Duration
	runRetries        int
	runMemory         string
	runCPUs           int
	runMaxOutput      string
	runConstraints    []string
	runKeepWorkspace  bool
	runAffinity       string
	runEnv            string
	runEnvSetup       string
	runEnvFingerprint []string
)

func newRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run [flags] -- command [args...]",
		Short: "Submit a task for execution",
		Long:  `Submits a task to the cluster. Use -- to separate ziggurat flags from the task command.`,
		Args:  cobra.MinimumNArgs(1),
		RunE:  runRun,
	}
	cmd.Flags().BoolVarP(&runWait, "wait", "w", false, "wait for the task to complete")
	cmd.Flags().StringArrayVar(&runInputs, "input", nil, "named input reference (name=store-key)")
	cmd.Flags().StringArrayVar(&runArtifacts, "artifact", nil, "artifact store key fetched into workspace root")
	cmd.Flags().StringArrayVar(&runParams, "param", nil, "task parameter (key=value)")
	cmd.Flags().StringArrayVar(&runRequires, "require", nil, "required worker tag")
	cmd.Flags().StringArrayVar(&runConstraints, "constraint", nil, "capability constraint (e.g. \"gpu.vram >= 16GB\")")
	cmd.Flags().StringVar(&runImage, "image", "", "OCI image ref (optional)")
	cmd.Flags().IntVar(&runPriority, "priority", 0, "task priority (higher = sooner)")
	cmd.Flags().DurationVar(&runTimeout, "timeout", 0, "per-attempt timeout")
	cmd.Flags().IntVar(&runRetries, "retries", 0, "max retry count")
	cmd.Flags().StringVar(&runMemory, "memory", "", "memory requirement (e.g. 4GB)")
	cmd.Flags().IntVar(&runCPUs, "cpus", 0, "CPU cores requirement")
	cmd.Flags().StringVar(&runMaxOutput, "max-output", "", "output size limit")
	cmd.Flags().BoolVar(&runKeepWorkspace, "keep-workspace", false, "don't clean up workspace on failure")
	cmd.Flags().StringVar(&runAffinity, "affinity", "", "prefer a specific node ID for scheduling")
	cmd.Flags().StringVar(&runEnv, "env", "", "persistent environment name")
	cmd.Flags().StringVar(&runEnvSetup, "env-setup", "", "setup command for the environment (run in shell)")
	cmd.Flags().StringArrayVar(&runEnvFingerprint, "env-fingerprint", nil, "file whose content determines env staleness")
	return cmd
}

func runRun(cmd *cobra.Command, args []string) error {
	body := map[string]any{
		"command": args,
	}

	// Build input_refs map.
	if len(runInputs) > 0 {
		inputs := make(map[string]string)
		for _, kv := range runInputs {
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid --input format %q (expected name=store-key)", kv)
			}
			inputs[parts[0]] = parts[1]
		}
		body["input_refs"] = inputs
	}

	if len(runArtifacts) > 0 {
		body["artifacts"] = runArtifacts
	}

	// Build params map.
	if len(runParams) > 0 {
		params := make(map[string]string)
		for _, kv := range runParams {
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid --param format %q (expected key=value)", kv)
			}
			params[parts[0]] = parts[1]
		}
		body["params"] = params
	}

	if len(runRequires) > 0 {
		body["requires"] = runRequires
	}

	if len(runConstraints) > 0 {
		body["constraints"] = runConstraints
	}

	if runImage != "" {
		body["image"] = runImage
	}

	// Build environment sub-object.
	if runEnv != "" || runEnvSetup != "" || len(runEnvFingerprint) > 0 {
		envObj := map[string]any{}
		if runEnv != "" {
			envObj["name"] = runEnv
		}
		if runEnvSetup != "" {
			// Wrap in shell so the user can write a single string command.
			envObj["setup"] = []string{"sh", "-c", runEnvSetup}
		}
		if len(runEnvFingerprint) > 0 {
			envObj["fingerprint"] = runEnvFingerprint
		}
		body["environment"] = envObj
	}

	// Build config sub-object.
	cfg := map[string]any{}
	if runPriority != 0 {
		cfg["priority"] = runPriority
	}
	if runTimeout != 0 {
		cfg["timeout"] = runTimeout.String()
	}
	if runRetries != 0 {
		cfg["max_retries"] = runRetries
	}
	if runKeepWorkspace {
		cfg["keep_workspace"] = true
	}
	if runAffinity != "" {
		cfg["affinity"] = runAffinity
	}
	if runMaxOutput != "" {
		size, err := parseSize(runMaxOutput)
		if err != nil {
			return fmt.Errorf("invalid --max-output: %w", err)
		}
		cfg["max_output_size"] = size
	}
	if len(cfg) > 0 {
		body["config"] = cfg
	}

	// Build resources sub-object.
	resources := map[string]any{}
	if runCPUs > 0 {
		resources["cpu_cores"] = runCPUs
	}
	if runMemory != "" {
		size, err := parseSize(runMemory)
		if err != nil {
			return fmt.Errorf("invalid --memory: %w", err)
		}
		resources["memory"] = size
	}
	if len(resources) > 0 {
		body["resources"] = resources
	}

	resp, err := doPost("/tasks", body)
	if err != nil {
		return fmt.Errorf("submit task: %w", err)
	}

	var task map[string]any
	if err := readJSON(resp, &task); err != nil {
		return err
	}

	id, _ := task["id"].(string)
	if !runWait {
		if jsonOut {
			printJSON(task)
		} else {
			fmt.Println(id)
		}
		return nil
	}

	// Wait for completion.
	resp, err = doPost("/tasks/"+id+"/wait", nil)
	if err != nil {
		return fmt.Errorf("wait for task: %w", err)
	}

	var result map[string]any
	if err := readJSON(resp, &result); err != nil {
		return err
	}

	status, _ := result["status"].(string)

	if jsonOut {
		printJSON(result)
		if status != "completed" {
			exitCode := 3
			if ec, ok := result["exit_code"].(float64); ok && int(ec) > 0 && int(ec) < 126 {
				exitCode = int(ec)
			}
			return &ExitError{Code: exitCode, Msg: fmt.Sprintf("task %s", status)}
		}
		return nil
	}

	// Human-readable output.
	stdout, _ := result["stdout"].(string)
	stderr, _ := result["stderr"].(string)

	if stdout != "" {
		fmt.Print(stdout)
	}
	if stderr != "" {
		fmt.Fprintf(cmd.ErrOrStderr(), "%s", stderr)
	}

	if status != "completed" {
		errMsg, _ := result["error"].(string)
		exitCode := 3
		if ec, ok := result["exit_code"].(float64); ok && int(ec) > 0 && int(ec) < 126 {
			exitCode = int(ec)
		}
		return &ExitError{
			Code: exitCode,
			Msg:  fmt.Sprintf("task %s: %s", status, strings.TrimSpace(errMsg)),
		}
	}

	return nil
}

// parseSize parses human-readable byte sizes (e.g. "4GB", "512MB", "1024").
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}

	multiplier := int64(1)
	upper := strings.ToUpper(s)
	for _, suffix := range []struct {
		s string
		m int64
	}{
		{"TB", 1 << 40},
		{"GB", 1 << 30},
		{"MB", 1 << 20},
		{"KB", 1 << 10},
	} {
		if strings.HasSuffix(upper, suffix.s) {
			multiplier = suffix.m
			s = strings.TrimSpace(s[:len(s)-len(suffix.s)])
			break
		}
	}

	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * multiplier, nil
}
