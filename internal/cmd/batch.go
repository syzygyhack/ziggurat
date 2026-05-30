package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var batchFile string

func newBatchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "batch",
		Short: "Submit a batch of tasks from a YAML or JSON file",
		Args:  cobra.NoArgs,
		RunE:  runBatch,
	}
	cmd.Flags().StringVar(&batchFile, "from", "", "path to batch file (YAML or JSON)")
	cmd.MarkFlagRequired("from")
	return cmd
}

// batchTask mirrors the API submitTaskRequest. YAML/JSON fields match the API.
type batchTask struct {
	Command     []string          `yaml:"command" json:"command"`
	Env         map[string]string `yaml:"env,omitempty" json:"env,omitempty"`
	InputRefs   map[string]string `yaml:"input_refs,omitempty" json:"input_refs,omitempty"`
	Artifacts   []string          `yaml:"artifacts,omitempty" json:"artifacts,omitempty"`
	Params      map[string]string `yaml:"params,omitempty" json:"params,omitempty"`
	Requires    []string          `yaml:"requires,omitempty" json:"requires,omitempty"`
	Constraints []string          `yaml:"constraints,omitempty" json:"constraints,omitempty"`
	Image       string            `yaml:"image,omitempty" json:"image,omitempty"`
	Config      json.RawMessage   `yaml:"config,omitempty" json:"config,omitempty"`
}

func runBatch(cmd *cobra.Command, args []string) error {
	data, err := os.ReadFile(batchFile)
	if err != nil {
		return fmt.Errorf("read batch file: %w", err)
	}

	var tasks []batchTask
	if err := yaml.Unmarshal(data, &tasks); err != nil {
		return fmt.Errorf("parse batch file: %w", err)
	}
	if len(tasks) == 0 {
		return fmt.Errorf("batch file contains no tasks")
	}


	// Convert to JSON for the API call (yaml tags match json tags).
	jsonData, err := json.Marshal(tasks)
	if err != nil {
		return err
	}

	// Re-decode as generic slice for doPost.
	var payload []any
	if err := json.Unmarshal(jsonData, &payload); err != nil {
		return fmt.Errorf("encode batch: %w", err)
	}

	resp, err := doPost("/tasks/batch", payload)
	if err != nil {
		return err
	}

	var results []map[string]any
	if err := readJSON(resp, &results); err != nil {
		return err
	}

	if jsonOut {
		printJSON(results)
		return nil
	}

	fmt.Printf("Submitted %d tasks:\n", len(results))
	for _, t := range results {
		id, _ := t["id"].(string)
		if len(id) > 8 {
			id = id[:8]
		}
		fmt.Printf("  %s\n", id)
	}
	return nil
}
