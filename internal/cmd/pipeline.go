package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newPipelineCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pipeline",
		Short: "Manage pipelines (task DAGs)",
	}
	cmd.AddCommand(newPipelineListCmd())
	cmd.AddCommand(newPipelineSubmitCmd())
	cmd.AddCommand(newPipelineStatusCmd())
	cmd.AddCommand(newPipelineCancelCmd())
	cmd.AddCommand(newPipelineRetryCmd())
	return cmd
}

func newPipelineListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all pipelines",
		Args:  cobra.NoArgs,
		RunE:  runPipelineList,
	}
}

func newPipelineSubmitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "submit <file>",
		Short: "Submit a pipeline definition (YAML or JSON)",
		Args:  cobra.ExactArgs(1),
		RunE:  runPipelineSubmit,
	}
}

func newPipelineStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <id>",
		Short: "Show pipeline status and stage details",
		Args:  cobra.ExactArgs(1),
		RunE:  runPipelineStatus,
	}
}

func newPipelineCancelCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <id>",
		Short: "Cancel a running pipeline",
		Args:  cobra.ExactArgs(1),
		RunE:  runPipelineCancel,
	}
}

func newPipelineRetryCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "retry <id>",
		Short: "Retry a failed pipeline from the first failed stage",
		Args:  cobra.ExactArgs(1),
		RunE:  runPipelineRetry,
	}
}

type pipelineDef struct {
	Name   string     `yaml:"name" json:"name"`
	Stages []stageDef `yaml:"stages" json:"stages"`
}

type stageDef struct {
	ID          string            `yaml:"id" json:"id"`
	Command     []string          `yaml:"command" json:"command"`
	Artifacts   []string          `yaml:"artifacts,omitempty" json:"artifacts,omitempty"`
	InputRefs   map[string]string `yaml:"input_refs,omitempty" json:"input_refs,omitempty"`
	Params      map[string]string `yaml:"params,omitempty" json:"params,omitempty"`
	Requires    []string          `yaml:"requires,omitempty" json:"requires,omitempty"`
	Constraints []string          `yaml:"constraints,omitempty" json:"constraints,omitempty"`
	Image       string            `yaml:"image,omitempty" json:"image,omitempty"`
	DependsOn   []string          `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`
	Config      json.RawMessage   `yaml:"config,omitempty" json:"config,omitempty"`
}

func runPipelineList(cmd *cobra.Command, args []string) error {
	resp, err := doGet("/pipelines")
	if err != nil {
		return err
	}

	var pipelines []map[string]any
	if err := readJSON(resp, &pipelines); err != nil {
		return err
	}

	if jsonOut {
		printJSON(pipelines)
		return nil
	}

	if len(pipelines) == 0 {
		fmt.Println("No pipelines.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tSTATUS\tSTAGES")
	for _, p := range pipelines {
		id, _ := p["id"].(string)
		name, _ := p["name"].(string)
		status, _ := p["status"].(string)
		stages, _ := p["stages"].([]any)
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", shortID(id), name, status, len(stages))
	}
	w.Flush()
	return nil
}

func runPipelineSubmit(cmd *cobra.Command, args []string) error {
	data, err := os.ReadFile(args[0])
	if err != nil {
		return fmt.Errorf("read pipeline file: %w", err)
	}

	var def pipelineDef
	if err := yaml.Unmarshal(data, &def); err != nil {
		return fmt.Errorf("parse pipeline file: %w", err)
	}

	// Reject image field client-side — OCI execution is not yet supported.
	for i, s := range def.Stages {
		if s.Image != "" {
			return fmt.Errorf("stage[%d] (%s): OCI image execution is not yet supported; remove the image field", i, s.ID)
		}
	}

	// Convert to JSON for the API.
	jsonData, err := json.Marshal(def)
	if err != nil {
		return err
	}
	var payload map[string]any
	json.Unmarshal(jsonData, &payload)

	resp, err := doPost("/pipelines", payload)
	if err != nil {
		return err
	}

	var result map[string]any
	if err := readJSON(resp, &result); err != nil {
		return err
	}

	if jsonOut {
		printJSON(result)
		return nil
	}

	id, _ := result["id"].(string)
	name, _ := result["name"].(string)
	status, _ := result["status"].(string)
	stages, _ := result["stages"].([]any)
	fmt.Printf("Pipeline %s (%s) — %s, %d stages\n", id, name, status, len(stages))
	return nil
}

func runPipelineStatus(cmd *cobra.Command, args []string) error {
	resp, err := doGet("/pipelines/" + args[0])
	if err != nil {
		return err
	}

	var result map[string]any
	if err := readJSON(resp, &result); err != nil {
		return err
	}

	if jsonOut {
		printJSON(result)
		return nil
	}

	id, _ := result["id"].(string)
	name, _ := result["name"].(string)
	status, _ := result["status"].(string)
	fmt.Printf("Pipeline: %s\n", id)
	fmt.Printf("Name:     %s\n", name)
	fmt.Printf("Status:   %s\n", status)
	fmt.Println()

	stages, _ := result["stages"].([]any)
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "STAGE\tSTATUS\tTASK\tERROR")
	for _, s := range stages {
		sm, _ := s.(map[string]any)
		sid, _ := sm["id"].(string)
		ss, _ := sm["status"].(string)
		tid, _ := sm["task_id"].(string)
		serr, _ := sm["error"].(string)
		if len(tid) > 8 {
			tid = tid[:8]
		}
		if len(serr) > 80 {
			serr = serr[:80] + "..."
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", sid, ss, tid, serr)
	}
	w.Flush()
	return nil
}

func runPipelineCancel(cmd *cobra.Command, args []string) error {
	resp, err := doDelete("/pipelines/" + args[0])
	if err != nil {
		return err
	}

	var result map[string]any
	if err := readJSON(resp, &result); err != nil {
		return err
	}

	if jsonOut {
		printJSON(result)
		return nil
	}
	id, _ := result["id"].(string)
	fmt.Printf("Pipeline %s cancelled.\n", shortID(id))
	return nil
}

func runPipelineRetry(cmd *cobra.Command, args []string) error {
	resp, err := doPost("/pipelines/"+args[0]+"/retry", nil)
	if err != nil {
		return err
	}

	var result map[string]any
	if err := readJSON(resp, &result); err != nil {
		return err
	}

	if jsonOut {
		printJSON(result)
		return nil
	}
	id, _ := result["id"].(string)
	fmt.Printf("Pipeline %s retrying from failed stage.\n", shortID(id))
	return nil
}

