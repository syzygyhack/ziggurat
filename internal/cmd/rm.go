package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newRmCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm <key>",
		Short: "Delete a stored object",
		Args:  cobra.ExactArgs(1),
		RunE:  runRm,
	}
}

func runRm(cmd *cobra.Command, args []string) error {
	resp, err := doDelete(storeKeyPath(args[0]))
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	var result map[string]string
	if err := readJSON(resp, &result); err != nil {
		return err
	}

	if jsonOut {
		printJSON(result)
	} else {
		fmt.Printf("deleted %s\n", args[0])
	}
	return nil
}
