package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

func newPinCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "pin <key>",
		Short: "Pin an object to prevent garbage collection",
		Args:  cobra.ExactArgs(1),
		RunE:  runPin,
	}
}

func runPin(cmd *cobra.Command, args []string) error {
	path := storeKeyPath(args[0]) + "/pin"
	resp, err := doPost(path, nil)
	if err != nil {
		return fmt.Errorf("pin object: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var errBody struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&errBody)
		return fmt.Errorf("server: %s", errBody.Error)
	}
	fmt.Printf("pinned %s\n", args[0])
	return nil
}

func newUnpinCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unpin <key>",
		Short: "Unpin an object, allowing garbage collection",
		Args:  cobra.ExactArgs(1),
		RunE:  runUnpin,
	}
}

func runUnpin(cmd *cobra.Command, args []string) error {
	path := storeKeyPath(args[0]) + "/pin"
	resp, err := doDelete(path)
	if err != nil {
		return fmt.Errorf("unpin object: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var errBody struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&errBody)
		return fmt.Errorf("server: %s", errBody.Error)
	}
	fmt.Printf("unpinned %s\n", args[0])
	return nil
}
