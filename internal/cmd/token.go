package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"
)

func newTokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "token",
		Short: "Manage cluster join tokens",
	}
	cmd.AddCommand(newTokenGenerateCmd())
	return cmd
}

func newTokenGenerateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "generate",
		Short: "Generate a random cluster join token",
		Long: `Generates a cryptographically random 32-byte hex token for cluster join authentication.
Use this token in the security.join_token config field on all nodes.`,
		RunE: runTokenGenerate,
	}
}

func runTokenGenerate(cmd *cobra.Command, args []string) error {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return fmt.Errorf("generate token: %w", err)
	}
	fmt.Println(hex.EncodeToString(token))
	return nil
}
