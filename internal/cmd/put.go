package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/syzygyhack/ziggurat/internal/store"
)

func newPutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "put <key> <path>",
		Short: "Store a file or directory under a namespace key",
		Long:  `Uploads an object to storage. If path is a directory, it is archived as a deterministic tar before upload.`,
		Args:  cobra.ExactArgs(2),
		RunE:  runPut,
	}
}

func runPut(cmd *cobra.Command, args []string) error {
	key, filePath := args[0], args[1]

	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	var body io.Reader
	if info.IsDir() {
		// Directory: create deterministic tar archive via pipe.
		pr, pw := io.Pipe()
		go func() {
			tarErr := store.CreateDeterministicTar(filePath, pw)
			pw.CloseWithError(tarErr)
		}()
		body = pr
	} else {
		f, err := os.Open(filePath)
		if err != nil {
			return err
		}
		defer f.Close()
		body = f
	}

	resp, err := doPut("/store/"+key, body)
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}

	var result map[string]string
	if err := readJSON(resp, &result); err != nil {
		return err
	}

	if jsonOut {
		printJSON(result)
		return nil
	}

	fmt.Printf("%s -> %s\n", result["key"], result["hash"])
	return nil
}
