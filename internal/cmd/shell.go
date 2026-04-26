package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"

	"github.com/spf13/cobra"
	"github.com/syzygyhack/ziggurat/internal/store"
)

func newShellCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "shell",
		Short: "Interactive REPL for cluster operations",
		Long: `Opens an interactive shell connected to the Ziggurat cluster.
Supports ls, put, get, rm, run, tasks, status, top, nodes, and help commands.
Type 'exit' or Ctrl+D to quit.`,
		Args: cobra.NoArgs,
		RunE: runShell,
	}
}

// shellCommand maps a command name to its handler.
type shellCommand struct {
	name  string
	help  string
	run   func(args []string) error
	usage string
}

func runShell(cmd *cobra.Command, args []string) error {
	// Verify connectivity.
	_, err := doGet("/cluster")
	if err != nil {
		return fmt.Errorf("cannot connect to cluster: %w", err)
	}

	commands := buildShellCommands()

	// Handle Ctrl+C gracefully — don't exit, just cancel current line.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		for range sigCh {
			fmt.Fprintln(os.Stderr, "\nUse 'exit' or Ctrl+D to quit.")
			fmt.Print("zig> ")
		}
	}()
	defer signal.Stop(sigCh)

	fmt.Println("Ziggurat interactive shell. Type 'help' for commands, 'exit' to quit.")
	fmt.Print("zig> ")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("zig> ")
			continue
		}

		if line == "exit" || line == "quit" {
			return nil
		}

		parts := splitArgs(line)
		if len(parts) == 0 {
			fmt.Print("zig> ")
			continue
		}

		name := parts[0]
		cmdArgs := parts[1:]

		if name == "help" {
			shellHelp(commands, cmdArgs)
			fmt.Print("zig> ")
			continue
		}

		found := false
		for _, sc := range commands {
			if sc.name == name {
				found = true
				if err := sc.run(cmdArgs); err != nil {
					fmt.Fprintf(os.Stderr, "error: %v\n", err)
				}
				break
			}
		}
		if !found {
			fmt.Fprintf(os.Stderr, "unknown command: %s (type 'help' for commands)\n", name)
		}

		fmt.Print("zig> ")
	}

	// EOF (Ctrl+D).
	fmt.Println()
	return nil
}

func buildShellCommands() []shellCommand {
	return []shellCommand{
		{
			name:  "ls",
			help:  "List stored objects",
			usage: "ls [prefix]",
			run:   shellLs,
		},
		{
			name:  "put",
			help:  "Store a file under a key",
			usage: "put <key> <path>",
			run:   shellPut,
		},
		{
			name:  "get",
			help:  "Retrieve an object to stdout or file",
			usage: "get <key> [dest]",
			run:   shellGet,
		},
		{
			name:  "rm",
			help:  "Delete an object by key",
			usage: "rm <key>",
			run:   shellRm,
		},
		{
			name:  "run",
			help:  "Submit a task",
			usage: "run <command...>",
			run:   shellRun,
		},
		{
			name:  "tasks",
			help:  "List tasks",
			usage: "tasks [--status <status>]",
			run:   shellTasks,
		},
		{
			name:  "status",
			help:  "Show cluster status",
			usage: "status",
			run:   shellStatus,
		},
		{
			name:  "nodes",
			help:  "List cluster nodes",
			usage: "nodes",
			run:   shellNodes,
		},
		{
			name:  "top",
			help:  "Cluster dashboard snapshot",
			usage: "top",
			run:   shellTop,
		},
	}
}

func shellHelp(commands []shellCommand, args []string) {
	if len(args) > 0 {
		for _, c := range commands {
			if c.name == args[0] {
				fmt.Printf("  %s - %s\n  usage: %s\n", c.name, c.help, c.usage)
				return
			}
		}
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
		return
	}
	fmt.Println("Commands:")
	for _, c := range commands {
		fmt.Printf("  %-10s %s\n", c.name, c.help)
	}
	fmt.Println("  exit       Quit the shell")
	fmt.Println("\nType 'help <command>' for usage details.")
}

func shellLs(args []string) error {
	prefix := ""
	if len(args) > 0 {
		prefix = args[0]
	}
	path := "/store"
	if prefix != "" {
		path += "?prefix=" + url.QueryEscape(prefix)
	}
	resp, err := doGet(path)
	if err != nil {
		return err
	}
	var keys []string
	if err := readJSON(resp, &keys); err != nil {
		return err
	}
	if len(keys) == 0 {
		fmt.Println("(empty)")
		return nil
	}
	for _, k := range keys {
		fmt.Println(k)
	}
	return nil
}

func shellPut(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: put <key> <path>")
	}
	key, filePath := args[0], args[1]

	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	var body io.Reader
	if info.IsDir() {
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

	resp, err := doPut(storeKeyPath(key), body)
	if err != nil {
		return err
	}
	var result map[string]string
	if err := readJSON(resp, &result); err != nil {
		return err
	}
	fmt.Printf("stored: %s (blake3:%s)\n", result["key"], shortID(result["hash"]))
	return nil
}

func shellGet(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: get <key> [dest]")
	}
	key := args[0]
	resp, err := doGet(storeKeyPath(key))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		// Try to extract structured error; fall back to raw body.
		var errBody struct {
			Error string `json:"error"`
		}
		if decErr := json.NewDecoder(resp.Body).Decode(&errBody); decErr == nil && errBody.Error != "" {
			return fmt.Errorf("server: %s", errBody.Error)
		}
		return fmt.Errorf("server returned %d", resp.StatusCode)
	}

	var w io.Writer = os.Stdout
	if len(args) > 1 {
		f, err := os.Create(args[1])
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

func shellRm(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: rm <key>")
	}
	resp, err := doDelete(storeKeyPath(args[0]))
	if err != nil {
		return err
	}
	if err := readJSON(resp, nil); err != nil {
		return err
	}
	fmt.Printf("deleted: %s\n", args[0])
	return nil
}

func shellRun(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: run <command...>")
	}
	body := map[string]any{
		"command": args,
	}
	resp, err := doPost("/tasks", body)
	if err != nil {
		return err
	}
	var result map[string]any
	if err := readJSON(resp, &result); err != nil {
		return err
	}
	id, _ := result["id"].(string)
	fmt.Printf("submitted: %s\n", shortID(id))
	return nil
}

func shellTasks(args []string) error {
	path := "/tasks"
	// Simple --status flag parsing.
	for i, a := range args {
		if a == "--status" && i+1 < len(args) {
			path += "?status=" + args[i+1]
			break
		}
	}
	resp, err := doGet(path)
	if err != nil {
		return err
	}
	var tasks []map[string]any
	if err := readJSON(resp, &tasks); err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Println("No tasks.")
		return nil
	}
	fmt.Printf("%-10s %-10s %-10s %s\n", "ID", "STATUS", "NODE", "COMMAND")
	for _, t := range tasks {
		id, _ := t["id"].(string)
		status, _ := t["status"].(string)
		worker, _ := t["worker"].(string)
		cmdSlice, _ := t["command"].([]any)
		cmdStr := ""
		if len(cmdSlice) > 0 {
			first, _ := cmdSlice[0].(string)
			cmdStr = first
		}
		node := shortID(worker)
		if node == "" {
			node = "--"
		}
		fmt.Printf("%-10s %-10s %-10s %s\n", shortID(id), status, node, cmdStr)
	}
	return nil
}

func shellStatus(args []string) error {
	resp, err := doGet("/cluster")
	if err != nil {
		return err
	}
	var data map[string]any
	if err := readJSON(resp, &data); err != nil {
		return err
	}
	status, _ := data["status"].(string)
	nodes := intVal(data, "nodes")
	running := intVal(data, "tasks_running")
	queued := intVal(data, "tasks_queued")
	completed := intVal(data, "tasks_completed")
	storeObjects := intVal(data, "storage_objects")
	storeUsed := intVal(data, "storage_used_bytes")

	fmt.Printf("Status: %s  Nodes: %d  Tasks: %d running, %d queued, %d completed  Storage: %s (%d objects)\n",
		status, nodes, running, queued, completed, formatBytes(int64(storeUsed)), storeObjects)
	return nil
}

func shellNodes(args []string) error {
	resp, err := doGet("/nodes")
	if err != nil {
		return err
	}
	var nodes []map[string]any
	if err := readJSON(resp, &nodes); err != nil {
		return err
	}
	if len(nodes) == 0 {
		fmt.Println("No nodes.")
		return nil
	}
	fmt.Printf("%-10s %-15s %-10s %s\n", "ID", "NAME", "ROLE", "ADDRESS")
	for _, n := range nodes {
		id, _ := n["id"].(string)
		name, _ := n["name"].(string)
		role, _ := n["role"].(string)
		addr, _ := n["address"].(string)
		fmt.Printf("%-10s %-15s %-10s %s\n", shortID(id), name, role, addr)
	}
	return nil
}

func shellTop(args []string) error {
	snap := fetchTop()
	if snap.err != nil {
		return snap.err
	}
	renderTop(snap, false)
	return nil
}

// splitArgs splits a command line respecting simple quoting.
func splitArgs(line string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(line); i++ {
		ch := line[i]
		if inQuote {
			if ch == quoteChar {
				inQuote = false
			} else {
				current.WriteByte(ch)
			}
		} else if ch == '"' || ch == '\'' {
			inQuote = true
			quoteChar = ch
		} else if ch == ' ' || ch == '\t' {
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		} else {
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	return args
}
