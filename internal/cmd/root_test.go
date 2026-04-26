package cmd

import "testing"

func TestNewRootCmd_SubcommandsRegistered(t *testing.T) {
	root := NewRootCmd("test", "abc123")

	// All documented user-facing subcommands should be registered.
	expected := []string{
		"init", "start",
		"run", "tasks", "task", "cancel", "wait", "dead-letter", "batch", "pipeline",
		"put", "get", "ls", "rm",
		"status", "nodes", "node", "drain", "resume", "version",
		"env", "shell", "mount",
		"benchmark", "top",
	}

	registered := make(map[string]bool)
	for _, cmd := range root.Commands() {
		registered[cmd.Name()] = true
	}

	for _, name := range expected {
		if !registered[name] {
			t.Errorf("expected subcommand %q not found on root", name)
		}
	}
}

func TestNewRootCmd_PersistentFlags(t *testing.T) {
	root := NewRootCmd("test", "abc")

	// Verify persistent flags exist.
	for _, flag := range []string{"config", "addr", "json"} {
		if root.PersistentFlags().Lookup(flag) == nil {
			t.Errorf("expected persistent flag %q not found", flag)
		}
	}
}

func TestNewRootCmd_Version(t *testing.T) {
	root := NewRootCmd("1.2.3", "deadbeef")

	// Find the version subcommand.
	var versionCmd = root
	for _, cmd := range root.Commands() {
		if cmd.Name() == "version" {
			versionCmd = cmd
			break
		}
	}
	if versionCmd == root {
		t.Fatal("version subcommand not found")
	}
}
