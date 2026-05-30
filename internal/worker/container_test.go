package worker

import (
	"os"
	"strings"
	"testing"
)

func TestDetectRuntime(t *testing.T) {
	rt := detectRuntime()
	// On CI or developer machines, we can't assume a runtime is present.
	// The function should return a valid binary name or empty string.
	switch rt {
	case "", "podman", "docker":
		// All valid return values.
	default:
		t.Errorf("unexpected runtime: %q", rt)
	}
}

func TestWriteEnvFile(t *testing.T) {
	env := []string{"KEY1=val1", "KEY2=val with spaces", "KEY3=val3"}
	path, err := writeEnvFile(env)
	if err != nil {
		t.Fatalf("writeEnvFile: %v", err)
	}
	defer os.Remove(path)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read env file: %v", err)
	}

	content := string(data)
	for _, e := range env {
		if !strings.Contains(content, e) {
			t.Errorf("env file missing %q in:\n%s", e, content)
		}
	}
}
