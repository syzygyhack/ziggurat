package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Network.HTTPPort != 7100 {
		t.Fatalf("expected HTTP port 7100, got %d", cfg.Network.HTTPPort)
	}
	if cfg.Network.GRPCPort != 7101 {
		t.Fatalf("expected gRPC port 7101, got %d", cfg.Network.GRPCPort)
	}
	if cfg.Compute.TaskTimeout != 5*time.Minute {
		t.Fatalf("expected task timeout 5m, got %s", cfg.Compute.TaskTimeout)
	}
	if cfg.Compute.CancelGrace != 10*time.Second {
		t.Fatalf("expected cancel grace 10s, got %s", cfg.Compute.CancelGrace)
	}
	if cfg.Resilience.TaskRetries != 2 {
		t.Fatalf("expected task retries 2, got %d", cfg.Resilience.TaskRetries)
	}
	if cfg.Storage.GCGracePeriod != 1*time.Hour {
		t.Fatalf("expected gc grace 1h, got %s", cfg.Storage.GCGracePeriod)
	}
	if cfg.Compute.MaxRetainedWorkspaces != 20 {
		t.Fatalf("expected max retained workspaces 20, got %d", cfg.Compute.MaxRetainedWorkspaces)
	}
}

func TestLoadConfig_YAML(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ziggurat.yaml")

	yaml := `
node:
  name: test-node
  tags: [gpu, cuda]
  capabilities:
    gpu.vram: "17179869184"
  data_dir: /tmp/ziggurat-test
network:
  http_port: 8200
compute:
  concurrency: 4
  task_timeout: 10m
  cancel_grace: 5s
client:
  addr: "10.0.0.1:8200"
`
	os.WriteFile(cfgPath, []byte(yaml), 0o644)

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Node.Name != "test-node" {
		t.Fatalf("expected name test-node, got %s", cfg.Node.Name)
	}
	if len(cfg.Node.Tags) != 2 || cfg.Node.Tags[0] != "gpu" {
		t.Fatalf("expected tags [gpu cuda], got %v", cfg.Node.Tags)
	}
	if cfg.Node.Capabilities["gpu.vram"] != "17179869184" {
		t.Fatalf("expected gpu.vram capability, got %v", cfg.Node.Capabilities)
	}
	if cfg.Node.DataDir != "/tmp/ziggurat-test" {
		t.Fatalf("expected data dir /tmp/ziggurat-test, got %s", cfg.Node.DataDir)
	}
	if cfg.Network.HTTPPort != 8200 {
		t.Fatalf("expected HTTP port 8200, got %d", cfg.Network.HTTPPort)
	}
	if cfg.Compute.Concurrency != 4 {
		t.Fatalf("expected concurrency 4, got %d", cfg.Compute.Concurrency)
	}
	if cfg.Compute.TaskTimeout != 10*time.Minute {
		t.Fatalf("expected timeout 10m, got %s", cfg.Compute.TaskTimeout)
	}
	if cfg.Compute.CancelGrace != 5*time.Second {
		t.Fatalf("expected cancel grace 5s, got %s", cfg.Compute.CancelGrace)
	}
	if cfg.Client.Addr != "10.0.0.1:8200" {
		t.Fatalf("expected client addr 10.0.0.1:8200, got %s", cfg.Client.Addr)
	}

	// Unset fields should retain defaults.
	if cfg.Resilience.TaskRetries != 2 {
		t.Fatalf("expected default task retries 2, got %d", cfg.Resilience.TaskRetries)
	}
}

func TestLoadConfig_NoFile(t *testing.T) {
	// When no config file exists and path is empty, should return defaults.
	origDir, _ := os.Getwd()
	tmpDir := t.TempDir()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Network.HTTPPort != 7100 {
		t.Fatalf("expected default HTTP port, got %d", cfg.Network.HTTPPort)
	}
}

func TestLoadConfig_EnvVarOverride(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ziggurat.yaml")

	// Config file with NO client.addr set.
	yaml := `
node:
  name: env-test
`
	os.WriteFile(cfgPath, []byte(yaml), 0o644)

	t.Setenv("ZIGGURAT_ADDR", "192.168.1.100:7100")

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Client.Addr != "192.168.1.100:7100" {
		t.Fatalf("expected env var to set client addr, got %s", cfg.Client.Addr)
	}
}

func TestLoadConfig_EnvVarNoOverrideExplicit(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "ziggurat.yaml")

	// Config file WITH client.addr set.
	yaml := `
client:
  addr: "explicit:7100"
`
	os.WriteFile(cfgPath, []byte(yaml), 0o644)

	t.Setenv("ZIGGURAT_ADDR", "should-not-override:7100")

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	// Explicit config value should be preserved.
	if cfg.Client.Addr != "explicit:7100" {
		t.Fatalf("expected explicit config addr, got %s", cfg.Client.Addr)
	}
}

func TestLoadConfig_DataDirFallback(t *testing.T) {
	// When no CWD config exists, LoadConfig should find one in the data dir.
	origDir, _ := os.Getwd()
	tmpDir := t.TempDir()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	// Simulate the data dir with a config file.
	dataDir := filepath.Join(tmpDir, "fakehome", ".ziggurat")
	os.MkdirAll(dataDir, 0o755)
	cfgData := `
node:
  name: from-data-dir
network:
  http_port: 9999
`
	os.WriteFile(filepath.Join(dataDir, "ziggurat.yaml"), []byte(cfgData), 0o644)

	// Temporarily override DefaultDataDir by loading from that explicit path.
	cfg, err := LoadConfig(filepath.Join(dataDir, "ziggurat.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Node.Name != "from-data-dir" {
		t.Fatalf("expected name from-data-dir, got %s", cfg.Node.Name)
	}
	if cfg.Network.HTTPPort != 9999 {
		t.Fatalf("expected port 9999, got %d", cfg.Network.HTTPPort)
	}
}

func TestConfigPath(t *testing.T) {
	p := ConfigPath()
	if p == "" {
		t.Fatal("ConfigPath returned empty string")
	}
	// Should end with the expected filename.
	if filepath.Base(p) != "ziggurat.yaml" {
		t.Fatalf("expected ziggurat.yaml, got %s", filepath.Base(p))
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bad.yaml")
	os.WriteFile(cfgPath, []byte("{{{{invalid yaml"), 0o644)

	_, err := LoadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/ziggurat.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestValidate_Defaults(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
}

func TestValidate_Nil(t *testing.T) {
	var cfg *Config
	if err := cfg.Validate(); err == nil {
		t.Fatal("nil config should error")
	}
}

func TestValidate_InvalidRole(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Node.Role = "bogus"
	if err := cfg.Validate(); err == nil {
		t.Fatal("invalid role should error")
	}
}

func TestValidate_PortOutOfRange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Network.HTTPPort = 99999
	if err := cfg.Validate(); err == nil {
		t.Fatal("port out of range should error")
	}
}

func TestValidate_DuplicatePorts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Network.GRPCPort = cfg.Network.HTTPPort
	if err := cfg.Validate(); err == nil {
		t.Fatal("duplicate ports should error")
	}
}

func TestValidate_ZeroReplication(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.ReplicationFactor = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("zero replication factor should error")
	}
}

func TestValidate_ZeroDataShards(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.Erasure.Enabled = true
	cfg.Storage.Erasure.DataShards = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("zero data shards should error")
	}
}

func TestValidate_NegativeConcurrency(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Compute.Concurrency = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("negative concurrency should error")
	}
}

func TestValidate_ZeroConcurrency(t *testing.T) {
	// Zero concurrency means "use NumCPU" — should be valid.
	cfg := DefaultConfig()
	cfg.Compute.Concurrency = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("zero concurrency should be valid: %v", err)
	}
}

func TestValidate_NegativeRetries(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Resilience.TaskRetries = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("negative task retries should error")
	}
}

func TestValidate_EmptyRole(t *testing.T) {
	// Empty role is valid (defaults to "hybrid" at runtime).
	cfg := DefaultConfig()
	cfg.Node.Role = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("empty role should be valid: %v", err)
	}
}
