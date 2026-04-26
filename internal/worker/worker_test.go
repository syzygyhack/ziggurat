package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTruncate(t *testing.T) {
	tests := []struct {
		input  string
		max    int
		want   string
		suffix bool // expect truncation suffix
	}{
		{"short", 100, "short", false},
		{"", 10, "", false},
		{"hello world", 5, "hello\n... (truncated)", true},
		{"exact", 5, "exact", false},
		{"abcdef", 5, "abcde\n... (truncated)", true},
	}
	for _, tt := range tests {
		got := truncate(tt.input, tt.max)
		if tt.suffix {
			if !strings.HasSuffix(got, "\n... (truncated)") {
				t.Errorf("truncate(%q, %d) = %q, expected truncation suffix", tt.input, tt.max, got)
			}
			// Verify the prefix is correct.
			prefix := got[:tt.max]
			if prefix != tt.input[:tt.max] {
				t.Errorf("truncate(%q, %d) prefix = %q, want %q", tt.input, tt.max, prefix, tt.input[:tt.max])
			}
		} else if got != tt.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.max, got, tt.want)
		}
	}
}

func TestDirSize(t *testing.T) {
	dir := t.TempDir()

	// Empty dir = 0.
	size, err := dirSize(dir)
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Errorf("empty dir size = %d, want 0", size)
	}

	// Create some files.
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(dir, "b.txt"), []byte("world!"), 0o644)

	size, err = dirSize(dir)
	if err != nil {
		t.Fatal(err)
	}
	if size != 11 {
		t.Errorf("dir size = %d, want 11", size)
	}

	// Nested files should be counted.
	sub := filepath.Join(dir, "sub")
	os.MkdirAll(sub, 0o755)
	os.WriteFile(filepath.Join(sub, "c.txt"), []byte("abc"), 0o644)

	size, err = dirSize(dir)
	if err != nil {
		t.Fatal(err)
	}
	if size != 14 {
		t.Errorf("dir size with subdirs = %d, want 14", size)
	}
}

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()

	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	content := []byte("test content for copy")
	if err := os.WriteFile(src, content, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := copyFile(src, dst, 0o644); err != nil {
		t.Fatalf("copyFile: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) {
		t.Errorf("copied content = %q, want %q", got, content)
	}
}

func TestApplyEnvPath(t *testing.T) {
	env := []string{
		"HOME=/home/user",
		"PATH=/usr/bin:/usr/local/bin",
		"ZIGGURAT_WORKSPACE=/ws",
	}

	result := applyEnvPath(env, "/data/envs/myenv")

	// PATH should have env bin dir prepended.
	var pathVal string
	var hasEnv, hasEnvName, hasVirtualEnv bool
	for _, e := range result {
		k, v, _ := strings.Cut(e, "=")
		switch k {
		case "PATH":
			pathVal = v
		case "ZIGGURAT_ENV":
			hasEnv = true
			if v != "/data/envs/myenv" {
				t.Errorf("ZIGGURAT_ENV = %q, want /data/envs/myenv", v)
			}
		case "ZIGGURAT_ENV_NAME":
			hasEnvName = true
			if v != "myenv" {
				t.Errorf("ZIGGURAT_ENV_NAME = %q, want myenv", v)
			}
		case "VIRTUAL_ENV":
			hasVirtualEnv = true
			if v != "/data/envs/myenv" {
				t.Errorf("VIRTUAL_ENV = %q, want /data/envs/myenv", v)
			}
		}
	}

	expectedBin := filepath.Join("/data/envs/myenv", "bin")
	if !strings.HasPrefix(pathVal, expectedBin) {
		t.Errorf("PATH should start with %q, got %q", expectedBin, pathVal)
	}
	if !hasEnv {
		t.Error("missing ZIGGURAT_ENV in result")
	}
	if !hasEnvName {
		t.Error("missing ZIGGURAT_ENV_NAME in result")
	}
	if !hasVirtualEnv {
		t.Error("missing VIRTUAL_ENV in result")
	}
}

func TestSanitizeName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"path/sep", "path_sep"},
		{"back\\slash", "back_slash"},
		{"double..dot", "double_dot"},
		{"has space", "has_space"},
		{"combo/a\\b..c d", "combo_a_b_c_d"},
	}
	for _, tt := range tests {
		if got := sanitizeName(tt.input); got != tt.want {
			t.Errorf("sanitizeName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestEnvDir(t *testing.T) {
	got := EnvDir("/data/ziggurat")
	want := filepath.Join("/data/ziggurat", "envs")
	if got != want {
		t.Errorf("EnvDir = %q, want %q", got, want)
	}
}

func TestPruneEnvs(t *testing.T) {
	dir := t.TempDir()
	envRoot := EnvDir(dir)
	os.MkdirAll(envRoot, 0o755)

	// Create 5 env dirs.
	for i := 0; i < 5; i++ {
		envPath := filepath.Join(envRoot, filepath.Base(t.Name())+string(rune('a'+i)))
		os.MkdirAll(envPath, 0o755)
		os.WriteFile(filepath.Join(envPath, envFingerprintFile), []byte("fp"), 0o644)
	}

	// Prune to max 3.
	removed := PruneEnvs(dir, 0, 3)
	if removed != 2 {
		t.Errorf("PruneEnvs removed %d, want 2", removed)
	}

	// Verify 3 remain.
	entries, _ := os.ReadDir(envRoot)
	dirs := 0
	for _, e := range entries {
		if e.IsDir() {
			dirs++
		}
	}
	if dirs != 3 {
		t.Errorf("after prune: %d dirs remain, want 3", dirs)
	}
}

func TestListEnvs(t *testing.T) {
	dir := t.TempDir()
	envRoot := EnvDir(dir)
	os.MkdirAll(envRoot, 0o755)

	// No envs yet.
	if envs := ListEnvs(dir); len(envs) != 0 {
		t.Errorf("expected 0 envs, got %d", len(envs))
	}

	// Create an env.
	envPath := filepath.Join(envRoot, "test-env")
	os.MkdirAll(envPath, 0o755)
	os.WriteFile(filepath.Join(envPath, envFingerprintFile), []byte("abc123"), 0o644)
	os.WriteFile(filepath.Join(envPath, "file.txt"), []byte("data"), 0o644)

	envs := ListEnvs(dir)
	if len(envs) != 1 {
		t.Fatalf("expected 1 env, got %d", len(envs))
	}
	if envs[0].Name != "test-env" {
		t.Errorf("env name = %q, want test-env", envs[0].Name)
	}
	if envs[0].Fingerprint != "abc123" {
		t.Errorf("fingerprint = %q, want abc123", envs[0].Fingerprint)
	}
	if envs[0].SizeBytes == 0 {
		t.Error("expected non-zero size")
	}
}
