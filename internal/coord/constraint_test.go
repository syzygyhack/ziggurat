package coord

import (
	"testing"
)

func TestParseConstraint(t *testing.T) {
	tests := []struct {
		expr    string
		wantKey string
		wantOp  string
		wantVal string
		wantErr bool
	}{
		{"gpu.count >= 1", "gpu.count", ">=", "1", false},
		{"os == linux", "os", "==", "linux", false},
		{"mem.total >= 32GB", "mem.total", ">=", "32GB", false},
		{"python.version >= 3.11", "python.version", ">=", "3.11", false},
		{"site != building-7", "site", "!=", "building-7", false},
		{"gpu.vram > 16GB", "gpu.vram", ">", "16GB", false},
		{"cpu.cores <= 8", "cpu.cores", "<=", "8", false},
		{"arch < z", "arch", "<", "z", false},
		// Errors
		{"", "", "", "", true},
		{"gpu.count", "", "", "", true},
		{"gpu.count >=", "", "", "", true},
		{"gpu.count ~= 1", "", "", "", true},
	}

	for _, tt := range tests {
		c, err := ParseConstraint(tt.expr)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseConstraint(%q): expected error", tt.expr)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseConstraint(%q): unexpected error: %v", tt.expr, err)
			continue
		}
		if c.Key != tt.wantKey || c.Op != tt.wantOp || c.Value != tt.wantVal {
			t.Errorf("ParseConstraint(%q) = {%s %s %s}, want {%s %s %s}",
				tt.expr, c.Key, c.Op, c.Value, tt.wantKey, tt.wantOp, tt.wantVal)
		}
	}
}

func TestEvalConstraint_IntComparisons(t *testing.T) {
	caps := map[string]string{
		"gpu.count": "4",
		"cpu.cores": "16",
		"mem.total": "34359738368", // 32 GB
		"gpu.vram":  "85899345920", // 80 GB
	}

	tests := []struct {
		expr string
		want bool
	}{
		{"gpu.count >= 1", true},
		{"gpu.count >= 4", true},
		{"gpu.count >= 5", false},
		{"gpu.count == 4", true},
		{"gpu.count != 4", false},
		{"gpu.count != 3", true},
		{"gpu.count > 3", true},
		{"gpu.count > 4", false},
		{"gpu.count < 5", true},
		{"gpu.count < 4", false},
		{"gpu.count <= 4", true},
		{"gpu.count <= 3", false},
		{"cpu.cores >= 8", true},
		// Byte suffix parsing
		{"mem.total >= 32GB", true},
		{"mem.total >= 64GB", false},
		{"gpu.vram >= 80GB", true},
		{"gpu.vram >= 81GB", false},
		// Missing key
		{"gpu.tensor_cores >= 1", false},
	}

	for _, tt := range tests {
		c, err := ParseConstraint(tt.expr)
		if err != nil {
			t.Fatalf("ParseConstraint(%q): %v", tt.expr, err)
		}
		got := EvalConstraint(c, caps)
		if got != tt.want {
			t.Errorf("EvalConstraint(%q, caps) = %v, want %v", tt.expr, got, tt.want)
		}
	}
}

func TestEvalConstraint_StringComparisons(t *testing.T) {
	caps := map[string]string{
		"os":             "linux",
		"arch":           "amd64",
		"site":           "building-7",
		"python.version": "3.12",
	}

	tests := []struct {
		expr string
		want bool
	}{
		{"os == linux", true},
		{"os == darwin", false},
		{"os != windows", true},
		{"arch == amd64", true},
		{"site == building-7", true},
		{"site != building-7", false},
	}

	for _, tt := range tests {
		c, err := ParseConstraint(tt.expr)
		if err != nil {
			t.Fatalf("ParseConstraint(%q): %v", tt.expr, err)
		}
		got := EvalConstraint(c, caps)
		if got != tt.want {
			t.Errorf("EvalConstraint(%q, caps) = %v, want %v", tt.expr, got, tt.want)
		}
	}
}

func TestEvalConstraint_VersionComparisons(t *testing.T) {
	caps := map[string]string{
		"python.version": "3.12",
		"cuda.version":   "12.4",
		"julia.version":  "1.10.2",
	}

	tests := []struct {
		expr string
		want bool
	}{
		{"python.version >= 3.11", true},
		{"python.version >= 3.12", true},
		{"python.version >= 3.13", false},
		{"python.version > 3.11", true},
		{"python.version > 3.12", false},
		{"python.version < 3.13", true},
		{"python.version <= 3.12", true},
		{"cuda.version >= 12.0", true},
		{"cuda.version >= 13.0", false},
		{"cuda.version >= 12.4", true},
		{"cuda.version >= 12.5", false},
		{"julia.version >= 1.10", true},
		{"julia.version >= 1.10.2", true},
		{"julia.version >= 1.10.3", false},
		{"julia.version >= 1.9", true},
		{"julia.version >= 2.0", false},
	}

	for _, tt := range tests {
		c, err := ParseConstraint(tt.expr)
		if err != nil {
			t.Fatalf("ParseConstraint(%q): %v", tt.expr, err)
		}
		got := EvalConstraint(c, caps)
		if got != tt.want {
			t.Errorf("EvalConstraint(%q, caps) = %v, want %v", tt.expr, got, tt.want)
		}
	}
}

func TestMatchesConstraints(t *testing.T) {
	caps := map[string]string{
		"os":        "linux",
		"gpu.count": "2",
		"gpu.vram":  "34359738368", // 32GB
	}

	// All pass.
	ok := MatchesConstraints([]string{
		"os == linux",
		"gpu.count >= 1",
		"gpu.vram >= 16GB",
	}, caps)
	if !ok {
		t.Error("expected all constraints to pass")
	}

	// One fails.
	ok = MatchesConstraints([]string{
		"os == linux",
		"gpu.count >= 4", // only have 2
	}, caps)
	if ok {
		t.Error("expected constraint failure (gpu.count >= 4)")
	}

	// Empty constraints always match.
	ok = MatchesConstraints(nil, caps)
	if !ok {
		t.Error("nil constraints should match")
	}
}
