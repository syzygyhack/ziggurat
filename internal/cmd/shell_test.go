package cmd

import (
	"testing"
)

func TestSplitArgs(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"ls", []string{"ls"}},
		{"ls datasets/", []string{"ls", "datasets/"}},
		{"put key path/to/file", []string{"put", "key", "path/to/file"}},
		{`run "python train.py"`, []string{"run", "python train.py"}},
		{`run 'echo hello world'`, []string{"run", "echo hello world"}},
		{"  status  ", []string{"status"}},
		{"", nil},
		{"   ", nil},
		{`get "key with spaces" dest`, []string{"get", "key with spaces", "dest"}},
	}

	for _, tt := range tests {
		got := splitArgs(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("splitArgs(%q) = %v, want %v", tt.input, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitArgs(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}
