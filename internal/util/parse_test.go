package util

import "testing"

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		{"10GB", 10 << 30, false},
		{"1TB", 1 << 40, false},
		{"512MB", 512 << 20, false},
		{"1024KB", 1024 << 10, false},
		{"1024", 1024, false},
		{"0", 0, false},
		{"  1GB  ", 1 << 30, false},
		{"", 0, true},
		{"abc", 0, true},
		{"10XB", 0, true},
		{"10gb", 10 << 30, false}, // case insensitive
		{"10Gb", 10 << 30, false},
		{"1tb", 1 << 40, false},
	}
	for _, tt := range tests {
		got, err := ParseByteSize(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseByteSize(%q) expected error, got %d", tt.input, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseByteSize(%q) unexpected error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseByteSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
