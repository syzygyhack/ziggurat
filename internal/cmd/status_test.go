package cmd

import (
	"testing"
	"time"

	"github.com/syzygyhack/ziggurat/internal/util"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		{"1024", 1024, false},
		{"4GB", 4 << 30, false},
		{"4gb", 4 << 30, false},
		{"512MB", 512 << 20, false},
		{"16KB", 16 << 10, false},
		{"2TB", 2 << 40, false},
		{"0", 0, false},
		{"", 0, true},
		{"abc", 0, true},
		{"  100  ", 100, false},
		{" 8 GB ", 8 << 30, false},
	}
	for _, tt := range tests {
		got, err := util.ParseByteSize(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("util.ParseByteSize(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if err == nil && got != tt.want {
			t.Errorf("util.ParseByteSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1 << 20, "1.0 MB"},
		{1<<30 + 1<<29, "1.5 GB"},
		{1 << 40, "1.0 TB"},
		{2<<40 + 1<<39, "2.5 TB"},
	}
	for _, tt := range tests {
		if got := formatBytes(tt.input); got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{0, "0s"},
		{30 * time.Second, "30s"},
		{59 * time.Second, "59s"},
		{5 * time.Minute, "5m"},
		{90 * time.Minute, "1h 30m"},
		{2 * time.Hour, "2h"},
		{25 * time.Hour, "1d 1h"},
		{48 * time.Hour, "2d"},
		{49 * time.Hour, "2d 1h"},
	}
	for _, tt := range tests {
		if got := formatDuration(tt.input); got != tt.want {
			t.Errorf("formatDuration(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIntVal(t *testing.T) {
	m := map[string]any{
		"float":   float64(42),
		"int":     int(7),
		"string":  "nope",
		"missing": nil,
	}
	tests := []struct {
		key  string
		want int
	}{
		{"float", 42},
		{"int", 7},
		{"string", 0},
		{"missing", 0},
		{"absent", 0},
	}
	for _, tt := range tests {
		if got := intVal(m, tt.key); got != tt.want {
			t.Errorf("intVal(m, %q) = %d, want %d", tt.key, got, tt.want)
		}
	}
}

func TestIntFromAny(t *testing.T) {
	tests := []struct {
		input any
		want  int
	}{
		{float64(42), 42},
		{int(7), 7},
		{"string", 0},
		{nil, 0},
		{true, 0},
	}
	for _, tt := range tests {
		if got := intFromAny(tt.input); got != tt.want {
			t.Errorf("intFromAny(%v) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
