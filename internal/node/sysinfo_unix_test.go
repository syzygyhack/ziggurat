//go:build !windows

package node

import (
	"math"
	"strconv"
	"testing"
)

func TestParseCgroupV2CPUMax(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"max 100000", 0},      // unlimited
		{"50000 100000", 1},    // 0.5 core -> round up to 1
		{"150000 100000", 2},   // 1.5 -> 2
		{"800000 100000", 8},   // exactly 8
		{"250000 100000\n", 3}, // 2.5 -> 3, trailing newline
		{"garbage", 0},
		{"", 0},
	}
	for _, c := range cases {
		if got := parseCgroupV2CPUMax(c.in); got != c.want {
			t.Errorf("parseCgroupV2CPUMax(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestParseCgroupV1CPU(t *testing.T) {
	if got := parseCgroupV1CPU("-1", "100000"); got != 0 {
		t.Errorf("unlimited quota -1 should give 0, got %d", got)
	}
	if got := parseCgroupV1CPU("200000", "100000"); got != 2 {
		t.Errorf("200000/100000 should give 2, got %d", got)
	}
	if got := parseCgroupV1CPU("50000", "100000"); got != 1 {
		t.Errorf("0.5 core should round up to 1, got %d", got)
	}
}

func TestParseCgroupMemLimit(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"max", 0},
		{"4294967296", 4294967296}, // 4 GiB
		{"0", 0},
		{strconv.FormatInt(math.MaxInt64, 10), 0}, // v1 unlimited sentinel
		{"9223372036854771712", 0},                // common v1 unlimited value
		{"notanumber", 0},
	}
	for _, c := range cases {
		if got := parseCgroupMemLimit(c.in); got != c.want {
			t.Errorf("parseCgroupMemLimit(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestDetectCPUCores_Positive(t *testing.T) {
	// Whatever the environment, must report at least one core.
	if got := detectCPUCores(); got < 1 {
		t.Errorf("detectCPUCores() = %d, want >= 1", got)
	}
}
