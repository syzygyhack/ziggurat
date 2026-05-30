package util

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ParseByteSize parses a human-readable byte size string with optional suffix
// (TB, GB, MB, KB). Returns the value in bytes, or an error if parsing fails.
//
// Examples: "10GB" → 10737418240, "512MB" → 536870912, "1024" → 1024
func ParseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}

	multiplier := int64(1)
	upper := strings.ToUpper(s)
	for _, suffix := range []struct {
		s string
		m int64
	}{
		{"TB", 1 << 40},
		{"GB", 1 << 30},
		{"MB", 1 << 20},
		{"KB", 1 << 10},
	} {
		if strings.HasSuffix(upper, suffix.s) {
			multiplier = suffix.m
			s = strings.TrimSpace(s[:len(s)-len(suffix.s)])
			break
		}
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	if multiplier > 1 && n > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("size overflow: %q", s)
	}
	return n * multiplier, nil
}
