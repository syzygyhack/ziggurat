package util

import "strings"

// ParseCUDAVersion extracts the CUDA version string from nvidia-smi output.
// Returns the version (e.g. "12.4") or "" if not found.
func ParseCUDAVersion(output string) string {
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "release") {
			idx := strings.Index(line, "release ")
			if idx < 0 {
				continue
			}
			rest := line[idx+len("release "):]
			if comma := strings.Index(rest, ","); comma > 0 {
				return strings.TrimSpace(rest[:comma])
			}
			return strings.TrimSpace(rest)
		}
	}
	return ""
}
