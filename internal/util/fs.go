package util

import "os"

// FileExists returns true if the given path exists and is accessible.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
