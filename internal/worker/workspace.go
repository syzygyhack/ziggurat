package worker

import (
	"os"
	"path/filepath"
	"sort"
)

// EnforceWorkspaceLimit removes the oldest workspace directories until
// the count is at or below max. A max of 0 disables eviction.
func EnforceWorkspaceLimit(wsDir string, max int) {
	if max <= 0 {
		return
	}

	entries, err := os.ReadDir(wsDir)
	if err != nil {
		return
	}

	// Filter to directories only (workspaces are dirs).
	type dirEntry struct {
		name    string
		modTime int64
	}
	var dirs []dirEntry
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		dirs = append(dirs, dirEntry{name: e.Name(), modTime: info.ModTime().UnixNano()})
	}

	if len(dirs) <= max {
		return
	}

	// Sort oldest first.
	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i].modTime < dirs[j].modTime
	})

	// Remove excess (oldest first).
	excess := len(dirs) - max
	for i := 0; i < excess; i++ {
		os.RemoveAll(filepath.Join(wsDir, dirs[i].name))
	}
}
