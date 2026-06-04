package certs

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadOrGenerateCerts_HonorsCertsDir verifies certificates are written to
// the directory the caller provides (security.tls.certs_dir), not a fixed path.
func TestLoadOrGenerateCerts_HonorsCertsDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "custom-certs")
	paths, err := LoadOrGenerateCerts(dir, "node-1", true, nil)
	if err != nil {
		t.Fatalf("LoadOrGenerateCerts: %v", err)
	}
	for _, p := range []string{paths.CACert, paths.CAKey, paths.Cert, paths.Key} {
		if filepath.Dir(p) != dir {
			t.Errorf("cert %q not under requested dir %q", p, dir)
		}
		if _, err := os.Stat(p); err != nil {
			t.Errorf("expected %q to exist: %v", p, err)
		}
	}
}
