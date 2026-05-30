package certs

import (
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateCA(t *testing.T) {
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	key := filepath.Join(dir, "ca.key")

	if err := GenerateCA(ca, key); err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	// Verify files exist.
	for _, p := range []string{ca, key} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("file %s does not exist: %v", p, err)
		}
	}

	// Verify it's a valid CA.
	cert, _, err := loadCA(ca, key)
	if err != nil {
		t.Fatalf("loadCA: %v", err)
	}
	if !cert.IsCA {
		t.Error("generated cert should be a CA")
	}
}

func TestGenerateNodeCert(t *testing.T) {
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	cakey := filepath.Join(dir, "ca.key")
	nodeCert := filepath.Join(dir, "node.crt")
	nodeKey := filepath.Join(dir, "node.key")

	if err := GenerateCA(ca, cakey); err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	if err := GenerateNodeCert(ca, cakey, nodeCert, nodeKey, "test-node", []string{"localhost", "127.0.0.1"}); err != nil {
		t.Fatalf("GenerateNodeCert: %v", err)
	}

	// Verify the node cert is signed by the CA.
	caCert, _, err := loadCA(ca, cakey)
	if err != nil {
		t.Fatalf("loadCA: %v", err)
	}
	cfg, err := LoadTLSConfig(nodeCert, nodeKey, ca, false)
	if err != nil {
		t.Fatalf("LoadTLSConfig: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatal("expected 1 certificate")
	}
	cert, err := x509.ParseCertificate(cfg.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatalf("parse node cert: %v", err)
	}
	if err := cert.CheckSignatureFrom(caCert); err != nil {
		t.Fatalf("node cert not signed by CA: %v", err)
	}
}
