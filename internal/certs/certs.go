package certs

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// CertPaths holds the filesystem paths for TLS material.
type CertPaths struct {
	CACert string // CA certificate
	CAKey  string // CA private key
	Cert   string // Node certificate
	Key    string // Node private key
}

// DefaultDir returns the default TLS directory under the data dir.
func DefaultDir(dataDir string) string {
	return filepath.Join(dataDir, "certs")
}

// GenerateCA creates a self-signed CA certificate and private key, writing
// them to the given paths. Returns an error if the files already exist.
func GenerateCA(certPath, keyPath string) error {
	if fileExists(certPath) || fileExists(keyPath) {
		return nil // already generated
	}

	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return fmt.Errorf("generate CA key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate CA serial: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "Ziggurat CA",
			Organization: []string{"Ziggurat Cluster"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour), // 10 years
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("create CA cert: %w", err)
	}

	if err := writeCertAndKey(certPath, keyPath, certDER, key); err != nil {
		return err
	}

	return nil
}

// GenerateNodeCert creates a node certificate signed by the given CA,
// writing to the given paths. The cert is valid for TLS client and server
// authentication with the given DNS names and node ID as CommonName.
func GenerateNodeCert(caCertPath, caKeyPath, certPath, keyPath, nodeID string, sans []string) error {
	if fileExists(certPath) && fileExists(keyPath) {
		return nil // already generated
	}

	caCert, caKey, err := loadCA(caCertPath, caKeyPath)
	if err != nil {
		return fmt.Errorf("load CA: %w", err)
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("generate node key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate node serial: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   nodeID,
			Organization: []string{"Ziggurat Node"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(5 * 365 * 24 * time.Hour), // 5 years
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
	}

	// Add DNS and IP SANs.
	for _, san := range sans {
		if ip := net.ParseIP(san); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, san)
		}
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("create node cert: %w", err)
	}

	if err := writeCertAndKey(certPath, keyPath, certDER, key); err != nil {
		return err
	}

	return nil
}

// LoadTLSConfig loads the node's TLS certificate and key, and configures
// the CA certificate pool for mutual TLS. If requireClientCert is true,
// the server will require and verify client certificates.
func LoadTLSConfig(certPath, keyPath, caCertPath string, requireClientCert bool) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load cert/key: %w", err)
	}

	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}

	if requireClientCert {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = caPool
	}

	return cfg, nil
}

// HasCA returns true if both the CA certificate and private key exist.
func HasCA(caCertPath, caKeyPath string) bool {
	return fileExists(caCertPath) && fileExists(caKeyPath)
}

// LoadCA reads and parses a CA certificate and private key from disk.
// Exported for use by the enrollment endpoint.
func LoadCA(certPath, keyPath string) (*x509.Certificate, *rsa.PrivateKey, error) {
	return loadCA(certPath, keyPath)
}

// ReadCertPEM reads a PEM-encoded certificate from disk.
func ReadCertPEM(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// EnrollWorker connects to the coordinator's enrollment endpoint, submits
// a CSR with the join token, and saves the signed certificate and CA cert.
func EnrollWorker(coordAddr, joinToken, nodeID string, sans []string, certPath, keyPath, caCertPath string) error {
	// Generate a keypair for the worker.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("generate worker key: %w", err)
	}

	// Create a CSR.
	tmpl := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   nodeID,
			Organization: []string{"Ziggurat Node"},
		},
	}
	for _, san := range sans {
		if ip := net.ParseIP(san); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, san)
		}
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, tmpl, key)
	if err != nil {
		return fmt.Errorf("create CSR: %w", err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})

	// Submit enrollment request to coordinator.
	reqBody, err := json.Marshal(map[string]interface{}{
		"node_id":    nodeID,
		"csr":        string(csrPEM),
		"join_token": joinToken,
		"sans":       sans,
	})
	if err != nil {
		return fmt.Errorf("marshal enroll request: %w", err)
	}

	url := fmt.Sprintf("http://%s/api/v1/cluster/enroll", coordAddr)
	resp, err := http.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("enroll request to %s: %w", coordAddr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("enrollment failed (status %d): %s", resp.StatusCode, string(body))
	}

	var enrollResp struct {
		Cert   string `json:"cert"`
		CACert string `json:"ca_cert"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&enrollResp); err != nil {
		return fmt.Errorf("decode enroll response: %w", err)
	}

	// Save the signed cert, key, and CA cert.
	if err := os.WriteFile(certPath, []byte(enrollResp.Cert), 0o644); err != nil {
		return fmt.Errorf("write cert: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	if err := os.WriteFile(caCertPath, []byte(enrollResp.CACert), 0o644); err != nil {
		return fmt.Errorf("write CA cert: %w", err)
	}

	return nil
}

// LoadOrGenerateCerts ensures TLS certificates exist for this node.
// The coordinator generates a CA and signs its own node cert. Workers
// generate a self-signed cert if the CA is not available locally; they
// must enroll with the coordinator (via EnrollWorker) to get a CA-signed
// cert for full mTLS authentication.
func LoadOrGenerateCerts(dataDir, nodeID string, isCoordinator bool, sans []string) (CertPaths, error) {
	dir := DefaultDir(dataDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return CertPaths{}, fmt.Errorf("create certs dir: %w", err)
	}

	paths := CertPaths{
		CACert: filepath.Join(dir, "ca.crt"),
		CAKey:  filepath.Join(dir, "ca.key"),
		Cert:   filepath.Join(dir, "node.crt"),
		Key:    filepath.Join(dir, "node.key"),
	}

	if isCoordinator {
		if err := GenerateCA(paths.CACert, paths.CAKey); err != nil {
			return CertPaths{}, fmt.Errorf("generate CA: %w", err)
		}
		if err := GenerateNodeCert(paths.CACert, paths.CAKey, paths.Cert, paths.Key, nodeID, sans); err != nil {
			return CertPaths{}, fmt.Errorf("generate node cert: %w", err)
		}
		return paths, nil
	}

	// Worker: use CA-signed cert if CA is available, otherwise self-sign.
	if fileExists(paths.CACert) && fileExists(paths.CAKey) {
		if err := GenerateNodeCert(paths.CACert, paths.CAKey, paths.Cert, paths.Key, nodeID, sans); err != nil {
			return CertPaths{}, fmt.Errorf("generate node cert: %w", err)
		}
		return paths, nil
	}

	// No CA available on this worker — generate a self-signed cert.
	// Encryption works, but full mTLS auth requires enrollment with the
	// coordinator to get a CA-signed cert.
	if err := generateSelfSigned(paths.Cert, paths.Key, nodeID, sans); err != nil {
		return CertPaths{}, fmt.Errorf("generate self-signed cert: %w", err)
	}
	return paths, nil
}

// generateSelfSigned creates a self-signed certificate and key for a
// worker that doesn't yet have access to the cluster CA.
func generateSelfSigned(certPath, keyPath, nodeID string, sans []string) error {
	if fileExists(certPath) && fileExists(keyPath) {
		return nil
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate serial: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   nodeID,
			Organization: []string{"Ziggurat Node (self-signed)"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(365 * 24 * time.Hour), // 1 year
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
	}

	for _, san := range sans {
		if ip := net.ParseIP(san); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, san)
		}
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("create self-signed cert: %w", err)
	}

	return writeCertAndKey(certPath, keyPath, certDER, key)
}

// loadCA reads and parses a CA certificate and private key.
func loadCA(certPath, keyPath string) (*x509.Certificate, *rsa.PrivateKey, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read CA cert: %w", err)
	}
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, nil, fmt.Errorf("failed to parse CA cert PEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA cert: %w", err)
	}

	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read CA key: %w", err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, nil, fmt.Errorf("failed to parse CA key PEM")
	}
	key, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		// Try PKCS1.
		key, err = x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA key: %w", err)
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("CA key is not RSA")
	}
	return cert, rsaKey, nil
}

func writeCertAndKey(certPath, keyPath string, certDER []byte, key *rsa.PrivateKey) error {
	// Write certificate.
	certFile, err := os.OpenFile(certPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create cert file: %w", err)
	}
	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		certFile.Close()
		return fmt.Errorf("write cert: %w", err)
	}
	certFile.Close()

	// Write private key.
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}
	keyFile, err := os.OpenFile(keyPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create key file: %w", err)
	}
	if err := pem.Encode(keyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}); err != nil {
		keyFile.Close()
		return fmt.Errorf("write key: %w", err)
	}
	keyFile.Close()

	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
