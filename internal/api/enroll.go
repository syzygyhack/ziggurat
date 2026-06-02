package api

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"time"

	"github.com/syzygyhack/ziggurat/internal/certs"
)

// enrollRequest is the JSON body for POST /api/v1/cluster/enroll.
type enrollRequest struct {
	NodeID    string   `json:"node_id"`
	CSR       string   `json:"csr"`        // PEM-encoded PKCS#10 CSR
	JoinToken string   `json:"join_token"` // shared secret
	SANs      []string `json:"sans,omitempty"`
}

// enrollResponse is returned on successful enrollment.
type enrollResponse struct {
	Cert   string `json:"cert"`    // PEM-encoded signed certificate
	CACert string `json:"ca_cert"` // PEM-encoded CA certificate
}

// SetEnrollConfig sets the CA paths and join token needed for worker
// enrollment. Call during node startup (only needed on coordinators).
func (s *Server) SetEnrollConfig(caCertPath, caKeyPath, joinToken string) {
	s.caCertPath = caCertPath
	s.caKeyPath = caKeyPath
	s.joinToken = joinToken
}

func (s *Server) handleEnroll(w http.ResponseWriter, r *http.Request) {
	if s.caCertPath == "" || s.caKeyPath == "" {
		writeError(w, http.StatusNotImplemented, "enrollment requires TLS CA; start with security.tls.enabled=true and coordinator role")
		return
	}

	var req enrollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid enrollment request: "+err.Error())
		return
	}

	// Validate join token.
	if s.joinToken != "" && req.JoinToken != s.joinToken {
		writeError(w, http.StatusForbidden, "invalid join token")
		return
	}

	// Parse the CSR.
	csrBlock, _ := pem.Decode([]byte(req.CSR))
	if csrBlock == nil || csrBlock.Type != "CERTIFICATE REQUEST" {
		writeError(w, http.StatusBadRequest, "invalid CSR: expected PEM-encoded CERTIFICATE REQUEST")
		return
	}
	csr, err := x509.ParseCertificateRequest(csrBlock.Bytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid CSR: "+err.Error())
		return
	}
	if err := csr.CheckSignature(); err != nil {
		writeError(w, http.StatusBadRequest, "CSR signature verification failed: "+err.Error())
		return
	}

	// Load CA cert and key.
	caCert, caKey, err := certs.LoadCA(s.caCertPath, s.caKeyPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load CA: "+err.Error())
		return
	}

	// Sign the CSR.
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "generate serial: "+err.Error())
		return
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      csr.Subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(5 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
	}

	// Add SANs from request.
	for _, san := range req.SANs {
		if ip := net.ParseIP(san); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, san)
		}
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, csr.PublicKey, caKey)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "sign certificate: "+err.Error())
		return
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	// Read CA cert for the response.
	caCertPEM, err := certs.ReadCertPEM(s.caCertPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read CA cert: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, enrollResponse{
		Cert:   string(certPEM),
		CACert: string(caCertPEM),
	})
}
