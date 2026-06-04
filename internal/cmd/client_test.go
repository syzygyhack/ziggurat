package cmd

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/syzygyhack/ziggurat/internal/api"
)

func TestShortID(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"abcdefghijklmnop", "abcdefgh"},
		{"abcdefgh", "abcdefgh"},
		{"short", "short"},
		{"", ""},
		{"12345678x", "12345678"},
	}
	for _, tt := range tests {
		if got := shortID(tt.input); got != tt.want {
			t.Errorf("shortID(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestStoreKeyPath(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"file.txt", "/store/file.txt"},
		{"data/train.csv", "/store/data/train.csv"},
		{"my file.txt", "/store/my%20file.txt"},
		{"data/file#1.txt", "/store/data/file%231.txt"},
		{"a/b/c", "/store/a/b/c"},
	}
	for _, tt := range tests {
		if got := storeKeyPath(tt.key); got != tt.want {
			t.Errorf("storeKeyPath(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestReadJSON_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"key": "value"})
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]string
	if err := readJSON(resp, &result); err != nil {
		t.Fatalf("readJSON: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("got %v, want key=value", result)
	}
}

func TestReadJSON_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": "something broke"})
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	err = readJSON(resp, nil)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if err.Error() != "server: something broke" {
		t.Errorf("got error %q, want %q", err.Error(), "server: something broke")
	}
}

func TestReadJSON_ServerErrorNoBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		w.Write([]byte("not json"))
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	err = readJSON(resp, nil)
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
	if err.Error() != "server returned 404" {
		t.Errorf("got error %q, want %q", err.Error(), "server returned 404")
	}
}

func TestReadJSON_NilTarget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	if err := readJSON(resp, nil); err != nil {
		t.Fatalf("readJSON with nil target: %v", err)
	}
}

func TestExitError(t *testing.T) {
	e := &ExitError{Code: 2, Msg: "connection refused"}
	if e.Error() != "connection refused" {
		t.Errorf("Error() = %q, want %q", e.Error(), "connection refused")
	}
	if e.Code != 2 {
		t.Errorf("Code = %d, want 2", e.Code)
	}
}

func TestWrapConnError_Nil(t *testing.T) {
	if err := wrapConnError(nil); err != nil {
		t.Errorf("wrapConnError(nil) = %v, want nil", err)
	}
}

func TestWrapConnError_ConnectionRefused(t *testing.T) {
	// Create a net.OpError that looks like connection refused.
	inner := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: errors.New("connection refused"),
	}
	err := wrapConnError(inner)
	if err == nil {
		t.Fatal("expected error")
	}
	var exitErr *ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected ExitError, got %T", err)
	}
	if exitErr.Code != 2 {
		t.Errorf("exit code = %d, want 2", exitErr.Code)
	}
}

func TestWrapConnError_Timeout(t *testing.T) {
	inner := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: &timeoutError{},
	}
	err := wrapConnError(inner)
	if err == nil {
		t.Fatal("expected error")
	}
	var exitErr *ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected ExitError, got %T", err)
	}
	if exitErr.Code != 2 {
		t.Errorf("exit code = %d, want 2", exitErr.Code)
	}
}

// timeoutError implements net.Error with Timeout() = true.
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

func TestWrapConnError_OtherError(t *testing.T) {
	other := errors.New("some random error")
	if err := wrapConnError(other); err != other {
		t.Errorf("expected original error returned unchanged")
	}
}

// emptyCfgFile writes a minimal config file so bearerToken()'s config fallback
// resolves to an empty token (and never auto-detects ~/.ziggurat).
func emptyCfgFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ziggurat.yaml")
	if err := os.WriteFile(p, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestBearerToken_Precedence(t *testing.T) {
	defer func(tf, cf string) { tokenFlag, cfgFile = tf, cf }(tokenFlag, cfgFile)
	cfgFile = emptyCfgFile(t)

	// 1. --token flag wins over the env var.
	tokenFlag = "flagtok"
	t.Setenv("ZIGGURAT_TOKEN", "envtok")
	if got := bearerToken(); got != "flagtok" {
		t.Errorf("flag precedence: got %q, want flagtok", got)
	}

	// 2. env var used when no flag.
	tokenFlag = ""
	if got := bearerToken(); got != "envtok" {
		t.Errorf("env precedence: got %q, want envtok", got)
	}

	// 3. empty when neither flag nor env set and config has none.
	t.Setenv("ZIGGURAT_TOKEN", "")
	if got := bearerToken(); got != "" {
		t.Errorf("no-token: got %q, want empty", got)
	}
}

func TestBearerToken_ConfigOnlyForLocalTarget(t *testing.T) {
	// Config file carrying the local node's API token.
	cfgPath := filepath.Join(t.TempDir(), "ziggurat.yaml")
	if err := os.WriteFile(cfgPath, []byte("security:\n  api_token: \"cfgtok\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	defer func(tf, cf, a string) { tokenFlag, cfgFile, addr = tf, cf, a }(tokenFlag, cfgFile, addr)
	tokenFlag = ""
	cfgFile = cfgPath
	t.Setenv("ZIGGURAT_TOKEN", "")

	// Local/default target (no --addr, no ZIGGURAT_ADDR): config token IS used.
	addr = ""
	t.Setenv("ZIGGURAT_ADDR", "")
	if got := bearerToken(); got != "cfgtok" {
		t.Errorf("local target: got %q, want cfgtok", got)
	}

	// Explicit --addr redirect: the local config token must NOT leak.
	addr = "remote.example:7100"
	if got := bearerToken(); got != "" {
		t.Errorf("--addr redirect leaked config token: got %q, want empty", got)
	}

	// ZIGGURAT_ADDR redirect: same protection.
	addr = ""
	t.Setenv("ZIGGURAT_ADDR", "remote.example:7100")
	if got := bearerToken(); got != "" {
		t.Errorf("ZIGGURAT_ADDR redirect leaked config token: got %q, want empty", got)
	}
}

func TestDoGet_SendsAuthHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	defer func(a, tf string) { addr, tokenFlag = a, tf }(addr, tokenFlag)
	addr = strings.TrimPrefix(srv.URL, "http://")
	tokenFlag = "sekret"

	resp, err := doGet("/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if gotAuth != "Bearer sekret" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer sekret")
	}
}

func TestDoPost_SendsAuthAndContentType(t *testing.T) {
	var gotAuth, gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(201)
	}))
	defer srv.Close()

	defer func(a, tf string) { addr, tokenFlag = a, tf }(addr, tokenFlag)
	addr = strings.TrimPrefix(srv.URL, "http://")
	tokenFlag = "ptok"

	resp, err := doPost("/tasks", map[string]string{"x": "y"})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if gotAuth != "Bearer ptok" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer ptok")
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotCT)
	}
}

// TestClientServerAuth_Interop wires the real server-side BearerTokenAuth
// middleware so the client's header and the server's check are verified against
// each other — a valid token is accepted, a wrong one is rejected (401).
func TestClientServerAuth_Interop(t *testing.T) {
	const tok = "interop-token"
	h := api.BearerTokenAuth(tok)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv := httptest.NewServer(h)
	defer srv.Close()

	defer func(a, tf string) { addr, tokenFlag = a, tf }(addr, tokenFlag)
	addr = strings.TrimPrefix(srv.URL, "http://")

	tokenFlag = tok
	resp, err := doGet("/anything")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid token: got %d, want 200", resp.StatusCode)
	}

	tokenFlag = "wrong-token"
	resp, err = doGet("/anything")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token: got %d, want 401", resp.StatusCode)
	}
}

func TestDoGet_NoToken_NoHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	defer func(a, tf, cf string) { addr, tokenFlag, cfgFile = a, tf, cf }(addr, tokenFlag, cfgFile)
	addr = strings.TrimPrefix(srv.URL, "http://")
	tokenFlag = ""
	cfgFile = emptyCfgFile(t)
	t.Setenv("ZIGGURAT_TOKEN", "")

	resp, err := doGet("/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if gotAuth != "" {
		t.Errorf("Authorization = %q, want empty (no token configured)", gotAuth)
	}
}
