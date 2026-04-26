package cmd

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
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
