//go:build !windows

package fuse

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStoreURL(t *testing.T) {
	tests := []struct {
		name    string
		apiBase string
		key     string
		want    string
	}{
		{
			name:    "simple key",
			apiBase: "http://localhost:7100",
			key:     "myfile.txt",
			want:    "http://localhost:7100/api/v1/store/myfile.txt",
		},
		{
			name:    "nested key with slashes",
			apiBase: "http://localhost:7100",
			key:     "data/experiments/run1.csv",
			want:    "http://localhost:7100/api/v1/store/data/experiments/run1.csv",
		},
		{
			name:    "key with spaces",
			apiBase: "http://localhost:7100",
			key:     "my file.txt",
			want:    "http://localhost:7100/api/v1/store/my%20file.txt",
		},
		{
			name:    "key with special chars",
			apiBase: "http://localhost:7100",
			key:     "data/file#1.txt",
			want:    "http://localhost:7100/api/v1/store/data/file%231.txt",
		},
		{
			name:    "empty key",
			apiBase: "http://localhost:7100",
			key:     "",
			want:    "http://localhost:7100/api/v1/store/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := storeURL(tt.apiBase, tt.key)
			if got != tt.want {
				t.Errorf("storeURL(%q, %q) = %q, want %q", tt.apiBase, tt.key, got, tt.want)
			}
		})
	}
}

func TestListKeys(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/store" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.Error(w, "not found", 404)
			return
		}
		prefix := r.URL.Query().Get("prefix")
		var keys []string
		switch prefix {
		case "":
			keys = []string{"a.txt", "b.txt", "data/c.txt"}
		case "data/":
			keys = []string{"data/c.txt", "data/d.txt"}
		default:
			keys = []string{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(keys)
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	z := &ZigFS{
		apiBase: srv.URL,
		client:  srv.Client(),
	}

	// No prefix — should return all keys.
	keys, err := z.listKeys("")
	if err != nil {
		t.Fatalf("listKeys(\"\"): %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("listKeys(\"\") returned %d keys, want 3", len(keys))
	}

	// With prefix.
	keys, err = z.listKeys("data/")
	if err != nil {
		t.Fatalf("listKeys(\"data/\"): %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("listKeys(\"data/\") returned %d keys, want 2", len(keys))
	}

	// Non-matching prefix.
	keys, err = z.listKeys("nope/")
	if err != nil {
		t.Fatalf("listKeys(\"nope/\"): %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("listKeys(\"nope/\") returned %d keys, want 0", len(keys))
	}
}

func TestListKeys_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", 500)
	}))
	defer srv.Close()

	z := &ZigFS{
		apiBase: srv.URL,
		client:  srv.Client(),
	}

	_, err := z.listKeys("")
	if err == nil {
		t.Error("expected error from 500 response, got nil")
	}
}

func TestFetchContent(t *testing.T) {
	content := []byte("hello ziggurat")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/store/test.txt" {
			w.Write(content)
			return
		}
		http.Error(w, "not found", 404)
	}))
	defer srv.Close()

	f := &zigFile{
		apiBase: srv.URL,
		key:     "test.txt",
		client:  srv.Client(),
	}

	data, err := f.fetchContent()
	if err != nil {
		t.Fatalf("fetchContent: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("fetchContent = %q, want %q", data, content)
	}
}

func TestFetchContent_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", 404)
	}))
	defer srv.Close()

	f := &zigFile{
		apiBase: srv.URL,
		key:     "missing.txt",
		client:  srv.Client(),
	}

	_, err := f.fetchContent()
	if err == nil {
		t.Error("expected error for 404, got nil")
	}
}

func TestContentSize(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/api/v1/store/test.txt" {
			w.Header().Set("Content-Length", "42")
			return
		}
		http.Error(w, "not found", 404)
	}))
	defer srv.Close()

	f := &zigFile{
		apiBase: srv.URL,
		key:     "test.txt",
		client:  srv.Client(),
	}

	size, err := f.contentSize()
	if err != nil {
		t.Fatalf("contentSize: %v", err)
	}
	if size != 42 {
		t.Errorf("contentSize = %d, want 42", size)
	}
}

func TestContentSize_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", 404)
	}))
	defer srv.Close()

	f := &zigFile{
		apiBase: srv.URL,
		key:     "missing.txt",
		client:  srv.Client(),
	}

	_, err := f.contentSize()
	if err == nil {
		t.Error("expected error for 404, got nil")
	}
}
