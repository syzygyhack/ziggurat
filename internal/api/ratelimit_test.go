package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRateLimiter_Allow(t *testing.T) {
	rl := NewRateLimiter(10, 3)
	addr := "192.168.1.1"

	// Burst of 3 should all succeed.
	for i := 0; i < 3; i++ {
		if !rl.Allow(addr) {
			t.Fatalf("request %d should be allowed (within burst)", i)
		}
	}
	// Fourth should be rate-limited.
	if rl.Allow(addr) {
		t.Fatal("request 4 should be rate-limited (burst exhausted)")
	}
}

func TestRateLimiter_Disabled(t *testing.T) {
	rl := NewRateLimiter(0, 0)
	for i := 0; i < 100; i++ {
		if !rl.Allow("10.0.0.1") {
			t.Fatal("disabled rate limiter should allow all requests")
		}
	}
}

func TestRateLimiter_Refill(t *testing.T) {
	rl := NewRateLimiter(100, 1)
	addr := "10.0.0.2"

	// Use burst.
	if !rl.Allow(addr) {
		t.Fatal("first request should be allowed")
	}
	if rl.Allow(addr) {
		t.Fatal("second request should be rate-limited")
	}

	// Wait for refill.
	time.Sleep(15 * time.Millisecond) // ~1.5 tokens at 100/s
	if !rl.Allow(addr) {
		t.Fatal("request after refill should be allowed")
	}
}

func TestRateLimiter_Cleanup(t *testing.T) {
	rl := NewRateLimiter(100, 5)
	rl.Allow("10.0.0.3")

	// Cleanup with 0 age should evict all.
	rl.Cleanup(0)
	rl.mu.Lock()
	if len(rl.peers) != 0 {
		t.Fatalf("cleanup should have evicted all peers, got %d", len(rl.peers))
	}
	rl.mu.Unlock()
}

func TestRateLimiter_Middleware(t *testing.T) {
	rl := NewRateLimiter(10, 1)

	var called bool
	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	// First request: allowed.
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.4:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !called {
		t.Fatalf("first request should be allowed, got status %d", rec.Code)
	}

	// Second request: rate-limited.
	called = false
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests || called {
		t.Fatalf("second request should get 429, got status %d", rec.Code)
	}
}

func TestStripPort(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"10.0.0.1:12345", "10.0.0.1"},
		{"[::1]:12345", "::1"},
		{"192.168.1.1:8080", "192.168.1.1"},
		{"hostname:9999", "hostname"},
		{"noport", "noport"},
		{"", ""},
		{"[::1]", "::1"}, // bracket with no port — strips brackets, keeps address
	}
	for _, tt := range tests {
		if got := stripPort(tt.input); got != tt.want {
			t.Errorf("stripPort(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
