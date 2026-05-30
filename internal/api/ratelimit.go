package api

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// RateLimiter implements a per-IP token bucket rate limiter.
// It is safe for concurrent use.
type RateLimiter struct {
	rate  float64 // tokens added per second
	burst int     // maximum bucket size

	mu    sync.Mutex
	peers map[string]*tokenBucket
}

// tokenBucket tracks tokens for a single peer.
type tokenBucket struct {
	tokens   float64
	lastSeen time.Time
}

// NewRateLimiter creates a rate limiter allowing rate requests per second
// with the given burst capacity. rate of 0 disables the limiter.
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	return &RateLimiter{
		rate:  rate,
		burst: burst,
		peers: make(map[string]*tokenBucket),
	}
}

// Allow reports whether a request from addr should be permitted.
// If the limiter is disabled (rate == 0), it always returns true.
func (rl *RateLimiter) Allow(addr string) bool {
	if rl.rate == 0 {
		return true
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, ok := rl.peers[addr]
	if !ok {
		b = &tokenBucket{tokens: float64(rl.burst), lastSeen: now}
		rl.peers[addr] = b
	}

	// Refill tokens based on elapsed time.
	elapsed := now.Sub(b.lastSeen).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > float64(rl.burst) {
		b.tokens = float64(rl.burst)
	}
	b.lastSeen = now

	if b.tokens >= 1.0 {
		b.tokens--
		return true
	}
	return false
}

// Middleware returns an HTTP middleware that rate-limits requests per IP.
// It uses r.RemoteAddr directly — this middleware must be installed AFTER
// chi's middleware.RealIP, which sets RemoteAddr from trusted proxy headers.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := stripPort(r.RemoteAddr)
		if !rl.Allow(ip) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Cleanup removes expired buckets. Call periodically to prevent memory
// leaks from departed clients. age is the max idle time before eviction.
func (rl *RateLimiter) Cleanup(age time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	cutoff := time.Now().Add(-age)
	for addr, b := range rl.peers {
		if b.lastSeen.Before(cutoff) {
			delete(rl.peers, addr)
		}
	}
}

// stripPort removes the port from an address string like "1.2.3.4:12345"
// or "[::1]:12345". Returns the addr unchanged if no port is found.
func stripPort(addr string) string {
	// IPv6 with brackets: [::1]:port
	if len(addr) > 0 && addr[0] == '[' {
		for i := len(addr) - 1; i >= 0; i-- {
			if addr[i] == ']' {
				return addr[1:i]
			}
		}
		return addr
	}
	// IPv4 or hostname: addr:port
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i]
		}
	}
	return addr
}

// BackgroundCleanup starts a goroutine that periodically cleans up
// expired rate limiter buckets. Call during server startup; the
// goroutine exits when ctx is cancelled.
func (rl *RateLimiter) BackgroundCleanup(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rl.Cleanup(interval * 2)
			}
		}
	}()
}
