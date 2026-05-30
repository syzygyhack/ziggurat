package api

import (
	"net/http"
	"strings"
)

// BearerTokenAuth returns middleware that requires a valid bearer token.
// If token is empty, authentication is disabled (all requests pass).
func BearerTokenAuth(token string) func(http.Handler) http.Handler {
	if token == "" {
		return func(next http.Handler) http.Handler { return next }
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") != token {
				writeError(w, http.StatusUnauthorized, "invalid or missing bearer token")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
