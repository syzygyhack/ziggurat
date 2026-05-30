package api

import (
	"encoding/json"
	"net/http"
)

// maxControlPlaneBody caps JSON request bodies on task/pipeline submission
// endpoints to prevent memory exhaustion from oversized payloads. 1 MiB is
// generous for any reasonable task or pipeline definition.
const maxControlPlaneBody = 1 << 20 // 1 MiB

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
