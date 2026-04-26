package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/syzygyhack/ziggurat/internal/store"
)

func (s *Server) putObject(w http.ResponseWriter, r *http.Request) {
	key := extractStoreKey(r)
	if key == "" {
		writeError(w, http.StatusBadRequest, "namespace key is required")
		return
	}

	body := r.Body
	if s.maxUploadSize > 0 {
		body = http.MaxBytesReader(w, r.Body, s.maxUploadSize)
	}

	hash, err := s.store.Put(r.Context(), key, body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("upload exceeds maximum size (%d bytes)", maxErr.Limit))
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"key":  key,
		"hash": hash,
	})
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request) {
	// Check for list mode.
	prefix := r.URL.Query().Get("prefix")
	if prefix != "" || r.URL.Path == "/api/v1/store" || r.URL.Path == "/api/v1/store/" {
		keys, err := s.store.List(r.Context(), prefix)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, keys)
		return
	}

	key := extractStoreKey(r)
	if key == "" {
		writeError(w, http.StatusBadRequest, "namespace key is required")
		return
	}

	// Direct hash access: GET /store/@hash/<hex> bypasses namespace
	// resolution and retrieves the raw object by content hash. Used to
	// fetch task output_ref values which are content hashes.
	var rc io.ReadCloser
	var err error
	if strings.HasPrefix(key, "@hash/") {
		hash := strings.TrimPrefix(key, "@hash/")
		if err := store.ValidateHashHex(hash); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid hash: %v", err))
			return
		}
		rc, err = s.store.GetByHash(r.Context(), hash)
	} else {
		rc, err = s.store.Get(r.Context(), key)
	}
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Stream content to client. The verifyingReader hashes data as it
	// flows through Read(); Close() checks the BLAKE3 digest.
	w.Header().Set("Content-Type", "application/octet-stream")
	if _, err := io.Copy(w, rc); err != nil {
		// Headers already sent; cannot write an error response.
		rc.Close()
		return
	}
	if err := rc.Close(); err != nil {
		// Integrity failure after data was already sent. Client received
		// potentially corrupt data -- log but can't un-send it.
		s.log.Warn("object integrity check failed after streaming", "key", key, "err", err)
	}
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	key := extractStoreKey(r)
	if key == "" {
		writeError(w, http.StatusBadRequest, "namespace key is required")
		return
	}

	if err := s.store.Delete(r.Context(), key); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"deleted": key})
}

func extractStoreKey(r *http.Request) string {
	// Route is /api/v1/store/*, chi gives us the wildcard.
	path := r.URL.Path
	prefix := "/api/v1/store/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	return strings.TrimPrefix(path, prefix)
}
