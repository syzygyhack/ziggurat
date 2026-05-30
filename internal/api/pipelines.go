package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/util"
)

type submitPipelineRequest struct {
	Name   string        `json:"name"`
	Stages []model.Stage `json:"stages"`
}

func (s *Server) submitPipeline(w http.ResponseWriter, r *http.Request) {
	if !s.requireCoordinator(w) {
		return
	}
	if s.pipelines == nil {
		writeError(w, http.StatusServiceUnavailable, "pipeline manager not initialized")
		return
	}

	var req submitPipelineRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxControlPlaneBody)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if len(req.Stages) == 0 {
		writeError(w, http.StatusBadRequest, "pipeline must have at least one stage")
		return
	}
	for i, stage := range req.Stages {
		if err := util.ValidateNoOCIImage(stage.Image); err != nil {
			writeError(w, http.StatusNotImplemented, fmt.Sprintf("stage[%d]: %s", i, err.Error()))
			return
		}
	}

	p := &model.Pipeline{
		Name:   req.Name,
		Stages: req.Stages,
	}

	result, err := s.pipelines.SubmitPipeline(r.Context(), p)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {
	if s.pipelines == nil {
		writeError(w, http.StatusServiceUnavailable, "pipeline manager not initialized")
		return
	}

	pipelines := s.pipelines.ListPipelines()
	writeJSON(w, http.StatusOK, pipelines)
}

func (s *Server) getPipeline(w http.ResponseWriter, r *http.Request) {
	if s.pipelines == nil {
		writeError(w, http.StatusServiceUnavailable, "pipeline manager not initialized")
		return
	}

	id := chi.URLParam(r, "id")
	p, err := s.pipelines.GetPipeline(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) retryPipeline(w http.ResponseWriter, r *http.Request) {
	if s.pipelines == nil {
		writeError(w, http.StatusServiceUnavailable, "pipeline manager not initialized")
		return
	}

	id := chi.URLParam(r, "id")
	p, err := s.pipelines.RetryPipeline(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) cancelPipeline(w http.ResponseWriter, r *http.Request) {
	if s.pipelines == nil {
		writeError(w, http.StatusServiceUnavailable, "pipeline manager not initialized")
		return
	}

	id := chi.URLParam(r, "id")
	p, err := s.pipelines.CancelPipeline(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, p)
}
