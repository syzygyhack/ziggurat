package api

import (
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterRoutes mounts all API v1 routes onto the router.
func RegisterRoutes(r chi.Router, s *Server) {
	// Prometheus metrics endpoint (outside /api/v1, per spec).
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/api/v1", func(r chi.Router) {
		// Tasks
		r.Post("/tasks", s.submitTask)
		r.Post("/tasks/batch", s.submitBatch)
		r.Get("/tasks", s.listTasks)
		r.Get("/tasks/{id}", s.getTask)
		r.Delete("/tasks/{id}", s.cancelTask)
		r.Post("/tasks/{id}/wait", s.waitTask)

		// Pipelines
		r.Post("/pipelines", s.submitPipeline)
		r.Get("/pipelines/{id}", s.getPipeline)
		r.Post("/pipelines/{id}/retry", s.retryPipeline)
		r.Delete("/pipelines/{id}", s.cancelPipeline)

		// Storage
		r.Put("/store/*", s.putObject)
		r.Get("/store/*", s.getObject)
		r.Delete("/store/*", s.deleteObject)

		// Cluster
		r.Get("/health", s.health)
		r.Get("/cluster", s.clusterStatus)
		r.Get("/nodes", s.listNodes)
		r.Get("/nodes/{id}", s.getNode)
		r.Post("/drain", s.drain)
		r.Post("/resume", s.resume)
	})
}
