package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func NewRouter(handler *Handler, health *HealthHandler, logger *zap.Logger) http.Handler {
	r := chi.NewRouter()

	// Global middleware (applied to all routes)
	r.Use(RecoveryMiddleware(logger))
	r.Use(CORSMiddleware)
	r.Use(RequestIDMiddleware)
	r.Use(LoggingMiddleware(logger))

	// Health and metrics endpoints are registered BEFORE the rate limiter
	// so Kubernetes probes and Prometheus scrapes are never rejected under load.
	r.Get("/healthz", health.Liveness)
	r.Get("/readyz", health.Readiness)
	r.Handle("/metrics", promhttp.Handler())

	// Rate limiter only applies to API routes below
	r.Group(func(r chi.Router) {
		rl := NewRateLimiter(1000, logger)
		r.Use(rl.Middleware)

		// API v1
		r.Route("/api/v1", func(r chi.Router) {
			r.Get("/search", handler.Search)
			r.Post("/search", handler.Search)
			r.Get("/autocomplete", handler.Autocomplete)
			r.Get("/trending", handler.Trending)
		})
	})

	return r
}
