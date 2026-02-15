package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func NewRouter(handler *Handler, health *HealthHandler, logger *zap.Logger) http.Handler {
	r := chi.NewRouter()

	// Global middleware
	r.Use(RecoveryMiddleware(logger))
	r.Use(CORSMiddleware)
	r.Use(RequestIDMiddleware)
	r.Use(LoggingMiddleware(logger))

	// Rate limiter: max 1000 concurrent requests
	rl := NewRateLimiter(1000, logger)
	r.Use(rl.Middleware)

	// Health endpoints (no rate limit)
	r.Get("/healthz", health.Liveness)
	r.Get("/readyz", health.Readiness)

	// Metrics endpoint
	r.Handle("/metrics", promhttp.Handler())

	// API v1
	r.Route("/api/v1", func(r chi.Router) {
		// Search endpoints
		r.Get("/search", handler.Search)
		r.Post("/search", handler.Search)

		// Autocomplete
		r.Get("/autocomplete", handler.Autocomplete)

		// Trending
		r.Get("/trending", handler.Trending)
	})

	return r
}
