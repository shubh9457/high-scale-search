package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

type HealthChecker interface {
	HealthCheck(ctx context.Context) error
}

type ESHealthChecker interface {
	HealthCheck(ctx context.Context) (string, error)
}

type HealthHandler struct {
	checks map[string]HealthChecker
	esCheck ESHealthChecker
	logger  *zap.Logger
}

func NewHealthHandler(logger *zap.Logger) *HealthHandler {
	return &HealthHandler{
		checks: make(map[string]HealthChecker),
		logger: logger,
	}
}

func (h *HealthHandler) Register(name string, checker HealthChecker) {
	h.checks[name] = checker
}

func (h *HealthHandler) RegisterES(checker ESHealthChecker) {
	h.esCheck = checker
}

type componentHealth struct {
	Status  string `json:"status"`
	Latency string `json:"latency,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (h *HealthHandler) Liveness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
}

func (h *HealthHandler) Readiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	results := make(map[string]componentHealth)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Check regular health checkers
	for name, checker := range h.checks {
		wg.Add(1)
		go func(n string, c HealthChecker) {
			defer wg.Done()
			start := time.Now()
			err := c.HealthCheck(ctx)
			ch := componentHealth{
				Status:  "healthy",
				Latency: time.Since(start).String(),
			}
			if err != nil {
				ch.Status = "unhealthy"
				ch.Error = err.Error()
			}
			mu.Lock()
			results[n] = ch
			mu.Unlock()
		}(name, checker)
	}

	// Check ES
	if h.esCheck != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			status, err := h.esCheck.HealthCheck(ctx)
			ch := componentHealth{
				Status:  status,
				Latency: time.Since(start).String(),
			}
			if err != nil {
				ch.Error = err.Error()
			}
			mu.Lock()
			results["elasticsearch"] = ch
			mu.Unlock()
		}()
	}

	wg.Wait()

	overallStatus := http.StatusOK
	overall := "healthy"
	for _, ch := range results {
		if ch.Status == "unhealthy" || ch.Status == "red" {
			overallStatus = http.StatusServiceUnavailable
			overall = "degraded"
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(overallStatus)
	json.NewEncoder(w).Encode(map[string]any{
		"status":     overall,
		"components": results,
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	})
}
