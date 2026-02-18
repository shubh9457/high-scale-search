package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

type mockHealthChecker struct {
	err error
}

func (m *mockHealthChecker) HealthCheck(ctx context.Context) error {
	return m.err
}

type mockESHealthChecker struct {
	status string
	err    error
}

func (m *mockESHealthChecker) HealthCheck(ctx context.Context) (string, error) {
	return m.status, m.err
}

func TestNewHealthHandler(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	if hh == nil {
		t.Fatal("expected non-nil HealthHandler")
	}
	if hh.checks == nil {
		t.Error("expected checks map to be initialized")
	}
}

func TestHealthHandler_Register(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	checker := &mockHealthChecker{}
	hh.Register("redis", checker)

	if len(hh.checks) != 1 {
		t.Errorf("expected 1 registered check, got %d", len(hh.checks))
	}
}

func TestHealthHandler_RegisterES(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	checker := &mockESHealthChecker{status: "green"}
	hh.RegisterES(checker)

	if hh.esCheck == nil {
		t.Error("expected esCheck to be set")
	}
}

func TestHealthHandler_Liveness(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()

	hh.Liveness(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "application/json" {
		t.Error("expected application/json content type")
	}

	var result map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if result["status"] != "alive" {
		t.Errorf("expected status 'alive', got %q", result["status"])
	}
}

func TestHealthHandler_Readiness_AllHealthy(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	hh.Register("redis", &mockHealthChecker{err: nil})
	hh.Register("clickhouse", &mockHealthChecker{err: nil})
	hh.RegisterES(&mockESHealthChecker{status: "green"})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	hh.Readiness(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	var result map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if result["status"] != "healthy" {
		t.Errorf("expected overall status 'healthy', got %v", result["status"])
	}

	components, ok := result["components"].(map[string]any)
	if !ok {
		t.Fatal("expected components map")
	}
	if len(components) != 3 {
		t.Errorf("expected 3 components, got %d", len(components))
	}
}

func TestHealthHandler_Readiness_OneUnhealthy(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	hh.Register("redis", &mockHealthChecker{err: nil})
	hh.Register("clickhouse", &mockHealthChecker{err: fmt.Errorf("connection refused")})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	hh.Readiness(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rr.Code)
	}

	var result map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if result["status"] != "degraded" {
		t.Errorf("expected overall status 'degraded', got %v", result["status"])
	}
}

func TestHealthHandler_Readiness_ESRed(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	hh.RegisterES(&mockESHealthChecker{status: "red", err: fmt.Errorf("cluster red")})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	hh.Readiness(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for ES red, got %d", rr.Code)
	}
}

func TestHealthHandler_Readiness_ESYellow(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	hh.RegisterES(&mockESHealthChecker{status: "yellow"})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	hh.Readiness(rr, req)

	// Yellow should be considered healthy (not "unhealthy" or "red")
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for ES yellow, got %d", rr.Code)
	}
}

func TestHealthHandler_Readiness_NoChecks(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	hh.Readiness(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 when no checks registered, got %d", rr.Code)
	}

	var result map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if result["status"] != "healthy" {
		t.Errorf("expected 'healthy' when no checks, got %v", result["status"])
	}
}

func TestHealthHandler_Readiness_HasTimestamp(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	hh.Readiness(rr, req)

	var result map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if _, ok := result["timestamp"]; !ok {
		t.Error("expected timestamp in response")
	}
}

func TestHealthHandler_Readiness_ComponentLatency(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	hh.Register("redis", &mockHealthChecker{err: nil})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	hh.Readiness(rr, req)

	var result map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	components := result["components"].(map[string]any)
	redis := components["redis"].(map[string]any)
	if redis["latency"] == nil || redis["latency"] == "" {
		t.Error("expected latency to be populated")
	}
	if redis["status"] != "healthy" {
		t.Errorf("expected redis status 'healthy', got %v", redis["status"])
	}
}

func TestHealthHandler_Readiness_UnhealthyComponentHasError(t *testing.T) {
	logger := zap.NewNop()
	hh := NewHealthHandler(logger)

	hh.Register("redis", &mockHealthChecker{err: fmt.Errorf("connection refused")})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()

	hh.Readiness(rr, req)

	var result map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	components := result["components"].(map[string]any)
	redis := components["redis"].(map[string]any)
	if redis["status"] != "unhealthy" {
		t.Errorf("expected redis status 'unhealthy', got %v", redis["status"])
	}
	if redis["error"] != "connection refused" {
		t.Errorf("expected error 'connection refused', got %v", redis["error"])
	}
}
