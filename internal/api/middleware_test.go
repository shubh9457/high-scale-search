package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

func TestRequestIDFromContext_Empty(t *testing.T) {
	ctx := context.Background()
	id := RequestIDFromContext(ctx)
	if id != "" {
		t.Errorf("expected empty string from empty context, got %q", id)
	}
}

func TestRequestIDFromContext_WithValue(t *testing.T) {
	ctx := context.WithValue(context.Background(), requestIDKey, "test-123")
	id := RequestIDFromContext(ctx)
	if id != "test-123" {
		t.Errorf("expected 'test-123', got %q", id)
	}
}

func TestRequestIDFromContext_WrongType(t *testing.T) {
	ctx := context.WithValue(context.Background(), requestIDKey, 12345)
	id := RequestIDFromContext(ctx)
	if id != "" {
		t.Errorf("expected empty string for wrong type, got %q", id)
	}
}

func TestRequestIDMiddleware_GeneratesID(t *testing.T) {
	var capturedID string
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = RequestIDFromContext(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if capturedID == "" {
		t.Error("expected generated request ID")
	}
	if rr.Header().Get("X-Request-ID") == "" {
		t.Error("expected X-Request-ID header in response")
	}
	if rr.Header().Get("X-Request-ID") != capturedID {
		t.Error("response header should match context ID")
	}
}

func TestRequestIDMiddleware_UsesExistingHeader(t *testing.T) {
	var capturedID string
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = RequestIDFromContext(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "existing-id-123")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if capturedID != "existing-id-123" {
		t.Errorf("expected existing-id-123, got %q", capturedID)
	}
	if rr.Header().Get("X-Request-ID") != "existing-id-123" {
		t.Error("response should echo back the provided request ID")
	}
}

func TestResponseWriter_WriteHeader(t *testing.T) {
	rr := httptest.NewRecorder()
	rw := &responseWriter{ResponseWriter: rr, statusCode: http.StatusOK}

	rw.WriteHeader(http.StatusNotFound)
	if rw.statusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rw.statusCode)
	}
	if !rw.wroteHeader {
		t.Error("expected wroteHeader to be true")
	}
}

func TestResponseWriter_WriteHeaderOnlyOnce(t *testing.T) {
	rr := httptest.NewRecorder()
	rw := &responseWriter{ResponseWriter: rr, statusCode: http.StatusOK}

	rw.WriteHeader(http.StatusNotFound)
	rw.WriteHeader(http.StatusOK) // should be ignored

	if rw.statusCode != http.StatusNotFound {
		t.Errorf("second WriteHeader should be ignored, status should be 404, got %d", rw.statusCode)
	}
}

func TestResponseWriter_Unwrap(t *testing.T) {
	rr := httptest.NewRecorder()
	rw := &responseWriter{ResponseWriter: rr}

	unwrapped := rw.Unwrap()
	if unwrapped != rr {
		t.Error("Unwrap should return the underlying ResponseWriter")
	}
}

func TestLoggingMiddleware(t *testing.T) {
	logger := zap.NewNop()
	middleware := LoggingMiddleware(logger)

	var handlerCalled bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	handler := middleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if !handlerCalled {
		t.Error("expected inner handler to be called")
	}
}

func TestRecoveryMiddleware_NoPanic(t *testing.T) {
	logger := zap.NewNop()
	middleware := RecoveryMiddleware(logger)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := middleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestRecoveryMiddleware_WithPanic(t *testing.T) {
	logger := zap.NewNop()
	middleware := RecoveryMiddleware(logger)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	handler := middleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 after panic, got %d", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "application/json" {
		t.Error("expected JSON content type in panic response")
	}
	body := rr.Body.String()
	if body == "" {
		t.Error("expected non-empty error body")
	}
}

func TestRateLimiter_AllowsRequests(t *testing.T) {
	logger := zap.NewNop()
	rl := NewRateLimiter(5, logger)

	var handlerCalled bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
	})

	handler := rl.Middleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if !handlerCalled {
		t.Error("expected handler to be called when under limit")
	}
}

func TestRateLimiter_RejectsWhenFull(t *testing.T) {
	logger := zap.NewNop()
	rl := NewRateLimiter(1, logger)

	// Block the token by taking it
	blocker := make(chan struct{})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocker
	})

	handler := rl.Middleware(inner)

	// First request takes the token
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
	}()

	// Wait a bit for the goroutine to acquire the token
	// Use a second request that should be rejected
	// We need to ensure first request has acquired its token
	req2 := httptest.NewRequest(http.MethodGet, "/test2", nil)
	rr2 := httptest.NewRecorder()

	// Allow a small delay for the first goroutine to start
	// This is inherently racy, but rate limiter with capacity 1 is deterministic
	// once the token is consumed (which happens immediately)
	// The goroutine needs time to start, so let's drain the token manually
	<-rl.tokens // take the second path: drain from channel directly after refill from first handler

	// Now put one token back so the test limiter is at capacity 0
	// Actually, let me restructure: just create a limiter with 0 capacity
	close(blocker) // cleanup

	rl2 := &RateLimiter{
		tokens: make(chan struct{}, 1), // capacity 1 but empty
		logger: logger,
	}
	handler2 := rl2.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not be called")
	}))

	handler2.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", rr2.Code)
	}
}

func TestRateLimiter_TokenReturnedAfterRequest(t *testing.T) {
	logger := zap.NewNop()
	rl := NewRateLimiter(1, logger)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := rl.Middleware(inner)

	// First request
	req1 := httptest.NewRequest(http.MethodGet, "/test1", nil)
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req1)

	if rr1.Code != http.StatusOK {
		t.Errorf("expected 200 for first request, got %d", rr1.Code)
	}

	// Second request should also succeed because token is returned
	req2 := httptest.NewRequest(http.MethodGet, "/test2", nil)
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Errorf("expected 200 for second request (token returned), got %d", rr2.Code)
	}
}

func TestCORSMiddleware_SetsHeaders(t *testing.T) {
	handler := CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("expected CORS origin header")
	}
	if rr.Header().Get("Access-Control-Allow-Methods") != "GET, POST, OPTIONS" {
		t.Error("expected CORS methods header")
	}
	if rr.Header().Get("Access-Control-Allow-Headers") != "Content-Type, Authorization, X-Request-ID" {
		t.Error("expected CORS headers header")
	}
	if rr.Header().Get("Access-Control-Max-Age") != "86400" {
		t.Error("expected CORS max-age header")
	}
}

func TestCORSMiddleware_OptionsRequest(t *testing.T) {
	var handlerCalled bool
	handler := CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
	}))

	req := httptest.NewRequest(http.MethodOptions, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("expected 204 for OPTIONS, got %d", rr.Code)
	}
	if handlerCalled {
		t.Error("handler should not be called for OPTIONS preflight")
	}
}

func TestCORSMiddleware_NonOptionsPassesThrough(t *testing.T) {
	var handlerCalled bool
	handler := CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
	}))

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut} {
		handlerCalled = false
		req := httptest.NewRequest(method, "/test", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if !handlerCalled {
			t.Errorf("handler should be called for %s", method)
		}
	}
}
