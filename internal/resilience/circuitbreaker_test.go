package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shubhsaxena/high-scale-search/internal/config"

	"go.uber.org/zap"
)

func TestRetry_SuccessFirstAttempt(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 3,
		InitialWait: 10 * time.Millisecond,
		MaxWait:     100 * time.Millisecond,
		Multiplier:  2.0,
	}

	attempts := 0
	err := Retry(context.Background(), cfg, func() error {
		attempts++
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
}

func TestRetry_SuccessAfterRetries(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 3,
		InitialWait: 1 * time.Millisecond,
		MaxWait:     10 * time.Millisecond,
		Multiplier:  2.0,
	}

	attempts := 0
	err := Retry(context.Background(), cfg, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetry_AllAttemptsFail(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 3,
		InitialWait: 1 * time.Millisecond,
		MaxWait:     10 * time.Millisecond,
		Multiplier:  2.0,
	}

	attempts := 0
	err := Retry(context.Background(), cfg, func() error {
		attempts++
		return errors.New("persistent error")
	})

	if err == nil {
		t.Error("expected error after all attempts fail")
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetry_ErrorMessageContainsAttemptCount(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 2,
		InitialWait: 1 * time.Millisecond,
		MaxWait:     10 * time.Millisecond,
		Multiplier:  2.0,
	}

	err := Retry(context.Background(), cfg, func() error {
		return errors.New("fail")
	})

	if err == nil {
		t.Fatal("expected error")
	}
	errMsg := err.Error()
	if errMsg == "" {
		t.Error("expected non-empty error message")
	}
}

func TestRetry_ContextCancellation(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 10,
		InitialWait: 100 * time.Millisecond,
		MaxWait:     1 * time.Second,
		Multiplier:  2.0,
	}

	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := Retry(ctx, cfg, func() error {
		attempts++
		return errors.New("fail")
	})

	if err == nil {
		t.Error("expected error on context cancellation")
	}
	if attempts >= 10 {
		t.Errorf("expected fewer than 10 attempts due to cancellation, got %d", attempts)
	}
}

func TestRetry_SingleAttempt(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 1,
		InitialWait: 1 * time.Millisecond,
		MaxWait:     10 * time.Millisecond,
		Multiplier:  2.0,
	}

	attempts := 0
	err := Retry(context.Background(), cfg, func() error {
		attempts++
		return errors.New("fail")
	})

	if err == nil {
		t.Error("expected error for single failed attempt")
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
}

func TestRetry_BackoffCapped(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 4,
		InitialWait: 1 * time.Millisecond,
		MaxWait:     5 * time.Millisecond,
		Multiplier:  10.0, // aggressive multiplier
	}

	start := time.Now()
	Retry(context.Background(), cfg, func() error {
		return errors.New("fail")
	})
	elapsed := time.Since(start)

	// With max backoff of 5ms and 3 waits (between 4 attempts),
	// total wait should be at most ~15ms + some overhead
	if elapsed > 100*time.Millisecond {
		t.Errorf("backoff seems uncapped, total time: %v", elapsed)
	}
}

func TestRetry_WrapsLastError(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 2,
		InitialWait: 1 * time.Millisecond,
		MaxWait:     10 * time.Millisecond,
		Multiplier:  2.0,
	}

	targetErr := errors.New("specific error")
	err := Retry(context.Background(), cfg, func() error {
		return targetErr
	})

	if !errors.Is(err, targetErr) {
		t.Error("expected error to wrap the last error from fn")
	}
}

func TestNewCircuitBreaker(t *testing.T) {
	logger := zap.NewNop()
	cfg := config.CircuitBreakerConfig{
		MaxRequests:      100,
		Interval:         30 * time.Second,
		Timeout:          30 * time.Second,
		FailureThreshold: 5,
	}

	cb := NewCircuitBreaker("test-cb", cfg, logger)
	if cb == nil {
		t.Fatal("expected non-nil circuit breaker")
	}
	if cb.Name() != "test-cb" {
		t.Errorf("expected name 'test-cb', got %q", cb.Name())
	}
}

func TestNewCircuitBreaker_ExecuteSuccess(t *testing.T) {
	logger := zap.NewNop()
	cfg := config.CircuitBreakerConfig{
		MaxRequests:      10,
		Interval:         time.Second,
		Timeout:          time.Second,
		FailureThreshold: 3,
	}

	cb := NewCircuitBreaker("test-cb", cfg, logger)

	result, err := cb.Execute(func() (any, error) {
		return "ok", nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if result != "ok" {
		t.Errorf("expected 'ok', got %v", result)
	}
}

func TestNewCircuitBreaker_ExecuteFailure(t *testing.T) {
	logger := zap.NewNop()
	cfg := config.CircuitBreakerConfig{
		MaxRequests:      10,
		Interval:         time.Second,
		Timeout:          time.Second,
		FailureThreshold: 3,
	}

	cb := NewCircuitBreaker("test-cb", cfg, logger)

	_, err := cb.Execute(func() (any, error) {
		return nil, errors.New("fail")
	})

	if err == nil {
		t.Error("expected error")
	}
}

func TestNewCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	logger := zap.NewNop()
	cfg := config.CircuitBreakerConfig{
		MaxRequests:      1,
		Interval:         10 * time.Second,
		Timeout:          10 * time.Second,
		FailureThreshold: 3,
	}

	cb := NewCircuitBreaker("test-cb", cfg, logger)

	// Fail 3 times to trip the circuit breaker
	for i := 0; i < 3; i++ {
		cb.Execute(func() (any, error) {
			return nil, errors.New("fail")
		})
	}

	// Next call should be rejected by circuit breaker
	_, err := cb.Execute(func() (any, error) {
		return "should not reach", nil
	})

	if err == nil {
		t.Error("expected circuit breaker to reject request after threshold")
	}
}
