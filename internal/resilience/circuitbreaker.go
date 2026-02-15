package resilience

import (
	"context"
	"fmt"
	"time"

	"github.com/sony/gobreaker"
	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/observability"
)

func NewCircuitBreaker(name string, cfg config.CircuitBreakerConfig, logger *zap.Logger) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,
		MaxRequests: cfg.MaxRequests,
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= uint32(cfg.FailureThreshold)
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			logger.Warn("circuit breaker state change",
				zap.String("name", name),
				zap.String("from", from.String()),
				zap.String("to", to.String()),
			)
			var stateVal float64
			switch to {
			case gobreaker.StateClosed:
				stateVal = 0
			case gobreaker.StateHalfOpen:
				stateVal = 1
			case gobreaker.StateOpen:
				stateVal = 2
			}
			observability.CircuitBreakerState.WithLabelValues(name).Set(stateVal)
		},
	})
}

type RetryConfig struct {
	MaxAttempts int
	InitialWait time.Duration
	MaxWait     time.Duration
	Multiplier  float64
}

// Retry executes fn with exponential backoff. It respects context cancellation
// between attempts, returning immediately if the context is done.
func Retry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	var lastErr error
	wait := cfg.InitialWait

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if attempt < cfg.MaxAttempts-1 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("retry cancelled: %w", ctx.Err())
			case <-time.After(wait):
			}
			wait = time.Duration(float64(wait) * cfg.Multiplier)
			if wait > cfg.MaxWait {
				wait = cfg.MaxWait
			}
		}
	}

	return fmt.Errorf("all %d retry attempts failed: %w", cfg.MaxAttempts, lastErr)
}
