package observability

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/models"
)

type SlowQueryDetector struct {
	warningThreshold  time.Duration
	criticalThreshold time.Duration
	logger            *zap.Logger
	analyticsWriter   AnalyticsWriter
}

type AnalyticsWriter interface {
	WriteQueryPerformance(ctx context.Context, event *models.AnalyticsEvent) error
}

func NewSlowQueryDetector(warningMs, criticalMs time.Duration, logger *zap.Logger, aw AnalyticsWriter) *SlowQueryDetector {
	return &SlowQueryDetector{
		warningThreshold:  warningMs,
		criticalThreshold: criticalMs,
		logger:            logger,
		analyticsWriter:   aw,
	}
}

func (sqd *SlowQueryDetector) Intercept(ctx context.Context, query string, queryType string, duration time.Duration, totalHits int64, shardsHit int, timedOut bool) {
	traceID := TraceIDFromContext(ctx)

	event := &models.AnalyticsEvent{
		EventType:  "query_performance",
		QueryHash:  hashQueryForLog(query),
		QueryType:  queryType,
		DurationMs: float64(duration.Milliseconds()),
		TotalHits:  totalHits,
		ShardsHit:  shardsHit,
		TimedOut:   timedOut,
		Timestamp:  time.Now().UTC(),
		TraceID:    traceID,
	}

	severity := sqd.classifySeverity(duration)

	if duration > sqd.warningThreshold {
		SlowQueryCounter.WithLabelValues(severity, queryType).Inc()

		sqd.logger.Warn("slow query detected",
			zap.String("trace_id", traceID),
			zap.String("query_hash", event.QueryHash),
			zap.String("query_type", queryType),
			zap.Float64("duration_ms", event.DurationMs),
			zap.Int64("total_hits", totalHits),
			zap.Int("shards_hit", shardsHit),
			zap.Bool("timed_out", timedOut),
			zap.String("severity", severity),
		)
	}

	if sqd.analyticsWriter != nil {
		if err := sqd.analyticsWriter.WriteQueryPerformance(ctx, event); err != nil {
			sqd.logger.Error("failed to write query analytics",
				zap.String("trace_id", traceID),
				zap.Error(err),
			)
		}
	}
}

func (sqd *SlowQueryDetector) classifySeverity(d time.Duration) string {
	if d > sqd.criticalThreshold {
		return "critical"
	}
	if d > sqd.warningThreshold {
		return "warning"
	}
	return "normal"
}

func hashQueryForLog(q string) string {
	return fmt.Sprintf("%016x", hashUint64(q))
}

func hashUint64(s string) uint64 {
	h := uint64(0)
	for _, c := range s {
		h = h*31 + uint64(c)
	}
	return h
}
