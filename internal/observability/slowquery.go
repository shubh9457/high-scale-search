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
	// Only log and write analytics for queries that exceed the warning threshold.
	// Fast queries (~99% of traffic) return immediately with zero overhead.
	if duration <= sqd.warningThreshold {
		return
	}

	traceID := TraceIDFromContext(ctx)
	severity := sqd.classifySeverity(duration)

	SlowQueryCounter.WithLabelValues(severity, queryType).Inc()

	sqd.logger.Warn("slow query detected",
		zap.String("trace_id", traceID),
		zap.String("query_hash", hashQueryForLog(query)),
		zap.String("query_type", queryType),
		zap.Float64("duration_ms", float64(duration.Milliseconds())),
		zap.Int64("total_hits", totalHits),
		zap.Int("shards_hit", shardsHit),
		zap.Bool("timed_out", timedOut),
		zap.String("severity", severity),
	)

	// Write to ClickHouse asynchronously so it doesn't block the response.
	if sqd.analyticsWriter != nil {
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
		go func() {
			writeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := sqd.analyticsWriter.WriteQueryPerformance(writeCtx, event); err != nil {
				sqd.logger.Error("failed to write query analytics",
					zap.String("trace_id", traceID),
					zap.Error(err),
				)
			}
		}()
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
