package observability

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/shubhsaxena/high-scale-search/internal/models"

	"go.uber.org/zap"
)

type mockAnalyticsWriter struct {
	mu     sync.Mutex
	events []*models.AnalyticsEvent
}

func (m *mockAnalyticsWriter) WriteQueryPerformance(ctx context.Context, event *models.AnalyticsEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
	return nil
}

func (m *mockAnalyticsWriter) getEvents() []*models.AnalyticsEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*models.AnalyticsEvent, len(m.events))
	copy(cp, m.events)
	return cp
}

func TestSlowQueryDetector_ClassifySeverity(t *testing.T) {
	sqd := &SlowQueryDetector{
		warningThreshold:  200 * time.Millisecond,
		criticalThreshold: 500 * time.Millisecond,
	}

	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{"below warning", 100 * time.Millisecond, "normal"},
		{"at warning", 200 * time.Millisecond, "normal"},
		{"above warning", 300 * time.Millisecond, "warning"},
		{"at critical", 500 * time.Millisecond, "warning"},
		{"above critical", 600 * time.Millisecond, "critical"},
		{"well above critical", 1 * time.Second, "critical"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sqd.classifySeverity(tt.duration)
			if got != tt.want {
				t.Errorf("classifySeverity(%v) = %q, want %q", tt.duration, got, tt.want)
			}
		})
	}
}

func TestSlowQueryDetector_InterceptBelowThreshold(t *testing.T) {
	aw := &mockAnalyticsWriter{}
	sqd := NewSlowQueryDetector(200*time.Millisecond, 500*time.Millisecond, zap.NewNop(), aw)

	sqd.Intercept(context.Background(), "fast query", "fulltext",
		100*time.Millisecond, 50, 5, false)

	// Give async writer time just in case (it shouldn't fire)
	time.Sleep(50 * time.Millisecond)

	events := aw.getEvents()
	if len(events) != 0 {
		t.Errorf("expected no analytics events for fast query, got %d", len(events))
	}
}

func TestSlowQueryDetector_InterceptAtThreshold(t *testing.T) {
	aw := &mockAnalyticsWriter{}
	sqd := NewSlowQueryDetector(200*time.Millisecond, 500*time.Millisecond, zap.NewNop(), aw)

	sqd.Intercept(context.Background(), "at-threshold query", "fulltext",
		200*time.Millisecond, 50, 5, false)

	time.Sleep(50 * time.Millisecond)

	events := aw.getEvents()
	if len(events) != 0 {
		t.Errorf("expected no analytics events at exact threshold, got %d", len(events))
	}
}

func TestSlowQueryDetector_InterceptAboveWarning(t *testing.T) {
	aw := &mockAnalyticsWriter{}
	sqd := NewSlowQueryDetector(200*time.Millisecond, 500*time.Millisecond, zap.NewNop(), aw)

	sqd.Intercept(context.Background(), "slow query", "fulltext",
		300*time.Millisecond, 100, 3, false)

	// Wait for async analytics write
	time.Sleep(100 * time.Millisecond)

	events := aw.getEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 analytics event, got %d", len(events))
	}

	event := events[0]
	if event.EventType != "query_performance" {
		t.Errorf("expected event type 'query_performance', got %q", event.EventType)
	}
	if event.QueryType != "fulltext" {
		t.Errorf("expected query type 'fulltext', got %q", event.QueryType)
	}
	if event.DurationMs != 300 {
		t.Errorf("expected duration 300ms, got %f", event.DurationMs)
	}
	if event.TotalHits != 100 {
		t.Errorf("expected total hits 100, got %d", event.TotalHits)
	}
	if event.ShardsHit != 3 {
		t.Errorf("expected shards hit 3, got %d", event.ShardsHit)
	}
}

func TestSlowQueryDetector_InterceptAboveCritical(t *testing.T) {
	aw := &mockAnalyticsWriter{}
	sqd := NewSlowQueryDetector(200*time.Millisecond, 500*time.Millisecond, zap.NewNop(), aw)

	sqd.Intercept(context.Background(), "critical query", "analytics",
		700*time.Millisecond, 200, 10, true)

	time.Sleep(100 * time.Millisecond)

	events := aw.getEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 analytics event, got %d", len(events))
	}

	event := events[0]
	if event.TimedOut != true {
		t.Error("expected timed_out true")
	}
}

func TestSlowQueryDetector_NilAnalyticsWriter(t *testing.T) {
	// Should not panic with nil analytics writer
	sqd := NewSlowQueryDetector(200*time.Millisecond, 500*time.Millisecond, zap.NewNop(), nil)

	// Should not panic
	sqd.Intercept(context.Background(), "slow query", "fulltext",
		300*time.Millisecond, 100, 3, false)
}

func TestNewSlowQueryDetector(t *testing.T) {
	aw := &mockAnalyticsWriter{}
	sqd := NewSlowQueryDetector(200*time.Millisecond, 500*time.Millisecond, zap.NewNop(), aw)

	if sqd == nil {
		t.Fatal("expected non-nil SlowQueryDetector")
	}
	if sqd.warningThreshold != 200*time.Millisecond {
		t.Errorf("expected warning threshold 200ms, got %v", sqd.warningThreshold)
	}
	if sqd.criticalThreshold != 500*time.Millisecond {
		t.Errorf("expected critical threshold 500ms, got %v", sqd.criticalThreshold)
	}
}

func TestHashQueryForLog(t *testing.T) {
	h1 := hashQueryForLog("test query")
	h2 := hashQueryForLog("test query")

	if h1 != h2 {
		t.Errorf("hashQueryForLog not deterministic: %q != %q", h1, h2)
	}
	if h1 == "" {
		t.Error("expected non-empty hash")
	}
	// Should be 16 hex chars
	if len(h1) != 16 {
		t.Errorf("expected 16 char hex, got %d chars: %q", len(h1), h1)
	}
}

func TestHashUint64(t *testing.T) {
	h1 := hashUint64("test")
	h2 := hashUint64("test")
	if h1 != h2 {
		t.Error("hashUint64 not deterministic")
	}

	h3 := hashUint64("other")
	if h1 == h3 {
		t.Error("different inputs should produce different hashes")
	}

	h4 := hashUint64("")
	if h4 != 0 {
		t.Errorf("expected 0 for empty string, got %d", h4)
	}
}
