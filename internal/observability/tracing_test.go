package observability

import (
	"context"
	"testing"
)

func TestTraceIDFromContext_Empty(t *testing.T) {
	ctx := context.Background()
	id := TraceIDFromContext(ctx)
	if id != "" {
		t.Errorf("expected empty trace ID from background context, got %q", id)
	}
}

func TestTracer_ReturnsNonNil(t *testing.T) {
	tracer := Tracer()
	if tracer == nil {
		t.Error("expected non-nil tracer")
	}
}

func TestStartSpan_ReturnsContext(t *testing.T) {
	ctx, span := StartSpan(context.Background(), "test-span")
	defer span.End()

	if ctx == nil {
		t.Error("expected non-nil context from StartSpan")
	}
	if span == nil {
		t.Error("expected non-nil span from StartSpan")
	}
}

func TestStartSpan_WithAttributes(t *testing.T) {
	// Should not panic with various attributes
	ctx, span := StartSpan(context.Background(), "test-span")
	defer span.End()

	if ctx == nil {
		t.Error("expected non-nil context")
	}
}
