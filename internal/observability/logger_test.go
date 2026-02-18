package observability

import "testing"

func TestNewLogger_ValidLevels(t *testing.T) {
	levels := []string{"debug", "info", "warn", "error"}

	for _, level := range levels {
		t.Run(level, func(t *testing.T) {
			logger, err := NewLogger(level)
			if err != nil {
				t.Errorf("expected no error for level %q, got %v", level, err)
			}
			if logger == nil {
				t.Errorf("expected non-nil logger for level %q", level)
			}
		})
	}
}

func TestNewLogger_DefaultLevel(t *testing.T) {
	// Unknown level should default to info
	logger, err := NewLogger("unknown")
	if err != nil {
		t.Errorf("expected no error for unknown level, got %v", err)
	}
	if logger == nil {
		t.Error("expected non-nil logger for unknown level")
	}
}

func TestNewLogger_EmptyLevel(t *testing.T) {
	logger, err := NewLogger("")
	if err != nil {
		t.Errorf("expected no error for empty level, got %v", err)
	}
	if logger == nil {
		t.Error("expected non-nil logger for empty level")
	}
}
