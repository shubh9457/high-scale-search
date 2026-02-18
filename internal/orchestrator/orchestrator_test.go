package orchestrator

import (
	"testing"

	"github.com/shubhsaxena/high-scale-search/internal/models"
)

func TestSanitizeIndexComponent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"us-east", "us-east"},
		{"us-west-2", "us-west-2"},
		{"global", "global"},
		{"abc123", "abc123"},
		{"", ""},
		{"US-EAST", ""},       // uppercase not allowed
		{"us_east", ""},       // underscore not allowed
		{"us east", ""},       // space not allowed
		{"us.east", ""},       // dot not allowed
		{"us/east", ""},       // slash not allowed
		{"../../etc", ""},     // path traversal attempt
		{"<script>", ""},      // injection attempt
		{"us-east-1", "us-east-1"},
		{"-leading", "-leading"},
		{"a", "a"},
		{"123", "123"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeIndexComponent(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeIndexComponent(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestStaticFallback_SetAndGet(t *testing.T) {
	o := &Orchestrator{
		staticFallback: make(map[string][]models.SearchResult),
	}

	results := []models.SearchResult{
		{ID: "1", Title: "Popular Item 1"},
		{ID: "2", Title: "Popular Item 2"},
	}

	o.SetStaticFallback("us-east", results)

	got := o.getStaticFallback("us-east")
	if len(got) != 2 {
		t.Fatalf("expected 2 results, got %d", len(got))
	}
	if got[0].ID != "1" || got[1].ID != "2" {
		t.Errorf("unexpected results: %v", got)
	}
}

func TestStaticFallback_ReturnsNilForMissing(t *testing.T) {
	o := &Orchestrator{
		staticFallback: make(map[string][]models.SearchResult),
	}

	got := o.getStaticFallback("nonexistent")
	if got != nil {
		t.Errorf("expected nil for missing region, got %v", got)
	}
}

func TestStaticFallback_DefaultFallback(t *testing.T) {
	o := &Orchestrator{
		staticFallback: make(map[string][]models.SearchResult),
	}

	defaultResults := []models.SearchResult{
		{ID: "default-1", Title: "Default Item"},
	}
	o.SetStaticFallback("default", defaultResults)

	// Requesting unknown region should fall back to default
	got := o.getStaticFallback("unknown-region")
	if len(got) != 1 {
		t.Fatalf("expected 1 default result, got %d", len(got))
	}
	if got[0].ID != "default-1" {
		t.Errorf("expected default-1, got %s", got[0].ID)
	}
}

func TestStaticFallback_RegionTakesPriorityOverDefault(t *testing.T) {
	o := &Orchestrator{
		staticFallback: make(map[string][]models.SearchResult),
	}

	o.SetStaticFallback("default", []models.SearchResult{
		{ID: "default-1", Title: "Default"},
	})
	o.SetStaticFallback("us-east", []models.SearchResult{
		{ID: "us-east-1", Title: "US East"},
	})

	got := o.getStaticFallback("us-east")
	if len(got) != 1 || got[0].ID != "us-east-1" {
		t.Errorf("expected us-east result, got %v", got)
	}
}

func TestStaticFallback_ReturnsCopy(t *testing.T) {
	o := &Orchestrator{
		staticFallback: make(map[string][]models.SearchResult),
	}

	o.SetStaticFallback("us-east", []models.SearchResult{
		{ID: "1", Title: "Item 1"},
	})

	got := o.getStaticFallback("us-east")
	got[0].Title = "MUTATED"

	// Original should not be affected
	again := o.getStaticFallback("us-east")
	if again[0].Title != "Item 1" {
		t.Errorf("static fallback was mutated: got %q, want 'Item 1'", again[0].Title)
	}
}

func TestStaticFallback_EmptySlice(t *testing.T) {
	o := &Orchestrator{
		staticFallback: make(map[string][]models.SearchResult),
	}

	o.SetStaticFallback("us-east", []models.SearchResult{})

	got := o.getStaticFallback("us-east")
	if got != nil {
		t.Errorf("expected nil for empty fallback, got %v", got)
	}
}

func TestStaticFallback_Overwrite(t *testing.T) {
	o := &Orchestrator{
		staticFallback: make(map[string][]models.SearchResult),
	}

	o.SetStaticFallback("us-east", []models.SearchResult{
		{ID: "1", Title: "Old"},
	})
	o.SetStaticFallback("us-east", []models.SearchResult{
		{ID: "2", Title: "New"},
	})

	got := o.getStaticFallback("us-east")
	if len(got) != 1 || got[0].ID != "2" {
		t.Errorf("expected overwritten result, got %v", got)
	}
}
