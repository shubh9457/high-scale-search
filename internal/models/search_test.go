package models

import "testing"

func TestIntentString(t *testing.T) {
	tests := []struct {
		intent Intent
		want   string
	}{
		{IntentFullText, "fulltext"},
		{IntentAnalytics, "analytics"},
		{IntentFaceted, "faceted"},
		{IntentAutocomplete, "autocomplete"},
		{Intent(99), "unknown"},
		{Intent(-1), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.intent.String()
			if got != tt.want {
				t.Errorf("Intent(%d).String() = %q, want %q", tt.intent, got, tt.want)
			}
		})
	}
}

func TestIntentConstants(t *testing.T) {
	// Verify iota ordering
	if IntentFullText != 0 {
		t.Errorf("IntentFullText = %d, want 0", IntentFullText)
	}
	if IntentAnalytics != 1 {
		t.Errorf("IntentAnalytics = %d, want 1", IntentAnalytics)
	}
	if IntentFaceted != 2 {
		t.Errorf("IntentFaceted = %d, want 2", IntentFaceted)
	}
	if IntentAutocomplete != 3 {
		t.Errorf("IntentAutocomplete = %d, want 3", IntentAutocomplete)
	}
}

func TestSearchRequest_Defaults(t *testing.T) {
	req := SearchRequest{}
	if req.Query != "" {
		t.Error("expected empty query")
	}
	if req.Page != 0 {
		t.Error("expected zero page")
	}
	if req.PageSize != 0 {
		t.Error("expected zero page size")
	}
	if req.ForceFresh {
		t.Error("expected ForceFresh to be false")
	}
}

func TestSearchResponse_Defaults(t *testing.T) {
	resp := SearchResponse{}
	if resp.Results != nil {
		t.Error("expected nil results")
	}
	if resp.Total != 0 {
		t.Error("expected zero total")
	}
	if resp.Metadata.CacheHit {
		t.Error("expected CacheHit to be false")
	}
}

func TestParsedQuery_Defaults(t *testing.T) {
	pq := ParsedQuery{}
	if pq.HasWildcard {
		t.Error("expected HasWildcard false")
	}
	if pq.HasQuotes {
		t.Error("expected HasQuotes false")
	}
	if pq.IsPhrase {
		t.Error("expected IsPhrase false")
	}
}
