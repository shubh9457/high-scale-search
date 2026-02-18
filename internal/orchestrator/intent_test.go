package orchestrator

import (
	"testing"

	"github.com/shubhsaxena/high-scale-search/internal/models"
)

func TestIntentClassifier_Classify_EmptyQuery(t *testing.T) {
	ic := NewIntentClassifier()
	parsed := &models.ParsedQuery{
		Normalized: "",
		Tokens:     nil,
		Fields:     make(map[string]string),
	}

	intent := ic.Classify(parsed)
	if intent != models.IntentFullText {
		t.Errorf("expected IntentFullText for empty query, got %v", intent)
	}
}

func TestIntentClassifier_Classify_Autocomplete(t *testing.T) {
	ic := NewIntentClassifier()

	tests := []struct {
		name       string
		normalized string
		tokens     []string
	}{
		{"single char", "a", []string{}},
		{"two chars", "la", []string{"la"}},
		{"three chars", "lap", []string{"lap"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed := &models.ParsedQuery{
				Normalized: tt.normalized,
				Tokens:     tt.tokens,
				Fields:     make(map[string]string),
			}
			intent := ic.Classify(parsed)
			if intent != models.IntentAutocomplete {
				t.Errorf("expected IntentAutocomplete for %q, got %v", tt.normalized, intent)
			}
		})
	}
}

func TestIntentClassifier_Classify_NotAutocompleteWhenLong(t *testing.T) {
	ic := NewIntentClassifier()
	parsed := &models.ParsedQuery{
		Normalized: "laptop",
		Tokens:     []string{"laptop"},
		Fields:     make(map[string]string),
	}

	intent := ic.Classify(parsed)
	if intent == models.IntentAutocomplete {
		t.Error("should not be autocomplete for longer single word")
	}
}

func TestIntentClassifier_Classify_AnalyticsKeywords(t *testing.T) {
	ic := NewIntentClassifier()

	keywords := []string{
		"count", "total", "average", "avg", "sum",
		"stats", "trending", "report", "analytics",
		"aggregate", "histogram", "breakdown",
	}

	for _, kw := range keywords {
		t.Run(kw, func(t *testing.T) {
			parsed := &models.ParsedQuery{
				Normalized: kw + " laptops",
				Tokens:     []string{kw, "laptops"},
				Fields:     make(map[string]string),
			}
			intent := ic.Classify(parsed)
			if intent != models.IntentAnalytics {
				t.Errorf("expected IntentAnalytics for leading token %q, got %v", kw, intent)
			}
		})
	}
}

func TestIntentClassifier_Classify_AnalyticsKeywordNotLeading(t *testing.T) {
	ic := NewIntentClassifier()
	parsed := &models.ParsedQuery{
		Normalized: "popular laptops count",
		Tokens:     []string{"popular", "laptops", "count"},
		Fields:     make(map[string]string),
	}

	intent := ic.Classify(parsed)
	// "count" is not the leading token, should not be analytics
	if intent == models.IntentAnalytics {
		t.Error("should not classify as analytics when keyword is not the leading token")
	}
}

func TestIntentClassifier_Classify_FacetedKeywords(t *testing.T) {
	ic := NewIntentClassifier()

	keywords := []string{"filter", "facet", "group"}

	for _, kw := range keywords {
		t.Run(kw, func(t *testing.T) {
			parsed := &models.ParsedQuery{
				Normalized: kw + " by category",
				Tokens:     []string{kw, "category"},
				Fields:     make(map[string]string),
			}
			intent := ic.Classify(parsed)
			if intent != models.IntentFaceted {
				t.Errorf("expected IntentFaceted for leading token %q, got %v", kw, intent)
			}
		})
	}
}

func TestIntentClassifier_Classify_FacetedViaFields(t *testing.T) {
	ic := NewIntentClassifier()
	parsed := &models.ParsedQuery{
		Normalized: "laptops",
		Tokens:     []string{"laptops"},
		Fields: map[string]string{
			"filter": "electronics",
		},
	}

	intent := ic.Classify(parsed)
	if intent != models.IntentFaceted {
		t.Errorf("expected IntentFaceted via field, got %v", intent)
	}
}

func TestIntentClassifier_Classify_FullText(t *testing.T) {
	ic := NewIntentClassifier()

	tests := []struct {
		name   string
		parsed *models.ParsedQuery
	}{
		{
			"regular query",
			&models.ParsedQuery{
				Normalized: "best gaming laptops 2024",
				Tokens:     []string{"best", "gaming", "laptops", "2024"},
				Fields:     make(map[string]string),
			},
		},
		{
			"multi-word query",
			&models.ParsedQuery{
				Normalized: "how to fix broken screen",
				Tokens:     []string{"how", "fix", "broken", "screen"},
				Fields:     make(map[string]string),
			},
		},
		{
			"non-keyword field",
			&models.ParsedQuery{
				Normalized: "laptop",
				Tokens:     []string{"laptop"},
				Fields: map[string]string{
					"brand": "apple",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			intent := ic.Classify(tt.parsed)
			if intent != models.IntentFullText {
				t.Errorf("expected IntentFullText, got %v", intent)
			}
		})
	}
}

func TestIntentClassifier_Classify_AnalyticsTakesPriorityOverFaceted(t *testing.T) {
	ic := NewIntentClassifier()
	// "count" is analytics, first token
	parsed := &models.ParsedQuery{
		Normalized: "count filter items",
		Tokens:     []string{"count", "filter", "items"},
		Fields:     make(map[string]string),
	}

	intent := ic.Classify(parsed)
	if intent != models.IntentAnalytics {
		t.Errorf("expected IntentAnalytics (leading token priority), got %v", intent)
	}
}

func TestIntentClassifier_AutocompleteMaxLen(t *testing.T) {
	ic := NewIntentClassifier()
	if ic.autocompleteMaxLen != 3 {
		t.Errorf("expected autocompleteMaxLen 3, got %d", ic.autocompleteMaxLen)
	}
}
