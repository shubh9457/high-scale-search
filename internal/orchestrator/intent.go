package orchestrator

import (
	"strings"

	"github.com/shubhsaxena/high-scale-search/internal/models"
)

type IntentClassifier struct {
	analyticsKeywords  map[string]bool
	facetedKeywords    map[string]bool
	autocompleteMaxLen int
}

func NewIntentClassifier() *IntentClassifier {
	return &IntentClassifier{
		analyticsKeywords: map[string]bool{
			"count":     true,
			"total":     true,
			"average":   true,
			"avg":       true,
			"sum":       true,
			"stats":     true,
			"trending":  true,
			"popular":   true,
			"top":       true,
			"report":    true,
			"analytics": true,
			"aggregate": true,
			"histogram": true,
			"breakdown": true,
		},
		facetedKeywords: map[string]bool{
			"category":  true,
			"filter":    true,
			"facet":     true,
			"group":     true,
			"type":      true,
			"brand":     true,
			"price":     true,
			"color":     true,
			"size":      true,
		},
		autocompleteMaxLen: 3,
	}
}

func (ic *IntentClassifier) Classify(parsed *models.ParsedQuery) models.Intent {
	if len(parsed.Normalized) == 0 {
		return models.IntentFullText
	}

	// Short queries are likely autocomplete
	if len(parsed.Tokens) <= 1 && len(parsed.Normalized) <= ic.autocompleteMaxLen {
		return models.IntentAutocomplete
	}

	// Check for analytics intent
	lower := strings.ToLower(parsed.Normalized)
	for kw := range ic.analyticsKeywords {
		if strings.Contains(lower, kw) {
			return models.IntentAnalytics
		}
	}

	// Check for faceted intent via field queries or faceted keywords
	if len(parsed.Fields) > 0 {
		for field := range parsed.Fields {
			if ic.facetedKeywords[strings.ToLower(field)] {
				return models.IntentFaceted
			}
		}
	}

	for kw := range ic.facetedKeywords {
		if strings.Contains(lower, kw) {
			return models.IntentFaceted
		}
	}

	return models.IntentFullText
}
