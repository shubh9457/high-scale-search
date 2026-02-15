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
			"report":    true,
			"analytics": true,
			"aggregate": true,
			"histogram": true,
			"breakdown": true,
		},
		facetedKeywords: map[string]bool{
			"filter": true,
			"facet":  true,
			"group":  true,
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

	// Match intent only when a keyword is the leading token, not a substring.
	// This prevents "popular laptops" from routing to analytics just because
	// "popular" is an analytics keyword — it's being used as an adjective.
	if len(parsed.Tokens) > 0 {
		lead := parsed.Tokens[0]
		if ic.analyticsKeywords[lead] {
			return models.IntentAnalytics
		}
		if ic.facetedKeywords[lead] {
			return models.IntentFaceted
		}
	}

	// Check for faceted intent via explicit field:value syntax
	if len(parsed.Fields) > 0 {
		for field := range parsed.Fields {
			if ic.facetedKeywords[strings.ToLower(field)] {
				return models.IntentFaceted
			}
		}
	}

	return models.IntentFullText
}
