package orchestrator

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/shubhsaxena/high-scale-search/internal/models"
)

type QueryParser struct {
	stopWords map[string]bool
}

func NewQueryParser() *QueryParser {
	stops := map[string]bool{
		"the": true, "a": true, "an": true, "and": true, "or": true,
		"but": true, "in": true, "on": true, "at": true, "to": true,
		"for": true, "of": true, "with": true, "by": true, "is": true,
		"it": true, "this": true, "that": true, "are": true, "was": true,
		"be": true, "has": true, "had": true, "do": true, "does": true,
	}
	return &QueryParser{stopWords: stops}
}

var (
	fieldPattern = regexp.MustCompile(`(\w+):(\S+)`)
	quotePattern = regexp.MustCompile(`"([^"]+)"`)
	wildcardPattern = regexp.MustCompile(`[*?]`)
	multiSpacePattern = regexp.MustCompile(`\s+`)
)

func (qp *QueryParser) Parse(rawQuery string) *models.ParsedQuery {
	parsed := &models.ParsedQuery{
		Original: rawQuery,
		Fields:   make(map[string]string),
	}

	query := strings.TrimSpace(rawQuery)
	if query == "" {
		return parsed
	}

	// Extract field:value pairs
	fieldMatches := fieldPattern.FindAllStringSubmatch(query, -1)
	for _, m := range fieldMatches {
		parsed.Fields[m[1]] = m[2]
	}
	query = fieldPattern.ReplaceAllString(query, "")

	// Detect quoted phrases
	quoteMatches := quotePattern.FindAllStringSubmatch(query, -1)
	if len(quoteMatches) > 0 {
		parsed.HasQuotes = true
		parsed.IsPhrase = true
	}

	// Detect wildcards
	parsed.HasWildcard = wildcardPattern.MatchString(query)

	// Normalize
	normalized := strings.ToLower(query)
	normalized = multiSpacePattern.ReplaceAllString(normalized, " ")
	normalized = strings.TrimSpace(normalized)
	parsed.Normalized = normalized

	// Tokenize and remove stop words
	words := strings.Fields(normalized)
	var tokens []string
	for _, w := range words {
		cleaned := strings.TrimFunc(w, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '*' && r != '?'
		})
		if cleaned != "" && !qp.stopWords[cleaned] {
			tokens = append(tokens, cleaned)
		}
	}
	parsed.Tokens = tokens

	return parsed
}
