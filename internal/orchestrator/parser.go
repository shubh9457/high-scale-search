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
	// fieldPattern matches explicit field:value syntax but excludes URLs (http:, https:, ftp:)
	// and time-like patterns (10:30). Requires field name to be at least 2 chars and
	// start at a word boundary.
	fieldPattern      = regexp.MustCompile(`(?:^|\s)([a-zA-Z][a-zA-Z_]{1,}):(\S+)`)
	quotePattern      = regexp.MustCompile(`"([^"]+)"`)
	wildcardPattern   = regexp.MustCompile(`[*?]`)
	multiSpacePattern = regexp.MustCompile(`\s+`)

	// excludedFields are field-like prefixes that should not be treated as field:value queries
	excludedFields = map[string]bool{
		"http":  true,
		"https": true,
		"ftp":   true,
		"ftps":  true,
		"mailto": true,
	}
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

	// Extract field:value pairs, skipping URLs and time patterns
	fieldMatches := fieldPattern.FindAllStringSubmatch(query, -1)
	for _, m := range fieldMatches {
		field := strings.TrimSpace(m[1])
		if excludedFields[strings.ToLower(field)] {
			continue
		}
		parsed.Fields[field] = m[2]
	}
	// Only strip matched field:value pairs that were accepted
	for field, value := range parsed.Fields {
		query = strings.Replace(query, field+":"+value, "", 1)
	}

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
