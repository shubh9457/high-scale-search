package orchestrator

import (
	"testing"
)

func TestQueryParser_Parse_EmptyQuery(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("")

	if parsed.Original != "" {
		t.Errorf("expected empty original, got %q", parsed.Original)
	}
	if parsed.Normalized != "" {
		t.Errorf("expected empty normalized, got %q", parsed.Normalized)
	}
	if len(parsed.Tokens) != 0 {
		t.Errorf("expected no tokens, got %v", parsed.Tokens)
	}
	if parsed.HasWildcard {
		t.Error("expected no wildcard")
	}
	if parsed.HasQuotes {
		t.Error("expected no quotes")
	}
	if parsed.IsPhrase {
		t.Error("expected no phrase")
	}
	if len(parsed.Fields) != 0 {
		t.Errorf("expected no fields, got %v", parsed.Fields)
	}
}

func TestQueryParser_Parse_WhitespaceOnly(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("   ")

	if parsed.Normalized != "" {
		t.Errorf("expected empty normalized, got %q", parsed.Normalized)
	}
	if len(parsed.Tokens) != 0 {
		t.Errorf("expected no tokens, got %v", parsed.Tokens)
	}
}

func TestQueryParser_Parse_SimpleQuery(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("laptop computer")

	if parsed.Original != "laptop computer" {
		t.Errorf("expected original 'laptop computer', got %q", parsed.Original)
	}
	if parsed.Normalized != "laptop computer" {
		t.Errorf("expected normalized 'laptop computer', got %q", parsed.Normalized)
	}
	if len(parsed.Tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d: %v", len(parsed.Tokens), parsed.Tokens)
	}
	if parsed.Tokens[0] != "laptop" || parsed.Tokens[1] != "computer" {
		t.Errorf("expected [laptop, computer], got %v", parsed.Tokens)
	}
}

func TestQueryParser_Parse_StopWordRemoval(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("the best laptop in the world")

	// "the", "in" are stop words
	for _, token := range parsed.Tokens {
		if token == "the" || token == "in" {
			t.Errorf("stop word %q should have been removed", token)
		}
	}
	if len(parsed.Tokens) != 3 {
		t.Errorf("expected 3 tokens (best, laptop, world), got %d: %v", len(parsed.Tokens), parsed.Tokens)
	}
}

func TestQueryParser_Parse_AllStopWords(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("the a an")

	if len(parsed.Tokens) != 0 {
		t.Errorf("expected 0 tokens for all stop words, got %v", parsed.Tokens)
	}
}

func TestQueryParser_Parse_CaseNormalization(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("LAPTOP Computer")

	if parsed.Normalized != "laptop computer" {
		t.Errorf("expected normalized 'laptop computer', got %q", parsed.Normalized)
	}
}

func TestQueryParser_Parse_MultipleSpaces(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("laptop   computer    review")

	if parsed.Normalized != "laptop computer review" {
		t.Errorf("expected normalized 'laptop computer review', got %q", parsed.Normalized)
	}
}

func TestQueryParser_Parse_QuotedPhrase(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse(`"gaming laptop" review`)

	if !parsed.HasQuotes {
		t.Error("expected HasQuotes to be true")
	}
	if !parsed.IsPhrase {
		t.Error("expected IsPhrase to be true")
	}
}

func TestQueryParser_Parse_Wildcards(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"laptop*", true},
		{"lap?op", true},
		{"laptop", false},
		{"laptop* review", true},
		{"test?ing*", true},
	}

	qp := NewQueryParser()
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			parsed := qp.Parse(tt.query)
			if parsed.HasWildcard != tt.want {
				t.Errorf("Parse(%q).HasWildcard = %v, want %v", tt.query, parsed.HasWildcard, tt.want)
			}
		})
	}
}

func TestQueryParser_Parse_FieldValuePairs(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("category:electronics laptop")

	if val, ok := parsed.Fields["category"]; !ok || val != "electronics" {
		t.Errorf("expected Fields[category]=electronics, got %v", parsed.Fields)
	}
	// "category:electronics" should be stripped from query
	if parsed.Normalized == "" {
		t.Error("expected non-empty normalized after stripping field")
	}
}

func TestQueryParser_Parse_MultipleFieldValues(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("category:electronics brand:apple laptop")

	if val, ok := parsed.Fields["category"]; !ok || val != "electronics" {
		t.Errorf("expected Fields[category]=electronics, got %v", parsed.Fields)
	}
	if val, ok := parsed.Fields["brand"]; !ok || val != "apple" {
		t.Errorf("expected Fields[brand]=apple, got %v", parsed.Fields)
	}
}

func TestQueryParser_Parse_URLsNotTreatedAsFields(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("http://example.com laptop")

	if _, ok := parsed.Fields["http"]; ok {
		t.Error("http should not be treated as a field")
	}
	if _, ok := parsed.Fields["https"]; ok {
		t.Error("https should not be treated as a field")
	}
}

func TestQueryParser_Parse_HTTPSNotTreatedAsField(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("https://example.com search")

	if _, ok := parsed.Fields["https"]; ok {
		t.Error("https should not be treated as a field")
	}
}

func TestQueryParser_Parse_FTPNotTreatedAsField(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("ftp://files.example.com document")

	if _, ok := parsed.Fields["ftp"]; ok {
		t.Error("ftp should not be treated as a field")
	}
}

func TestQueryParser_Parse_PunctuationTrimming(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("laptop, computer, review.")

	for _, token := range parsed.Tokens {
		if token == "laptop," || token == "computer," || token == "review." {
			t.Errorf("punctuation not trimmed from token %q", token)
		}
	}
}

func TestQueryParser_Parse_PreservesOriginal(t *testing.T) {
	qp := NewQueryParser()
	original := "  Best LAPTOP  deals  "
	parsed := qp.Parse(original)

	if parsed.Original != original {
		t.Errorf("Original should be preserved as-is, got %q", parsed.Original)
	}
}

func TestQueryParser_Parse_WildcardTokensPreserved(t *testing.T) {
	qp := NewQueryParser()
	parsed := qp.Parse("lap*")

	found := false
	for _, token := range parsed.Tokens {
		if token == "lap*" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected wildcard token 'lap*' to be preserved, got %v", parsed.Tokens)
	}
}
