package orchestrator

import (
	"testing"

	"github.com/shubhsaxena/high-scale-search/internal/models"
)

func TestQueryBuilder_BuildESQuery_BasicQuery(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Original:   "laptop review",
		Normalized: "laptop review",
		Tokens:     []string{"laptop", "review"},
	}
	req := &models.SearchRequest{
		Query:    "laptop review",
		Page:     0,
		PageSize: 20,
	}

	query := qb.BuildESQuery(parsed, req)

	// Verify top-level structure
	if _, ok := query["query"]; !ok {
		t.Error("expected 'query' key in result")
	}
	if _, ok := query["from"]; !ok {
		t.Error("expected 'from' key in result")
	}
	if _, ok := query["size"]; !ok {
		t.Error("expected 'size' key in result")
	}
	if _, ok := query["highlight"]; !ok {
		t.Error("expected 'highlight' key in result")
	}
	if _, ok := query["suggest"]; !ok {
		t.Error("expected 'suggest' key in result")
	}

	// Verify pagination
	if query["from"] != 0 {
		t.Errorf("expected from=0, got %v", query["from"])
	}
	if query["size"] != 20 {
		t.Errorf("expected size=20, got %v", query["size"])
	}
}

func TestQueryBuilder_BuildESQuery_PhraseQuery(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Original:   `"gaming laptop"`,
		Normalized: `"gaming laptop"`,
		Tokens:     []string{"gaming", "laptop"},
		IsPhrase:   true,
		HasQuotes:  true,
	}
	req := &models.SearchRequest{
		Query:    `"gaming laptop"`,
		PageSize: 10,
	}

	query := qb.BuildESQuery(parsed, req)

	scriptScore, ok := query["query"].(map[string]any)["script_score"].(map[string]any)
	if !ok {
		t.Fatal("expected script_score in query")
	}
	boolQuery, ok := scriptScore["query"].(map[string]any)["bool"].(map[string]any)
	if !ok {
		t.Fatal("expected bool query in script_score")
	}
	must, ok := boolQuery["must"].([]map[string]any)
	if !ok || len(must) == 0 {
		t.Fatal("expected must clause")
	}

	multiMatch, ok := must[0]["multi_match"].(map[string]any)
	if !ok {
		t.Fatal("expected multi_match in must clause")
	}
	if multiMatch["type"] != "phrase" {
		t.Errorf("expected phrase type, got %v", multiMatch["type"])
	}
}

func TestQueryBuilder_BuildESQuery_WildcardQuery(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Original:    "lap*",
		Normalized:  "lap*",
		Tokens:      []string{"lap*"},
		HasWildcard: true,
	}
	req := &models.SearchRequest{
		Query:    "lap*",
		PageSize: 10,
	}

	query := qb.BuildESQuery(parsed, req)

	scriptScore := query["query"].(map[string]any)["script_score"].(map[string]any)
	boolQuery := scriptScore["query"].(map[string]any)["bool"].(map[string]any)
	must := boolQuery["must"].([]map[string]any)

	if _, ok := must[0]["query_string"]; !ok {
		t.Error("expected query_string for wildcard query")
	}
	qs := must[0]["query_string"].(map[string]any)
	if qs["default_operator"] != "AND" {
		t.Errorf("expected default_operator AND, got %v", qs["default_operator"])
	}
}

func TestQueryBuilder_BuildESQuery_WithFields(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Original:   "laptop category:electronics",
		Normalized: "laptop",
		Tokens:     []string{"laptop"},
		Fields: map[string]string{
			"category": "electronics",
		},
	}
	req := &models.SearchRequest{
		Query:    "laptop category:electronics",
		PageSize: 10,
	}

	query := qb.BuildESQuery(parsed, req)

	scriptScore := query["query"].(map[string]any)["script_score"].(map[string]any)
	boolQuery := scriptScore["query"].(map[string]any)["bool"].(map[string]any)

	filters, ok := boolQuery["filter"].([]map[string]any)
	if !ok {
		t.Fatal("expected filter in bool query")
	}
	if len(filters) == 0 {
		t.Error("expected at least one filter")
	}

	found := false
	for _, f := range filters {
		if term, ok := f["term"].(map[string]any); ok {
			if term["category"] == "electronics" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected category:electronics in filters")
	}
}

func TestQueryBuilder_BuildESQuery_WithRequestFilters(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Original:   "laptop",
		Normalized: "laptop",
		Tokens:     []string{"laptop"},
		Fields:     make(map[string]string),
	}
	req := &models.SearchRequest{
		Query:    "laptop",
		PageSize: 10,
		Filters: map[string]any{
			"status": "active",
		},
	}

	query := qb.BuildESQuery(parsed, req)

	scriptScore := query["query"].(map[string]any)["script_score"].(map[string]any)
	boolQuery := scriptScore["query"].(map[string]any)["bool"].(map[string]any)

	filters, ok := boolQuery["filter"].([]map[string]any)
	if !ok {
		t.Fatal("expected filter in bool query")
	}

	found := false
	for _, f := range filters {
		if term, ok := f["term"].(map[string]any); ok {
			if term["status"] == "active" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected status:active in filters")
	}
}

func TestQueryBuilder_BuildESQuery_WithRegion(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Original:   "laptop",
		Normalized: "laptop",
		Tokens:     []string{"laptop"},
		Fields:     make(map[string]string),
	}
	req := &models.SearchRequest{
		Query:    "laptop",
		PageSize: 10,
		Region:   "us-east",
	}

	query := qb.BuildESQuery(parsed, req)

	scriptScore := query["query"].(map[string]any)["script_score"].(map[string]any)
	boolQuery := scriptScore["query"].(map[string]any)["bool"].(map[string]any)

	should, ok := boolQuery["should"].([]map[string]any)
	if !ok {
		t.Fatal("expected should clause for region")
	}
	if len(should) == 0 {
		t.Error("expected at least one should clause")
	}
}

func TestQueryBuilder_BuildESQuery_Pagination(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Normalized: "laptop",
		Tokens:     []string{"laptop"},
		Fields:     make(map[string]string),
	}

	tests := []struct {
		name     string
		page     int
		pageSize int
		wantFrom int
		wantSize int
	}{
		{"page 0", 0, 20, 0, 20},
		{"page 1", 1, 20, 20, 20},
		{"page 5", 5, 20, 100, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &models.SearchRequest{
				Query:    "laptop",
				Page:     tt.page,
				PageSize: tt.pageSize,
			}
			query := qb.BuildESQuery(parsed, req)
			if query["from"] != tt.wantFrom {
				t.Errorf("expected from=%d, got %v", tt.wantFrom, query["from"])
			}
			if query["size"] != tt.pageSize {
				t.Errorf("expected size=%d, got %v", tt.pageSize, query["size"])
			}
		})
	}
}

func TestQueryBuilder_BuildESQuery_DeepPaginationGuard(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Normalized: "laptop",
		Tokens:     []string{"laptop"},
		Fields:     make(map[string]string),
	}

	// Page 1000 * 20 = 20000 which exceeds maxESFromPlusSize (10000)
	req := &models.SearchRequest{
		Query:    "laptop",
		Page:     1000,
		PageSize: 20,
	}
	query := qb.BuildESQuery(parsed, req)

	from, ok := query["from"].(int)
	if !ok {
		t.Fatal("expected 'from' to be int")
	}
	if from+req.PageSize > maxESFromPlusSize {
		t.Errorf("from+pageSize (%d) exceeds maxESFromPlusSize (%d)", from+req.PageSize, maxESFromPlusSize)
	}
}

func TestQueryBuilder_BuildESQuery_SortOptions(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Normalized: "laptop",
		Tokens:     []string{"laptop"},
		Fields:     make(map[string]string),
	}

	tests := []struct {
		sort    string
		hasSort bool
	}{
		{"relevance", false},
		{"newest", true},
		{"popular", true},
		{"", false},
		{"unknown_sort", false},
	}

	for _, tt := range tests {
		t.Run("sort_"+tt.sort, func(t *testing.T) {
			req := &models.SearchRequest{
				Query:    "laptop",
				PageSize: 10,
				Sort:     tt.sort,
			}
			query := qb.BuildESQuery(parsed, req)
			_, hasSort := query["sort"]
			if hasSort != tt.hasSort {
				t.Errorf("expected sort presence=%v for sort=%q, got %v", tt.hasSort, tt.sort, hasSort)
			}
		})
	}
}

func TestQueryBuilder_BuildESQuery_Highlight(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Normalized: "laptop",
		Tokens:     []string{"laptop"},
		Fields:     make(map[string]string),
	}
	req := &models.SearchRequest{Query: "laptop", PageSize: 10}

	query := qb.BuildESQuery(parsed, req)
	highlight, ok := query["highlight"].(map[string]any)
	if !ok {
		t.Fatal("expected highlight config")
	}
	preTags, ok := highlight["pre_tags"].([]string)
	if !ok || len(preTags) == 0 || preTags[0] != "<em>" {
		t.Error("expected pre_tags with <em>")
	}
	postTags, ok := highlight["post_tags"].([]string)
	if !ok || len(postTags) == 0 || postTags[0] != "</em>" {
		t.Error("expected post_tags with </em>")
	}
}

func TestQueryBuilder_BuildESQuery_Suggest(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Original:   "lapton",
		Normalized: "lapton",
		Tokens:     []string{"lapton"},
		Fields:     make(map[string]string),
	}
	req := &models.SearchRequest{Query: "lapton", PageSize: 10}

	query := qb.BuildESQuery(parsed, req)
	suggest, ok := query["suggest"].(map[string]any)
	if !ok {
		t.Fatal("expected suggest config")
	}
	if suggest["text"] != "lapton" {
		t.Errorf("expected suggest text 'lapton', got %v", suggest["text"])
	}
}

func TestQueryBuilder_BuildAutocompleteQuery(t *testing.T) {
	qb := NewQueryBuilder()
	query := qb.BuildAutocompleteQuery("lap", 5)

	if query["size"] != 0 {
		t.Errorf("expected size=0 for autocomplete, got %v", query["size"])
	}

	suggest, ok := query["suggest"].(map[string]any)
	if !ok {
		t.Fatal("expected suggest in autocomplete query")
	}

	ac, ok := suggest["autocomplete"].(map[string]any)
	if !ok {
		t.Fatal("expected autocomplete in suggest")
	}
	if ac["prefix"] != "lap" {
		t.Errorf("expected prefix 'lap', got %v", ac["prefix"])
	}

	completion, ok := ac["completion"].(map[string]any)
	if !ok {
		t.Fatal("expected completion in autocomplete")
	}
	if completion["field"] != "title.autocomplete" {
		t.Errorf("expected field 'title.autocomplete', got %v", completion["field"])
	}
	if completion["size"] != 5 {
		t.Errorf("expected size 5, got %v", completion["size"])
	}
	if completion["skip_duplicates"] != true {
		t.Error("expected skip_duplicates true")
	}
}

func TestQueryBuilder_BuildESQuery_CombinedFieldsAndRequestFilters(t *testing.T) {
	qb := NewQueryBuilder()
	parsed := &models.ParsedQuery{
		Original:   "laptop category:electronics",
		Normalized: "laptop",
		Tokens:     []string{"laptop"},
		Fields: map[string]string{
			"category": "electronics",
		},
	}
	req := &models.SearchRequest{
		Query:    "laptop",
		PageSize: 10,
		Filters: map[string]any{
			"status": "active",
		},
	}

	query := qb.BuildESQuery(parsed, req)

	scriptScore := query["query"].(map[string]any)["script_score"].(map[string]any)
	boolQuery := scriptScore["query"].(map[string]any)["bool"].(map[string]any)
	filters := boolQuery["filter"].([]map[string]any)

	if len(filters) < 2 {
		t.Errorf("expected at least 2 filters (field + request), got %d", len(filters))
	}
}
