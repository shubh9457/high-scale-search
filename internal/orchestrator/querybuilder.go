package orchestrator

import (
	"github.com/shubhsaxena/high-scale-search/internal/models"
)

type QueryBuilder struct{}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func (qb *QueryBuilder) BuildESQuery(parsed *models.ParsedQuery, req *models.SearchRequest) map[string]any {
	query := make(map[string]any)

	// Build the main query
	var boolQuery map[string]any

	if parsed.IsPhrase {
		boolQuery = map[string]any{
			"must": []map[string]any{
				{
					"multi_match": map[string]any{
						"query":  parsed.Normalized,
						"type":   "phrase",
						"fields": []string{"title^3", "description^2", "tags"},
					},
				},
			},
		}
	} else if parsed.HasWildcard {
		boolQuery = map[string]any{
			"must": []map[string]any{
				{
					"query_string": map[string]any{
						"query":            parsed.Normalized,
						"fields":           []string{"title^3", "description^2", "tags"},
						"default_operator": "AND",
					},
				},
			},
		}
	} else {
		boolQuery = map[string]any{
			"must": []map[string]any{
				{
					"multi_match": map[string]any{
						"query":     parsed.Normalized,
						"type":      "best_fields",
						"fields":    []string{"title^3", "description^2", "tags"},
						"fuzziness": "AUTO",
						"tie_breaker": 0.3,
					},
				},
			},
		}
	}

	// Add field-specific queries
	if len(parsed.Fields) > 0 {
		var fieldFilters []map[string]any
		for field, value := range parsed.Fields {
			fieldFilters = append(fieldFilters, map[string]any{
				"term": map[string]any{
					field: value,
				},
			})
		}
		boolQuery["filter"] = fieldFilters
	}

	// Add request-level filters
	if len(req.Filters) > 0 {
		var filters []map[string]any
		if existing, ok := boolQuery["filter"]; ok {
			filters = existing.([]map[string]any)
		}
		for field, value := range req.Filters {
			filters = append(filters, map[string]any{
				"term": map[string]any{
					field: value,
				},
			})
		}
		boolQuery["filter"] = filters
	}

	// Add region routing boost
	if req.Region != "" {
		boolQuery["should"] = []map[string]any{
			{
				"term": map[string]any{
					"region": map[string]any{
						"value": req.Region,
						"boost": 1.5,
					},
				},
			},
		}
	}

	query["query"] = map[string]any{
		"bool": boolQuery,
	}

	// Script score for popularity boosting
	query["query"] = map[string]any{
		"script_score": map[string]any{
			"query": map[string]any{
				"bool": boolQuery,
			},
			"script": map[string]any{
				"source": "_score * (1 + Math.log1p(doc['popularity_score'].value))",
			},
		},
	}

	// Pagination
	from := req.Page * req.PageSize
	query["from"] = from
	query["size"] = req.PageSize

	// Highlighting
	query["highlight"] = map[string]any{
		"fields": map[string]any{
			"title":       map[string]any{},
			"description": map[string]any{"fragment_size": 150},
		},
		"pre_tags":  []string{"<em>"},
		"post_tags": []string{"</em>"},
	}

	// Sorting
	if req.Sort != "" {
		switch req.Sort {
		case "relevance":
			// default ES score sort
		case "newest":
			query["sort"] = []map[string]any{
				{"created_at": map[string]any{"order": "desc"}},
				{"_score": map[string]any{"order": "desc"}},
			}
		case "popular":
			query["sort"] = []map[string]any{
				{"popularity_score": map[string]any{"order": "desc"}},
				{"_score": map[string]any{"order": "desc"}},
			}
		}
	}

	// Suggest for spell correction
	query["suggest"] = map[string]any{
		"text": parsed.Original,
		"spell_suggest": map[string]any{
			"phrase": map[string]any{
				"field":     "title.suggest",
				"size":      1,
				"gram_size": 3,
				"confidence": 1.0,
			},
		},
	}

	return query
}

func (qb *QueryBuilder) BuildAutocompleteQuery(prefix string, size int) map[string]any {
	return map[string]any{
		"size": 0,
		"suggest": map[string]any{
			"autocomplete": map[string]any{
				"prefix": prefix,
				"completion": map[string]any{
					"field":           "title.autocomplete",
					"size":            size,
					"skip_duplicates": true,
					"fuzzy": map[string]any{
						"fuzziness": "AUTO",
					},
				},
			},
		},
	}
}
