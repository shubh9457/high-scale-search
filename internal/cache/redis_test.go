package cache

import (
	"testing"
	"time"

	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/models"
)

func TestHashString(t *testing.T) {
	// Deterministic
	h1 := hashString("test")
	h2 := hashString("test")
	if h1 != h2 {
		t.Errorf("hashString not deterministic: %q != %q", h1, h2)
	}

	// Different inputs produce different hashes
	h3 := hashString("other")
	if h1 == h3 {
		t.Error("different inputs should produce different hashes")
	}

	// Non-empty
	if h1 == "" {
		t.Error("hash should not be empty")
	}

	// Empty string is valid
	h4 := hashString("")
	if h4 == "" {
		t.Error("hash of empty string should not be empty")
	}
}

func TestCanonicalFilters_Empty(t *testing.T) {
	result := canonicalFilters(nil)
	if result != "" {
		t.Errorf("expected empty string for nil filters, got %q", result)
	}

	result = canonicalFilters(map[string]any{})
	if result != "" {
		t.Errorf("expected empty string for empty filters, got %q", result)
	}
}

func TestCanonicalFilters_SingleFilter(t *testing.T) {
	result := canonicalFilters(map[string]any{
		"category": "electronics",
	})
	if result != "category=electronics" {
		t.Errorf("expected 'category=electronics', got %q", result)
	}
}

func TestCanonicalFilters_SortedKeys(t *testing.T) {
	// Same filters in any insertion order should produce same result
	filters := map[string]any{
		"category": "electronics",
		"brand":    "apple",
		"status":   "active",
	}

	result := canonicalFilters(filters)
	expected := "brand=apple,category=electronics,status=active"
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestCanonicalFilters_Deterministic(t *testing.T) {
	filters := map[string]any{
		"z_field": "last",
		"a_field": "first",
		"m_field": "middle",
	}

	r1 := canonicalFilters(filters)
	r2 := canonicalFilters(filters)
	if r1 != r2 {
		t.Errorf("canonicalFilters not deterministic: %q != %q", r1, r2)
	}
}

func TestBuildSearchKey_Deterministic(t *testing.T) {
	rc := &RedisCache{}

	req := &models.SearchRequest{
		Query:    "laptop",
		Page:     0,
		PageSize: 20,
		Region:   "us-east",
		Sort:     "relevance",
	}

	k1 := rc.buildSearchKey(req)
	k2 := rc.buildSearchKey(req)
	if k1 != k2 {
		t.Errorf("buildSearchKey not deterministic: %q != %q", k1, k2)
	}
	if k1 == "" {
		t.Error("search key should not be empty")
	}
	// Should have sr: prefix
	if len(k1) < 3 || k1[:3] != "sr:" {
		t.Errorf("expected 'sr:' prefix, got %q", k1)
	}
}

func TestBuildSearchKey_DifferentQueriesProduceDifferentKeys(t *testing.T) {
	rc := &RedisCache{}

	req1 := &models.SearchRequest{Query: "laptop", PageSize: 20}
	req2 := &models.SearchRequest{Query: "desktop", PageSize: 20}

	k1 := rc.buildSearchKey(req1)
	k2 := rc.buildSearchKey(req2)
	if k1 == k2 {
		t.Error("different queries should produce different keys")
	}
}

func TestBuildSearchKey_DifferentPagesProduceDifferentKeys(t *testing.T) {
	rc := &RedisCache{}

	req1 := &models.SearchRequest{Query: "laptop", Page: 0, PageSize: 20}
	req2 := &models.SearchRequest{Query: "laptop", Page: 1, PageSize: 20}

	k1 := rc.buildSearchKey(req1)
	k2 := rc.buildSearchKey(req2)
	if k1 == k2 {
		t.Error("different pages should produce different keys")
	}
}

func TestBuildSearchKey_FiltersAffectKey(t *testing.T) {
	rc := &RedisCache{}

	req1 := &models.SearchRequest{Query: "laptop", PageSize: 20}
	req2 := &models.SearchRequest{
		Query:    "laptop",
		PageSize: 20,
		Filters:  map[string]any{"category": "electronics"},
	}

	k1 := rc.buildSearchKey(req1)
	k2 := rc.buildSearchKey(req2)
	if k1 == k2 {
		t.Error("filters should affect cache key")
	}
}

func TestBuildSearchKey_FilterOrderDoesNotAffectKey(t *testing.T) {
	rc := &RedisCache{}

	// Go maps don't guarantee order, but canonicalFilters sorts keys
	req := &models.SearchRequest{
		Query:    "laptop",
		PageSize: 20,
		Filters: map[string]any{
			"brand":    "apple",
			"category": "electronics",
		},
	}

	k1 := rc.buildSearchKey(req)
	k2 := rc.buildSearchKey(req)
	if k1 != k2 {
		t.Error("same filters should produce same key regardless of iteration order")
	}
}

func TestBuildStaleKey_HasStalePrefix(t *testing.T) {
	rc := &RedisCache{}

	req := &models.SearchRequest{Query: "laptop", PageSize: 20}
	key := rc.buildStaleKey(req)

	if len(key) < 9 || key[:9] != "sr:stale:" {
		t.Errorf("expected 'sr:stale:' prefix, got %q", key)
	}
}

func TestBuildStaleKey_DifferentFromSearchKey(t *testing.T) {
	rc := &RedisCache{}

	req := &models.SearchRequest{Query: "laptop", PageSize: 20}
	searchKey := rc.buildSearchKey(req)
	staleKey := rc.buildStaleKey(req)

	if searchKey == staleKey {
		t.Error("search key and stale key should be different")
	}
}

func TestTtlForIntent(t *testing.T) {
	rc := &RedisCache{
		ttl: config.CacheTTLConfig{
			Autocomplete:  10 * time.Minute,
			SearchResults: 2 * time.Minute,
			FacetCounts:   5 * time.Minute,
		},
	}

	tests := []struct {
		intent string
		want   time.Duration
	}{
		{"autocomplete", 10 * time.Minute},
		{"analytics", 5 * time.Minute},
		{"faceted", 5 * time.Minute},
		{"fulltext", 2 * time.Minute},
		{"unknown", 2 * time.Minute},
		{"", 2 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.intent, func(t *testing.T) {
			got := rc.ttlForIntent(tt.intent)
			if got != tt.want {
				t.Errorf("ttlForIntent(%q) = %v, want %v", tt.intent, got, tt.want)
			}
		})
	}
}
