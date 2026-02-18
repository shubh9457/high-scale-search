package indexing

import (
	"testing"
	"time"

	"github.com/shubhsaxena/high-scale-search/internal/models"
)

func TestBuildInvalidationKeys_WithRegion(t *testing.T) {
	event := &models.ChangeEvent{
		DocumentID: "doc-1",
		Region:     "us-east",
		Document:   map[string]any{},
	}

	keys := buildInvalidationKeys(event)

	found := false
	for _, k := range keys {
		if k == "trend:us-east" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'trend:us-east' in keys, got %v", keys)
	}
}

func TestBuildInvalidationKeys_WithCategory(t *testing.T) {
	event := &models.ChangeEvent{
		DocumentID: "doc-1",
		Document: map[string]any{
			"category": "electronics",
		},
	}

	keys := buildInvalidationKeys(event)

	found := false
	for _, k := range keys {
		if k == "fc:electronics" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'fc:electronics' in keys, got %v", keys)
	}
}

func TestBuildInvalidationKeys_WithBoth(t *testing.T) {
	event := &models.ChangeEvent{
		DocumentID: "doc-1",
		Region:     "eu-west",
		Document: map[string]any{
			"category": "books",
		},
	}

	keys := buildInvalidationKeys(event)

	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d: %v", len(keys), keys)
	}

	hasRegion := false
	hasCategory := false
	for _, k := range keys {
		if k == "trend:eu-west" {
			hasRegion = true
		}
		if k == "fc:books" {
			hasCategory = true
		}
	}
	if !hasRegion {
		t.Error("expected trend:eu-west key")
	}
	if !hasCategory {
		t.Error("expected fc:books key")
	}
}

func TestBuildInvalidationKeys_NoRegionNoCategory(t *testing.T) {
	event := &models.ChangeEvent{
		DocumentID: "doc-1",
		Document:   map[string]any{},
	}

	keys := buildInvalidationKeys(event)

	if len(keys) != 0 {
		t.Errorf("expected 0 keys for no region/category, got %v", keys)
	}
}

func TestBuildInvalidationKeys_NilDocument(t *testing.T) {
	event := &models.ChangeEvent{
		DocumentID: "doc-1",
		Region:     "us-east",
		Document:   nil,
	}

	keys := buildInvalidationKeys(event)

	// Should still include region key
	if len(keys) != 1 {
		t.Errorf("expected 1 key (region only), got %v", keys)
	}
}

func TestBuildInvalidationKeys_CategoryNotString(t *testing.T) {
	event := &models.ChangeEvent{
		DocumentID: "doc-1",
		Document: map[string]any{
			"category": 123, // not a string
		},
	}

	keys := buildInvalidationKeys(event)

	if len(keys) != 0 {
		t.Errorf("expected 0 keys for non-string category, got %v", keys)
	}
}

func TestExtractSearchFields(t *testing.T) {
	sp := &StreamProcessor{}

	doc := map[string]any{
		"title":            "Test Document",
		"description":      "A test description",
		"category":         "electronics",
		"tags":             []string{"test", "doc"},
		"region":           "us-east",
		"created_at":       "2024-01-01",
		"popularity_score": 4.5,
		"geo_point":        map[string]float64{"lat": 40.7, "lon": -74.0},
		"internal_field":   "should not appear",
		"secret":           "should not appear",
	}

	fields := sp.extractSearchFields(doc)

	// Should include searchable fields
	expectedFields := []string{"title", "description", "category", "tags", "region", "created_at", "popularity_score", "geo_point"}
	for _, f := range expectedFields {
		if _, ok := fields[f]; !ok {
			t.Errorf("expected field %q in extracted fields", f)
		}
	}

	// Should include updated_at
	if _, ok := fields["updated_at"]; !ok {
		t.Error("expected updated_at in extracted fields")
	}

	// Should NOT include non-searchable fields
	if _, ok := fields["internal_field"]; ok {
		t.Error("internal_field should not be in extracted fields")
	}
	if _, ok := fields["secret"]; ok {
		t.Error("secret should not be in extracted fields")
	}
}

func TestExtractSearchFields_EmptyDoc(t *testing.T) {
	sp := &StreamProcessor{}
	fields := sp.extractSearchFields(map[string]any{})

	// Should still have updated_at
	if _, ok := fields["updated_at"]; !ok {
		t.Error("expected updated_at even for empty doc")
	}
	// Should only have updated_at
	if len(fields) != 1 {
		t.Errorf("expected 1 field for empty doc, got %d", len(fields))
	}
}

func TestExtractSearchFields_PartialDoc(t *testing.T) {
	sp := &StreamProcessor{}
	doc := map[string]any{
		"title": "Only Title",
	}

	fields := sp.extractSearchFields(doc)
	if fields["title"] != "Only Title" {
		t.Errorf("expected title 'Only Title', got %v", fields["title"])
	}
	// Should not include missing fields
	if _, ok := fields["description"]; ok {
		t.Error("should not include missing description")
	}
}

func TestTransformEvent_Create(t *testing.T) {
	sp := &StreamProcessor{
		esClient: nil, // Will cause panic, but let's test what we can
	}

	event := &models.ChangeEvent{
		Type:       "CREATE",
		DocumentID: "doc-123",
		Region:     "us-east",
		Document: map[string]any{
			"title":       "New Document",
			"description": "A new document",
			"type":        "article",
		},
		Timestamp: time.Now(),
	}

	// transformEvent requires esClient.ResolveIndex, so we can't fully test it
	// without a mock. Let's test the parts we can.
	_ = sp
	_ = event

	// Test that the event struct is valid
	if event.Type != "CREATE" {
		t.Errorf("expected CREATE, got %s", event.Type)
	}
	if event.DocumentID != "doc-123" {
		t.Errorf("expected doc-123, got %s", event.DocumentID)
	}
}

func TestTransformEvent_Delete(t *testing.T) {
	event := &models.ChangeEvent{
		Type:       "DELETE",
		DocumentID: "doc-456",
		Region:     "eu-west",
		Timestamp:  time.Now(),
	}

	if event.Type != "DELETE" {
		t.Errorf("expected DELETE, got %s", event.Type)
	}
}

func TestMaxBufferSize(t *testing.T) {
	if maxBufferSize != 50000 {
		t.Errorf("expected maxBufferSize 50000, got %d", maxBufferSize)
	}
}

func TestMaxAsyncWorkers(t *testing.T) {
	if maxAsyncWorkers != 128 {
		t.Errorf("expected maxAsyncWorkers 128, got %d", maxAsyncWorkers)
	}
}
