package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func newTestHandler() *Handler {
	return &Handler{
		logger: zap.NewNop(),
	}
}

func TestParseSearchRequest_GET(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/search?q=laptop&page=2&page_size=30&region=us-east&sort=newest&user_id=u123&force_fresh=true", nil)

	sr, err := h.parseSearchRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sr.Query != "laptop" {
		t.Errorf("expected query 'laptop', got %q", sr.Query)
	}
	if sr.Page != 2 {
		t.Errorf("expected page 2, got %d", sr.Page)
	}
	if sr.PageSize != 30 {
		t.Errorf("expected page_size 30, got %d", sr.PageSize)
	}
	if sr.Region != "us-east" {
		t.Errorf("expected region 'us-east', got %q", sr.Region)
	}
	if sr.Sort != "newest" {
		t.Errorf("expected sort 'newest', got %q", sr.Sort)
	}
	if sr.UserID != "u123" {
		t.Errorf("expected user_id 'u123', got %q", sr.UserID)
	}
	if !sr.ForceFresh {
		t.Error("expected ForceFresh true")
	}
}

func TestParseSearchRequest_GET_Defaults(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/search?q=laptop", nil)
	sr, err := h.parseSearchRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sr.Page != 0 {
		t.Errorf("expected default page 0, got %d", sr.Page)
	}
	if sr.PageSize != 0 {
		t.Errorf("expected default page_size 0, got %d", sr.PageSize)
	}
	if sr.ForceFresh {
		t.Error("expected ForceFresh false by default")
	}
}

func TestParseSearchRequest_GET_InvalidPage(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/search?q=laptop&page=abc", nil)
	sr, err := h.parseSearchRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Invalid page should default to 0
	if sr.Page != 0 {
		t.Errorf("expected page 0 for invalid input, got %d", sr.Page)
	}
}

func TestParseSearchRequest_GET_NegativePage(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/search?q=laptop&page=-1", nil)
	sr, err := h.parseSearchRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Negative page should be ignored (stays at default 0)
	if sr.Page != 0 {
		t.Errorf("expected page 0 for negative input, got %d", sr.Page)
	}
}

func TestParseSearchRequest_GET_InvalidPageSize(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/search?q=laptop&page_size=abc", nil)
	sr, err := h.parseSearchRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sr.PageSize != 0 {
		t.Errorf("expected page_size 0 for invalid input, got %d", sr.PageSize)
	}
}

func TestParseSearchRequest_GET_ZeroPageSize(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/search?q=laptop&page_size=0", nil)
	sr, err := h.parseSearchRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sr.PageSize != 0 {
		t.Errorf("expected page_size 0, got %d", sr.PageSize)
	}
}

func TestParseSearchRequest_GET_ForceFreshVariants(t *testing.T) {
	h := newTestHandler()

	tests := []struct {
		value string
		want  bool
	}{
		{"true", true},
		{"false", false},
		{"1", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			url := "/search?q=laptop"
			if tt.value != "" {
				url += "&force_fresh=" + tt.value
			}
			req := httptest.NewRequest(http.MethodGet, url, nil)
			sr, err := h.parseSearchRequest(req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sr.ForceFresh != tt.want {
				t.Errorf("force_fresh=%q: expected %v, got %v", tt.value, tt.want, sr.ForceFresh)
			}
		})
	}
}

func TestParseSearchRequest_POST(t *testing.T) {
	h := newTestHandler()

	body := `{"query":"laptop","page":1,"page_size":25,"region":"eu-west","sort":"popular","force_fresh":true}`
	req := httptest.NewRequest(http.MethodPost, "/search", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	sr, err := h.parseSearchRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sr.Query != "laptop" {
		t.Errorf("expected query 'laptop', got %q", sr.Query)
	}
	if sr.Page != 1 {
		t.Errorf("expected page 1, got %d", sr.Page)
	}
	if sr.PageSize != 25 {
		t.Errorf("expected page_size 25, got %d", sr.PageSize)
	}
	if sr.Region != "eu-west" {
		t.Errorf("expected region 'eu-west', got %q", sr.Region)
	}
	if !sr.ForceFresh {
		t.Error("expected ForceFresh true")
	}
}

func TestParseSearchRequest_POST_InvalidJSON(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/search", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")

	_, err := h.parseSearchRequest(req)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseSearchRequest_POST_EmptyBody(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/search", strings.NewReader(""))
	_, err := h.parseSearchRequest(req)
	if err == nil {
		t.Error("expected error for empty body")
	}
}

func TestParseSearchRequest_POST_WithFilters(t *testing.T) {
	h := newTestHandler()

	body := `{"query":"laptop","filters":{"category":"electronics","status":"active"}}`
	req := httptest.NewRequest(http.MethodPost, "/search", strings.NewReader(body))

	sr, err := h.parseSearchRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sr.Filters == nil {
		t.Fatal("expected filters")
	}
	if sr.Filters["category"] != "electronics" {
		t.Errorf("expected category=electronics, got %v", sr.Filters["category"])
	}
}

func TestWriteJSON(t *testing.T) {
	h := newTestHandler()
	rr := httptest.NewRecorder()

	data := map[string]string{"hello": "world"}
	h.writeJSON(rr, http.StatusOK, data)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "application/json" {
		t.Error("expected application/json content type")
	}

	var result map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["hello"] != "world" {
		t.Errorf("unexpected response: %v", result)
	}
}

func TestWriteError(t *testing.T) {
	h := newTestHandler()
	rr := httptest.NewRecorder()

	h.writeError(rr, http.StatusBadRequest, "invalid_query", "Query is required")

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}

	var result map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["error"] != "Query is required" {
		t.Errorf("expected error message 'Query is required', got %q", result["error"])
	}
	if result["code"] != "invalid_query" {
		t.Errorf("expected code 'invalid_query', got %q", result["code"])
	}
}

func TestWriteJSON_StatusCodes(t *testing.T) {
	h := newTestHandler()

	codes := []int{200, 201, 204, 400, 404, 500, 503}
	for _, code := range codes {
		rr := httptest.NewRecorder()
		h.writeJSON(rr, code, map[string]string{})
		if rr.Code != code {
			t.Errorf("expected %d, got %d", code, rr.Code)
		}
	}
}

func TestSearch_MissingQuery(t *testing.T) {
	h := newTestHandler()

	// GET without q param
	req := httptest.NewRequest(http.MethodGet, "/search", nil)
	rr := httptest.NewRecorder()

	h.Search(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing query, got %d", rr.Code)
	}

	var result map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if result["code"] != "missing_query" {
		t.Errorf("expected code 'missing_query', got %q", result["code"])
	}
}

func TestSearch_InvalidPOSTBody(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/search", strings.NewReader("not json"))
	rr := httptest.NewRecorder()

	h.Search(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid body, got %d", rr.Code)
	}
}

func TestMaxAutocompletePrefixLen(t *testing.T) {
	if maxAutocompletePrefixLen != 100 {
		t.Errorf("expected maxAutocompletePrefixLen 100, got %d", maxAutocompletePrefixLen)
	}
}

func TestMaxRequestBodySize(t *testing.T) {
	if maxRequestBodySize != 1<<20 {
		t.Errorf("expected maxRequestBodySize 1MB, got %d", maxRequestBodySize)
	}
}
