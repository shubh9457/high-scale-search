package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/cache"
	"github.com/shubhsaxena/high-scale-search/internal/models"
	"github.com/shubhsaxena/high-scale-search/internal/orchestrator"
)

const maxRequestBodySize = 1 << 20 // 1 MB

type Handler struct {
	orchestrator *orchestrator.Orchestrator
	cache        *cache.RedisCache
	logger       *zap.Logger
}

func NewHandler(orch *orchestrator.Orchestrator, cache *cache.RedisCache, logger *zap.Logger) *Handler {
	return &Handler{
		orchestrator: orch,
		cache:        cache,
		logger:       logger,
	}
}

func (h *Handler) Search(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	requestID := RequestIDFromContext(ctx)

	req, err := h.parseSearchRequest(r)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if req.Query == "" {
		h.writeError(w, http.StatusBadRequest, "missing_query", "Query parameter 'q' is required")
		return
	}
	req.RequestID = requestID

	resp, err := h.orchestrator.Search(ctx, req)
	if err != nil {
		h.logger.Error("search failed",
			zap.String("request_id", requestID),
			zap.String("query", req.Query),
			zap.Error(err),
		)
		h.writeError(w, http.StatusInternalServerError, "search_error", "Search service temporarily unavailable")
		return
	}

	h.writeJSON(w, http.StatusOK, resp)
}

const maxAutocompletePrefixLen = 100

func (h *Handler) Autocomplete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	prefix := r.URL.Query().Get("q")
	if prefix == "" {
		h.writeError(w, http.StatusBadRequest, "missing_query", "Query parameter 'q' is required")
		return
	}
	if len(prefix) > maxAutocompletePrefixLen {
		prefix = prefix[:maxAutocompletePrefixLen]
	}

	// Check cache first
	results, err := h.cache.GetAutocomplete(ctx, prefix)
	if err != nil {
		h.logger.Warn("autocomplete cache error", zap.Error(err))
	}
	if results != nil {
		h.writeJSON(w, http.StatusOK, map[string]any{
			"suggestions": results,
			"source":      "cache",
		})
		return
	}

	// Fall back to search
	req := &models.SearchRequest{
		Query:    prefix,
		PageSize: 10,
	}

	resp, err := h.orchestrator.Search(ctx, req)
	if err != nil {
		h.logger.Error("autocomplete search failed", zap.Error(err))
		h.writeJSON(w, http.StatusOK, map[string]any{
			"suggestions": []string{},
			"source":      "none",
		})
		return
	}

	suggestions := make([]string, 0, len(resp.Results))
	for _, r := range resp.Results {
		if r.Title != "" {
			suggestions = append(suggestions, r.Title)
		}
	}

	// Cache results
	if err := h.cache.SetAutocomplete(ctx, prefix, suggestions); err != nil {
		h.logger.Warn("autocomplete cache set error", zap.Error(err))
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"suggestions": suggestions,
		"source":      "search",
	})
}

func (h *Handler) Trending(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	region := r.URL.Query().Get("region")
	if region == "" {
		region = "global"
	}

	results, err := h.cache.GetTrending(ctx, region)
	if err != nil {
		h.logger.Warn("trending cache error", zap.Error(err))
		h.writeJSON(w, http.StatusOK, map[string]any{
			"trending": []string{},
			"region":   region,
		})
		return
	}

	if results == nil {
		results = []string{}
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"trending": results,
		"region":   region,
	})
}

func (h *Handler) parseSearchRequest(r *http.Request) (*models.SearchRequest, error) {
	if r.Method == http.MethodPost {
		var req models.SearchRequest
		limited := io.LimitReader(r.Body, maxRequestBodySize)
		if err := json.NewDecoder(limited).Decode(&req); err != nil {
			return nil, err
		}
		return &req, nil
	}

	// GET request
	req := &models.SearchRequest{
		Query:  r.URL.Query().Get("q"),
		Region: r.URL.Query().Get("region"),
		Sort:   r.URL.Query().Get("sort"),
		UserID: r.URL.Query().Get("user_id"),
	}

	if p := r.URL.Query().Get("page"); p != "" {
		page, err := strconv.Atoi(p)
		if err == nil && page >= 0 {
			req.Page = page
		}
	}

	if ps := r.URL.Query().Get("page_size"); ps != "" {
		pageSize, err := strconv.Atoi(ps)
		if err == nil && pageSize > 0 {
			req.PageSize = pageSize
		}
	}

	if r.URL.Query().Get("force_fresh") == "true" {
		req.ForceFresh = true
	}

	return req, nil
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.logger.Error("writing json response", zap.Error(err))
	}
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code, message string) {
	h.writeJSON(w, status, map[string]string{
		"error": message,
		"code":  code,
	})
}
