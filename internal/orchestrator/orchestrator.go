package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/cache"
	"github.com/shubhsaxena/high-scale-search/internal/clickhouse"
	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/elasticsearch"
	"github.com/shubhsaxena/high-scale-search/internal/firestore"
	"github.com/shubhsaxena/high-scale-search/internal/models"
	"github.com/shubhsaxena/high-scale-search/internal/observability"
)

type Orchestrator struct {
	esClient   *elasticsearch.Client
	chClient   *clickhouse.Client
	fsClient   *firestore.Client
	cache      *cache.RedisCache
	parser     *QueryParser
	classifier *IntentClassifier
	builder    *QueryBuilder
	slowQuery  *observability.SlowQueryDetector
	cfg        config.SearchConfig
	esCfg      config.ElasticsearchConfig
	logger     *zap.Logger

	// Static fallback results by category
	staticFallback map[string][]models.SearchResult
	mu             sync.RWMutex
}

func New(
	esClient *elasticsearch.Client,
	chClient *clickhouse.Client,
	fsClient *firestore.Client,
	redisCache *cache.RedisCache,
	slowQuery *observability.SlowQueryDetector,
	cfg config.SearchConfig,
	esCfg config.ElasticsearchConfig,
	logger *zap.Logger,
) *Orchestrator {
	return &Orchestrator{
		esClient:       esClient,
		chClient:       chClient,
		fsClient:       fsClient,
		cache:          redisCache,
		parser:         NewQueryParser(),
		classifier:     NewIntentClassifier(),
		builder:        NewQueryBuilder(),
		slowQuery:      slowQuery,
		cfg:            cfg,
		esCfg:          esCfg,
		logger:         logger,
		staticFallback: make(map[string][]models.SearchResult),
	}
}

func (o *Orchestrator) Search(ctx context.Context, req *models.SearchRequest) (*models.SearchResponse, error) {
	start := time.Now()
	ctx, span := observability.StartSpan(ctx, "orchestrator.search",
		attribute.String("query", req.Query),
	)
	defer span.End()

	// Normalize page size
	if req.PageSize <= 0 {
		req.PageSize = o.cfg.DefaultPageSize
	}
	if req.PageSize > o.cfg.MaxPageSize {
		req.PageSize = o.cfg.MaxPageSize
	}

	// Step 1: Parse query
	parsed := o.parser.Parse(req.Query)

	// Step 2: Classify intent
	intent := o.classifier.Classify(parsed)
	o.logger.Debug("query classified",
		zap.String("query", req.Query),
		zap.String("intent", intent.String()),
	)

	// Step 3: Check cache
	if !req.ForceFresh {
		cached, err := o.cache.GetSearchResults(ctx, req)
		if err != nil {
			o.logger.Warn("cache lookup error", zap.Error(err))
		}
		if cached != nil {
			cached.Metadata.CacheHit = true
			cached.TookMs = time.Since(start).Milliseconds()
			observability.SearchRequestsTotal.WithLabelValues(intent.String(), "cache_hit").Inc()
			return cached, nil
		}
	}

	// Step 4-6: Route, execute, rank
	resp, err := o.searchWithFallback(ctx, req, parsed, intent)
	if err != nil {
		observability.SearchRequestsTotal.WithLabelValues(intent.String(), "error").Inc()
		observability.SearchRequestDuration.WithLabelValues(intent.String(), "error", "error").Observe(time.Since(start).Seconds())
		return nil, err
	}

	resp.TookMs = time.Since(start).Milliseconds()
	resp.Page = req.Page
	resp.PageSize = req.PageSize
	resp.Metadata.RequestID = req.RequestID
	resp.Metadata.Intent = intent.String()

	// Step 7: Cache results
	if err := o.cache.SetSearchResults(ctx, req, resp); err != nil {
		o.logger.Warn("cache set error", zap.Error(err))
	}

	// Track metrics
	observability.SearchRequestsTotal.WithLabelValues(intent.String(), "success").Inc()
	observability.SearchRequestDuration.WithLabelValues(intent.String(), resp.Source, "success").Observe(time.Since(start).Seconds())

	// Slow query detection
	o.slowQuery.Intercept(ctx, req.Query, intent.String(),
		time.Since(start), resp.Total, resp.Metadata.ShardsHit, resp.Metadata.TimedOut)

	return resp, nil
}

func (o *Orchestrator) searchWithFallback(ctx context.Context, req *models.SearchRequest, parsed *models.ParsedQuery, intent models.Intent) (*models.SearchResponse, error) {
	// Level 1: Primary search
	resp, err := o.primarySearch(ctx, req, parsed, intent)
	if err == nil {
		return resp, nil
	}
	o.logger.Warn("primary search failed, trying fallback", zap.Error(err))
	observability.FallbackCounter.WithLabelValues("primary_failed").Inc()

	// Level 2: Stale cache
	stale, cacheErr := o.cache.GetStaleResults(ctx, req)
	if cacheErr == nil && stale != nil {
		stale.Metadata.Stale = true
		stale.Source = "stale_cache"
		stale.Metadata.Source = "stale_cache"
		observability.FallbackCounter.WithLabelValues("stale_cache").Inc()
		return stale, nil
	}

	// Level 3: ClickHouse degraded search
	if o.chClient != nil {
		chResults, chErr := o.chClient.FallbackSearch(ctx, parsed.Normalized, req.PageSize)
		if chErr == nil && len(chResults) > 0 {
			observability.FallbackCounter.WithLabelValues("clickhouse").Inc()
			return &models.SearchResponse{
				Results: chResults,
				Total:   int64(len(chResults)),
				Source:  "degraded",
				Metadata: models.ResponseMetadata{
					Source: "degraded_clickhouse",
				},
			}, nil
		}
		if chErr != nil {
			o.logger.Warn("clickhouse fallback failed", zap.Error(chErr))
		}
	}

	// Level 4: Static popular results
	staticResults := o.getStaticFallback(req.Region)
	if len(staticResults) > 0 {
		observability.FallbackCounter.WithLabelValues("static").Inc()
		return &models.SearchResponse{
			Results: staticResults,
			Total:   int64(len(staticResults)),
			Source:  "static_fallback",
			Metadata: models.ResponseMetadata{
				Source: "static_fallback",
			},
		}, nil
	}

	return nil, fmt.Errorf("all search paths exhausted: primary error: %w", err)
}

func (o *Orchestrator) primarySearch(ctx context.Context, req *models.SearchRequest, parsed *models.ParsedQuery, intent models.Intent) (*models.SearchResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, o.cfg.QueryTimeout)
	defer cancel()

	switch intent {
	case models.IntentFullText, models.IntentAutocomplete:
		return o.fullTextSearch(ctx, req, parsed)

	case models.IntentAnalytics:
		return o.analyticsSearch(ctx, req, parsed)

	case models.IntentFaceted:
		return o.facetedSearch(ctx, req, parsed)

	default:
		return o.fullTextSearch(ctx, req, parsed)
	}
}

func (o *Orchestrator) fullTextSearch(ctx context.Context, req *models.SearchRequest, parsed *models.ParsedQuery) (*models.SearchResponse, error) {
	esQuery := o.builder.BuildESQuery(parsed, req)

	index := fmt.Sprintf("%s-*", o.esCfg.IndexPrefix)
	if req.Region != "" {
		index = fmt.Sprintf("%s-*-%s-*", o.esCfg.IndexPrefix, req.Region)
	}

	result, err := o.esClient.Search(ctx, index, esQuery)
	if err != nil {
		return nil, fmt.Errorf("es fulltext search: %w", err)
	}

	// Hydrate from Firestore if extra fields needed
	if len(req.Fields) > 0 && o.fsClient != nil {
		hydrated, err := o.fsClient.HydrateResults(ctx, result.Hits, "documents")
		if err != nil {
			o.logger.Warn("hydration failed", zap.Error(err))
		} else {
			result.Hits = hydrated
		}
	}

	return &models.SearchResponse{
		Results: result.Hits,
		Total:   result.Total,
		Source:  "primary",
		Metadata: models.ResponseMetadata{
			Source:    "elasticsearch",
			ShardsHit: result.ShardsHit,
			TimedOut:  result.TimedOut,
		},
	}, nil
}

func (o *Orchestrator) analyticsSearch(ctx context.Context, req *models.SearchRequest, parsed *models.ParsedQuery) (*models.SearchResponse, error) {
	if o.chClient == nil {
		return o.fullTextSearch(ctx, req, parsed)
	}

	aggResult, err := o.chClient.QueryAnalytics(ctx, parsed.Normalized, req.Filters)
	if err != nil {
		o.logger.Warn("clickhouse analytics failed, falling back to ES", zap.Error(err))
		return o.fullTextSearch(ctx, req, parsed)
	}

	return &models.SearchResponse{
		Total:  aggResult.Total,
		Facets: aggResult.Facets,
		Source: "analytics",
		Metadata: models.ResponseMetadata{
			Source: "clickhouse",
		},
	}, nil
}

func (o *Orchestrator) facetedSearch(ctx context.Context, req *models.SearchRequest, parsed *models.ParsedQuery) (*models.SearchResponse, error) {
	type esResult struct {
		resp *models.SearchResponse
		err  error
	}
	type chResult struct {
		facets map[string][]models.Facet
		err    error
	}

	esCh := make(chan esResult, 1)
	chCh := make(chan chResult, 1)

	// Fan-out: ES for results + ClickHouse for facet counts
	go func() {
		resp, err := o.fullTextSearch(ctx, req, parsed)
		esCh <- esResult{resp: resp, err: err}
	}()

	go func() {
		if o.chClient == nil {
			chCh <- chResult{err: fmt.Errorf("clickhouse not available")}
			return
		}
		category := ""
		if c, ok := req.Filters["category"].(string); ok {
			category = c
		}
		aggResult, err := o.chClient.QueryFacets(ctx, category, req.Filters)
		if err != nil {
			chCh <- chResult{err: err}
			return
		}
		chCh <- chResult{facets: aggResult.Facets}
	}()

	esRes := <-esCh
	chRes := <-chCh

	if esRes.err != nil {
		return nil, fmt.Errorf("faceted es search: %w", esRes.err)
	}

	resp := esRes.resp
	if chRes.err != nil {
		o.logger.Warn("facet counts from clickhouse failed", zap.Error(chRes.err))
	} else {
		resp.Facets = chRes.facets
	}

	resp.Source = "faceted"
	resp.Metadata.Source = "elasticsearch+clickhouse"
	return resp, nil
}

func (o *Orchestrator) SetStaticFallback(region string, results []models.SearchResult) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.staticFallback[region] = results
}

func (o *Orchestrator) getStaticFallback(region string) []models.SearchResult {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if results, ok := o.staticFallback[region]; ok {
		return results
	}
	if results, ok := o.staticFallback["default"]; ok {
		return results
	}
	return nil
}
