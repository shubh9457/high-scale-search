package indexing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/cache"
	"github.com/shubhsaxena/high-scale-search/internal/clickhouse"
	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/elasticsearch"
	"github.com/shubhsaxena/high-scale-search/internal/models"
	"github.com/shubhsaxena/high-scale-search/internal/observability"
)

type StreamProcessor struct {
	esClient *elasticsearch.Client
	chClient *clickhouse.Client
	cache    *cache.RedisCache
	esCfg   config.ElasticsearchConfig
	logger   *zap.Logger

	// Bulk buffer
	mu      sync.Mutex
	buffer  []models.IndexAction
	ticker  *time.Ticker
	done    chan struct{}
}

func NewStreamProcessor(
	esClient *elasticsearch.Client,
	chClient *clickhouse.Client,
	cache *cache.RedisCache,
	esCfg config.ElasticsearchConfig,
	logger *zap.Logger,
) *StreamProcessor {
	sp := &StreamProcessor{
		esClient: esClient,
		chClient: chClient,
		cache:    cache,
		esCfg:   esCfg,
		logger:   logger,
		buffer:   make([]models.IndexAction, 0, esCfg.BulkSize),
		ticker:   time.NewTicker(esCfg.BulkFlushInterval),
		done:     make(chan struct{}),
	}

	go sp.flushLoop()

	return sp
}

func (sp *StreamProcessor) HandleEvent(ctx context.Context, event *models.ChangeEvent) error {
	// Transform to index action
	action, err := sp.transformEvent(event)
	if err != nil {
		return fmt.Errorf("transforming event: %w", err)
	}

	// Buffer for bulk indexing
	sp.mu.Lock()
	sp.buffer = append(sp.buffer, *action)
	shouldFlush := len(sp.buffer) >= sp.esCfg.BulkSize
	sp.mu.Unlock()

	if shouldFlush {
		if err := sp.flush(ctx); err != nil {
			sp.logger.Error("flush on buffer full failed", zap.Error(err))
		}
	}

	// Write to ClickHouse for analytics (async, best-effort)
	if sp.chClient != nil {
		go func() {
			chCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := sp.chClient.InsertDocumentEvent(chCtx, event); err != nil {
				sp.logger.Warn("clickhouse event insert failed",
					zap.String("doc_id", event.DocumentID),
					zap.Error(err),
				)
			}
		}()
	}

	// Invalidate relevant caches
	go func() {
		cacheCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		patterns := buildInvalidationKeys(event)
		if err := sp.cache.InvalidatePattern(cacheCtx, patterns); err != nil {
			sp.logger.Warn("cache invalidation failed",
				zap.String("doc_id", event.DocumentID),
				zap.Error(err),
			)
		}
	}()

	return nil
}

func (sp *StreamProcessor) transformEvent(event *models.ChangeEvent) (*models.IndexAction, error) {
	action := &models.IndexAction{
		ID:        event.DocumentID,
		Routing:   event.Region,
		Timestamp: event.Timestamp,
	}

	// Resolve index name
	docType := "general"
	if t, ok := event.Document["type"].(string); ok {
		docType = t
	}
	region := event.Region
	if region == "" {
		if r, ok := event.Document["region"].(string); ok {
			region = r
		}
	}
	action.Index = sp.esClient.ResolveIndex(docType, region)

	switch event.Type {
	case "CREATE", "UPDATE":
		action.Action = "index"
		action.Body = sp.extractSearchFields(event.Document)
	case "DELETE":
		action.Action = "delete"
	default:
		return nil, fmt.Errorf("unknown event type: %s", event.Type)
	}

	return action, nil
}

func (sp *StreamProcessor) extractSearchFields(doc map[string]any) map[string]any {
	fields := map[string]any{
		"updated_at": time.Now().UTC().Format(time.RFC3339),
	}

	searchableFields := []string{
		"title", "description", "category", "tags",
		"region", "created_at", "popularity_score", "geo_point",
	}

	for _, field := range searchableFields {
		if v, ok := doc[field]; ok {
			fields[field] = v
		}
	}

	return fields
}

func (sp *StreamProcessor) flushLoop() {
	for {
		select {
		case <-sp.ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := sp.flush(ctx); err != nil {
				sp.logger.Error("periodic flush failed", zap.Error(err))
			}
			cancel()
		case <-sp.done:
			return
		}
	}
}

func (sp *StreamProcessor) flush(ctx context.Context) error {
	sp.mu.Lock()
	if len(sp.buffer) == 0 {
		sp.mu.Unlock()
		return nil
	}
	batch := make([]models.IndexAction, len(sp.buffer))
	copy(batch, sp.buffer)
	sp.buffer = sp.buffer[:0]
	sp.mu.Unlock()

	start := time.Now()
	if err := sp.esClient.BulkIndex(ctx, batch); err != nil {
		// Put failed items back into buffer for retry
		sp.mu.Lock()
		sp.buffer = append(batch, sp.buffer...)
		sp.mu.Unlock()

		observability.IndexingEventsTotal.WithLabelValues("bulk", "error").Inc()
		return fmt.Errorf("bulk index flush: %w", err)
	}

	observability.IndexingEventsTotal.WithLabelValues("bulk", "success").Add(float64(len(batch)))
	sp.logger.Info("bulk flush completed",
		zap.Int("count", len(batch)),
		zap.Duration("duration", time.Since(start)),
	)

	return nil
}

func (sp *StreamProcessor) Stop() error {
	sp.ticker.Stop()
	close(sp.done)

	// Final flush
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return sp.flush(ctx)
}

func buildInvalidationKeys(event *models.ChangeEvent) []string {
	var patterns []string

	// Invalidate search results containing this document
	patterns = append(patterns, "sr:*")

	// Invalidate facets for the document's category
	if category, ok := event.Document["category"].(string); ok {
		patterns = append(patterns, fmt.Sprintf("fc:%s:*", category))
	}

	// Invalidate trending and popular for the region
	if event.Region != "" {
		patterns = append(patterns, fmt.Sprintf("trend:%s", event.Region))
		patterns = append(patterns, fmt.Sprintf("pop:%s:*", event.Region))
	}

	return patterns
}
