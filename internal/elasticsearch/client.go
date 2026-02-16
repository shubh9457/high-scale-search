package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/models"
	"github.com/shubhsaxena/high-scale-search/internal/observability"
	"github.com/shubhsaxena/high-scale-search/internal/resilience"
)

type Client struct {
	es      *elasticsearch.Client
	cb      *gobreaker.CircuitBreaker
	cfg     config.ElasticsearchConfig
	retryCfg resilience.RetryConfig
	logger  *zap.Logger
}

func NewClient(cfg config.ElasticsearchConfig, searchCfg config.SearchConfig, logger *zap.Logger) (*Client, error) {
	esCfg := elasticsearch.Config{
		Addresses:  cfg.Addresses,
		Username:   cfg.Username,
		Password:   cfg.Password,
		MaxRetries: cfg.MaxRetries,
	}

	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("creating elasticsearch client: %w", err)
	}

	res, err := es.Ping()
	if err != nil {
		return nil, fmt.Errorf("pinging elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch ping returned status: %s", res.Status())
	}

	cb := resilience.NewCircuitBreaker("elasticsearch-primary", searchCfg.CircuitBreaker, logger)

	logger.Info("elasticsearch client connected", zap.Strings("addresses", cfg.Addresses))

	return &Client{
		es:  es,
		cb:  cb,
		cfg: cfg,
		retryCfg: resilience.RetryConfig{
			MaxAttempts: searchCfg.Retry.MaxAttempts,
			InitialWait: searchCfg.Retry.InitialWait,
			MaxWait:     searchCfg.Retry.MaxWait,
			Multiplier:  searchCfg.Retry.Multiplier,
		},
		logger: logger,
	}, nil
}

type SearchResult struct {
	Hits      []models.SearchResult
	Total     int64
	TookMs    int64
	ShardsHit int
	TimedOut  bool
}

func (c *Client) Search(ctx context.Context, index string, query map[string]any) (*SearchResult, error) {
	ctx, span := observability.StartSpan(ctx, "es.search",
		attribute.String("es.index", index),
	)
	defer span.End()

	start := time.Now()
	var result *SearchResult

	cbResult, err := c.cb.Execute(func() (any, error) {
		var retryResult *SearchResult
		retryErr := resilience.Retry(ctx, c.retryCfg, func() error {
			var execErr error
			retryResult, execErr = c.executeSearch(ctx, index, query)
			return execErr
		})
		return retryResult, retryErr
	})

	duration := time.Since(start)
	status := "success"
	if err != nil {
		status = "error"
		observability.ESQueryDuration.WithLabelValues(index, status).Observe(duration.Seconds())
		return nil, fmt.Errorf("es search (index=%s): %w", index, err)
	}

	result, ok := cbResult.(*SearchResult)
	if !ok || result == nil {
		observability.ESQueryDuration.WithLabelValues(index, "error").Observe(duration.Seconds())
		return nil, fmt.Errorf("es search (index=%s): unexpected nil result from circuit breaker", index)
	}
	observability.ESQueryDuration.WithLabelValues(index, status).Observe(duration.Seconds())

	return result, nil
}

func (c *Client) executeSearch(ctx context.Context, index string, query map[string]any) (*SearchResult, error) {
	body, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("marshaling es query: %w", err)
	}

	res, err := c.es.Search(
		c.es.Search.WithContext(ctx),
		c.es.Search.WithIndex(index),
		c.es.Search.WithBody(bytes.NewReader(body)),
		c.es.Search.WithTimeout(c.cfg.RequestTimeout),
		c.es.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, fmt.Errorf("executing es search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("es search error status=%s body=%s", res.Status(), string(bodyBytes))
	}

	var esResp esSearchResponse
	if err := json.NewDecoder(res.Body).Decode(&esResp); err != nil {
		return nil, fmt.Errorf("decoding es response: %w", err)
	}

	hits := make([]models.SearchResult, 0, len(esResp.Hits.Hits))
	for _, h := range esResp.Hits.Hits {
		hit := models.SearchResult{
			ID:    h.ID,
			Score: h.Score,
		}
		if h.Source != nil {
			if v, ok := h.Source["title"].(string); ok {
				hit.Title = v
			}
			if v, ok := h.Source["description"].(string); ok {
				hit.Description = v
			}
			if v, ok := h.Source["category"].(string); ok {
				hit.Category = v
			}
			if v, ok := h.Source["region"].(string); ok {
				hit.Region = v
			}
			if v, ok := h.Source["popularity_score"].(float64); ok {
				hit.PopularityScore = v
			}
			if tags, ok := h.Source["tags"].([]any); ok {
				for _, t := range tags {
					if s, ok := t.(string); ok {
						hit.Tags = append(hit.Tags, s)
					}
				}
			}
		}
		if h.Highlight != nil {
			hit.Highlights = h.Highlight
		}
		hits = append(hits, hit)
	}

	return &SearchResult{
		Hits:      hits,
		Total:     esResp.Hits.Total.Value,
		TookMs:    esResp.Took,
		ShardsHit: esResp.Shards.Total,
		TimedOut:  esResp.TimedOut,
	}, nil
}

func (c *Client) BulkIndex(ctx context.Context, actions []models.IndexAction) error {
	if len(actions) == 0 {
		return nil
	}

	ctx, span := observability.StartSpan(ctx, "es.bulk_index",
		attribute.Int("batch_size", len(actions)),
	)
	defer span.End()

	var buf bytes.Buffer
	for _, action := range actions {
		meta := map[string]any{
			action.Action: map[string]any{
				"_index": action.Index,
				"_id":    action.ID,
			},
		}
		if action.Routing != "" {
			if inner, ok := meta[action.Action].(map[string]any); ok {
				inner["routing"] = action.Routing
			}
		}

		metaLine, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshaling bulk meta: %w", err)
		}
		buf.Write(metaLine)
		buf.WriteByte('\n')

		if action.Action != "delete" && action.Body != nil {
			bodyLine, err := json.Marshal(action.Body)
			if err != nil {
				return fmt.Errorf("marshaling bulk body: %w", err)
			}
			buf.Write(bodyLine)
			buf.WriteByte('\n')
		}
	}

	res, err := c.es.Bulk(
		bytes.NewReader(buf.Bytes()),
		c.es.Bulk.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("executing bulk request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		return fmt.Errorf("bulk request error status=%s body=%s", res.Status(), string(bodyBytes))
	}

	var bulkResp bulkResponse
	if err := json.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		return fmt.Errorf("decoding bulk response: %w", err)
	}

	if bulkResp.Errors {
		var errMsgs []string
		for _, item := range bulkResp.Items {
			for _, result := range item {
				if result.Error != nil {
					errMsgs = append(errMsgs, fmt.Sprintf("id=%s: %s", result.ID, result.Error.Reason))
				}
			}
		}
		return fmt.Errorf("bulk indexing had errors: %s", strings.Join(errMsgs, "; "))
	}

	return nil
}

func (c *Client) ResolveIndex(docType, region string) string {
	now := time.Now()
	return fmt.Sprintf("%s-%s-%s-%s", c.cfg.IndexPrefix, docType, region, now.Format("2006.01"))
}

func (c *Client) HealthCheck(ctx context.Context) (string, error) {
	res, err := c.es.Cluster.Health(
		c.es.Cluster.Health.WithContext(ctx),
	)
	if err != nil {
		return "red", fmt.Errorf("es health check: %w", err)
	}
	defer res.Body.Close()

	var health struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(res.Body).Decode(&health); err != nil {
		return "red", fmt.Errorf("decoding health response: %w", err)
	}
	return health.Status, nil
}

func (c *Client) Close() error {
	return nil
}

// ES response types

type esSearchResponse struct {
	Took     int64 `json:"took"`
	TimedOut bool  `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []esHit `json:"hits"`
	} `json:"hits"`
}

type esHit struct {
	Index     string              `json:"_index"`
	ID        string              `json:"_id"`
	Score     float64             `json:"_score"`
	Source    map[string]any      `json:"_source"`
	Highlight map[string][]string `json:"highlight,omitempty"`
}

type bulkResponse struct {
	Errors bool `json:"errors"`
	Items  []map[string]bulkItemResult `json:"items"`
}

type bulkItemResult struct {
	ID     string `json:"_id"`
	Status int    `json:"status"`
	Error  *struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error,omitempty"`
}
