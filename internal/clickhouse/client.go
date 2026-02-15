package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/models"
	"github.com/shubhsaxena/high-scale-search/internal/observability"
)

type Client struct {
	conn   driver.Conn
	logger *zap.Logger
}

func NewClient(cfg config.ClickHouseConfig, logger *zap.Logger) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": int(cfg.QueryTimeout.Seconds()),
		},
		DialTimeout: cfg.DialTimeout,
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxIdleConns,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}

	logger.Info("clickhouse client connected", zap.Strings("addresses", cfg.Addresses))

	return &Client{
		conn:   conn,
		logger: logger,
	}, nil
}

type AggregationResult struct {
	Facets  map[string][]models.Facet
	Total   int64
	TookMs  int64
}

func (c *Client) QueryFacets(ctx context.Context, category string, filters map[string]any) (*AggregationResult, error) {
	ctx, span := observability.StartSpan(ctx, "ch.query_facets",
		attribute.String("category", category),
	)
	defer span.End()

	start := time.Now()

	query := `
		SELECT
			facet_name,
			facet_value,
			count() AS cnt
		FROM search_facets
		WHERE category = ?
		GROUP BY facet_name, facet_value
		ORDER BY cnt DESC
		LIMIT 100
	`

	rows, err := c.conn.Query(ctx, query, category)
	if err != nil {
		observability.CHQueryDuration.WithLabelValues("facets", "error").Observe(time.Since(start).Seconds())
		return nil, fmt.Errorf("ch facet query: %w", err)
	}
	defer rows.Close()

	facets := make(map[string][]models.Facet)
	for rows.Next() {
		var facetName, facetValue string
		var count int64
		if err := rows.Scan(&facetName, &facetValue, &count); err != nil {
			return nil, fmt.Errorf("scanning facet row: %w", err)
		}
		facets[facetName] = append(facets[facetName], models.Facet{
			Value: facetValue,
			Count: count,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating facet rows: %w", err)
	}

	duration := time.Since(start)
	observability.CHQueryDuration.WithLabelValues("facets", "success").Observe(duration.Seconds())

	return &AggregationResult{
		Facets: facets,
		TookMs: duration.Milliseconds(),
	}, nil
}

func (c *Client) QueryAnalytics(ctx context.Context, query string, filters map[string]any) (*AggregationResult, error) {
	ctx, span := observability.StartSpan(ctx, "ch.query_analytics")
	defer span.End()

	start := time.Now()

	chQuery := `
		SELECT
			category,
			count() AS total,
			avg(popularity_score) AS avg_score
		FROM search_documents
		WHERE match(title, ?) OR match(description, ?)
		GROUP BY category
		ORDER BY total DESC
		LIMIT 50
	`

	rows, err := c.conn.Query(ctx, chQuery, query, query)
	if err != nil {
		observability.CHQueryDuration.WithLabelValues("analytics", "error").Observe(time.Since(start).Seconds())
		return nil, fmt.Errorf("ch analytics query: %w", err)
	}
	defer rows.Close()

	facets := make(map[string][]models.Facet)
	var total int64
	for rows.Next() {
		var category string
		var count int64
		var avgScore float64
		if err := rows.Scan(&category, &count, &avgScore); err != nil {
			return nil, fmt.Errorf("scanning analytics row: %w", err)
		}
		total += count
		facets["category"] = append(facets["category"], models.Facet{
			Value: category,
			Count: count,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating analytics rows: %w", err)
	}

	duration := time.Since(start)
	observability.CHQueryDuration.WithLabelValues("analytics", "success").Observe(duration.Seconds())

	return &AggregationResult{
		Facets: facets,
		Total:  total,
		TookMs: duration.Milliseconds(),
	}, nil
}

func (c *Client) WriteQueryPerformance(ctx context.Context, event *models.AnalyticsEvent) error {
	query := `
		INSERT INTO query_performance (
			event_type, query_hash, query_type, duration_ms,
			total_hits, shards_hit, timed_out, timestamp, trace_id, source
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	return c.conn.Exec(ctx, query,
		event.EventType,
		event.QueryHash,
		event.QueryType,
		event.DurationMs,
		event.TotalHits,
		event.ShardsHit,
		event.TimedOut,
		event.Timestamp,
		event.TraceID,
		event.Source,
	)
}

func (c *Client) InsertDocumentEvent(ctx context.Context, event *models.ChangeEvent) error {
	query := `
		INSERT INTO search_documents_changelog (
			document_id, collection, operation, region, timestamp, version
		) VALUES (?, ?, ?, ?, ?, ?)
	`
	return c.conn.Exec(ctx, query,
		event.DocumentID,
		event.Collection,
		event.Type,
		event.Region,
		event.Timestamp,
		event.Version,
	)
}

func (c *Client) FallbackSearch(ctx context.Context, queryText string, limit int) ([]models.SearchResult, error) {
	ctx, span := observability.StartSpan(ctx, "ch.fallback_search")
	defer span.End()

	start := time.Now()

	query := `
		SELECT
			document_id,
			title,
			description,
			category,
			region,
			popularity_score
		FROM search_documents
		WHERE match(title, ?) OR match(description, ?)
		ORDER BY popularity_score DESC
		LIMIT ?
	`

	rows, err := c.conn.Query(ctx, query, queryText, queryText, limit)
	if err != nil {
		observability.CHQueryDuration.WithLabelValues("fallback", "error").Observe(time.Since(start).Seconds())
		return nil, fmt.Errorf("ch fallback search: %w", err)
	}
	defer rows.Close()

	var results []models.SearchResult
	for rows.Next() {
		var r models.SearchResult
		if err := rows.Scan(&r.ID, &r.Title, &r.Description, &r.Category, &r.Region, &r.PopularityScore); err != nil {
			return nil, fmt.Errorf("scanning fallback row: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating fallback rows: %w", err)
	}

	observability.CHQueryDuration.WithLabelValues("fallback", "success").Observe(time.Since(start).Seconds())
	return results, nil
}

func (c *Client) HealthCheck(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) EnsureTables(ctx context.Context) error {
	tables := []string{
		`CREATE TABLE IF NOT EXISTS query_performance (
			event_type String,
			query_hash String,
			query_type String,
			duration_ms Float64,
			total_hits Int64,
			shards_hit Int32,
			timed_out Bool,
			timestamp DateTime,
			trace_id String,
			source String
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (timestamp, query_hash)`,

		`CREATE TABLE IF NOT EXISTS search_documents (
			document_id String,
			title String,
			description String,
			category String,
			region String,
			popularity_score Float64,
			created_at DateTime,
			updated_at DateTime
		) ENGINE = ReplacingMergeTree(updated_at)
		PARTITION BY toYYYYMM(created_at)
		ORDER BY (document_id)`,

		`CREATE TABLE IF NOT EXISTS search_documents_changelog (
			document_id String,
			collection String,
			operation String,
			region String,
			timestamp DateTime,
			version Int64
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (timestamp, document_id)`,

		`CREATE TABLE IF NOT EXISTS search_facets (
			category String,
			facet_name String,
			facet_value String,
			count UInt64,
			updated_at DateTime
		) ENGINE = SummingMergeTree(count)
		PARTITION BY category
		ORDER BY (category, facet_name, facet_value)`,
	}

	for _, ddl := range tables {
		if err := c.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
	}

	c.logger.Info("clickhouse tables ensured")
	return nil
}
