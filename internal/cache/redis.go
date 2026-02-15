package cache

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/models"
	"github.com/shubhsaxena/high-scale-search/internal/observability"
)

type RedisCache struct {
	client redis.UniversalClient
	ttl    config.CacheTTLConfig
	logger *zap.Logger
}

func NewRedisCache(cfg config.RedisConfig, logger *zap.Logger) (*RedisCache, error) {
	var client redis.UniversalClient

	if len(cfg.Addresses) > 1 {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        cfg.Addresses,
			Password:     cfg.Password,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConns,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Addr:         cfg.Addresses[0],
			Password:     cfg.Password,
			DB:           cfg.DB,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConns,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	logger.Info("redis cache connected", zap.Strings("addresses", cfg.Addresses))

	return &RedisCache{
		client: client,
		ttl:    cfg.TTL,
		logger: logger,
	}, nil
}

func (rc *RedisCache) GetSearchResults(ctx context.Context, req *models.SearchRequest) (*models.SearchResponse, error) {
	key := rc.buildSearchKey(req)
	return rc.getResponse(ctx, key)
}

func (rc *RedisCache) SetSearchResults(ctx context.Context, req *models.SearchRequest, resp *models.SearchResponse) error {
	key := rc.buildSearchKey(req)
	ttl := rc.ttlForIntent(resp.Metadata.Intent)
	if err := rc.setResponse(ctx, key, resp, ttl); err != nil {
		return err
	}
	staleKey := rc.buildStaleKey(req)
	return rc.setResponse(ctx, staleKey, resp, rc.ttl.StaleFallback)
}

func (rc *RedisCache) GetStaleResults(ctx context.Context, req *models.SearchRequest) (*models.SearchResponse, error) {
	key := rc.buildStaleKey(req)
	return rc.getResponse(ctx, key)
}

// InvalidateKeys deletes specific cache keys. Prefer this over pattern-based
// invalidation to avoid O(N) SCAN operations on large keyspaces.
func (rc *RedisCache) InvalidateKeys(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := rc.client.Del(ctx, keys...).Err(); err != nil {
		rc.logger.Warn("cache delete error", zap.Int("key_count", len(keys)), zap.Error(err))
		return err
	}
	return nil
}

func (rc *RedisCache) GetAutocomplete(ctx context.Context, prefix string) ([]string, error) {
	key := fmt.Sprintf("ac:%s", hashString(prefix))
	val, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		observability.CacheMisses.Inc()
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cache get autocomplete: %w", err)
	}
	observability.CacheHits.Inc()
	var results []string
	if err := json.Unmarshal([]byte(val), &results); err != nil {
		return nil, fmt.Errorf("cache unmarshal autocomplete: %w", err)
	}
	return results, nil
}

func (rc *RedisCache) SetAutocomplete(ctx context.Context, prefix string, results []string) error {
	key := fmt.Sprintf("ac:%s", hashString(prefix))
	data, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("cache marshal autocomplete: %w", err)
	}
	return rc.client.Set(ctx, key, data, rc.ttl.Autocomplete).Err()
}

func (rc *RedisCache) GetTrending(ctx context.Context, region string) ([]string, error) {
	key := fmt.Sprintf("trend:%s", region)
	val, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cache get trending: %w", err)
	}
	var results []string
	if err := json.Unmarshal([]byte(val), &results); err != nil {
		return nil, fmt.Errorf("cache unmarshal trending: %w", err)
	}
	return results, nil
}

func (rc *RedisCache) SetTrending(ctx context.Context, region string, queries []string) error {
	key := fmt.Sprintf("trend:%s", region)
	data, err := json.Marshal(queries)
	if err != nil {
		return fmt.Errorf("cache marshal trending: %w", err)
	}
	return rc.client.Set(ctx, key, data, rc.ttl.Trending).Err()
}

func (rc *RedisCache) HealthCheck(ctx context.Context) error {
	return rc.client.Ping(ctx).Err()
}

func (rc *RedisCache) Close() error {
	return rc.client.Close()
}

func (rc *RedisCache) getResponse(ctx context.Context, key string) (*models.SearchResponse, error) {
	val, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		observability.CacheMisses.Inc()
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cache get: %w", err)
	}

	observability.CacheHits.Inc()
	var resp models.SearchResponse
	if err := json.Unmarshal([]byte(val), &resp); err != nil {
		return nil, fmt.Errorf("cache unmarshal: %w", err)
	}
	return &resp, nil
}

func (rc *RedisCache) setResponse(ctx context.Context, key string, resp *models.SearchResponse, ttl time.Duration) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("cache marshal: %w", err)
	}
	return rc.client.Set(ctx, key, data, ttl).Err()
}

// buildSearchKey produces a deterministic cache key by sorting filter keys
// before hashing, ensuring identical filter sets always produce the same key.
func (rc *RedisCache) buildSearchKey(req *models.SearchRequest) string {
	raw := fmt.Sprintf("%s:%s:%d:%d", req.Query, canonicalFilters(req.Filters), req.Page, req.PageSize)
	return fmt.Sprintf("sr:%s", hashString(raw))
}

func (rc *RedisCache) buildStaleKey(req *models.SearchRequest) string {
	raw := fmt.Sprintf("%s:%s:%d:%d", req.Query, canonicalFilters(req.Filters), req.Page, req.PageSize)
	return fmt.Sprintf("sr:stale:%s", hashString(raw))
}

// canonicalFilters produces a deterministic string from a filter map by sorting keys.
func canonicalFilters(filters map[string]any) string {
	if len(filters) == 0 {
		return ""
	}
	keys := make([]string, 0, len(filters))
	for k := range filters {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%s=%v", k, filters[k])
	}
	return sb.String()
}

func (rc *RedisCache) ttlForIntent(intent string) time.Duration {
	switch intent {
	case "autocomplete":
		return rc.ttl.Autocomplete
	case "analytics":
		return rc.ttl.FacetCounts
	case "faceted":
		return rc.ttl.FacetCounts
	default:
		return rc.ttl.SearchResults
	}
}

func hashString(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h[:8])
}
