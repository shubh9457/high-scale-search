# High-Scale Search System

A production-grade search orchestration service built in Go, designed to handle **10B+ records** with sub-200ms p99 latency. Integrates Elasticsearch, Firestore, ClickHouse, Redis, and Kafka into a unified search pipeline with multi-level fault tolerance.

## Architecture Overview

```
Client → API Gateway (rate limit, auth, request ID)
           │
           ▼
     Search Orchestrator
     ├── Query Parser        (tokenize, normalize, spell-correct)
     ├── Intent Classifier   (fulltext / analytics / faceted / autocomplete)
     ├── Cache Lookup        (Redis - query fingerprint hash)
     ├── Route & Execute     (ES / ClickHouse / fan-out)
     ├── Hydrate             (Firestore for full documents)
     └── Cache Set           (TTL by query type)
           │
     ┌─────┼──────────┐
     ▼     ▼          ▼
   Redis  Elastic   ClickHouse
  (cache) (search)  (analytics)
           ▲
           │
     Indexing Pipeline
     Firestore → Kafka → Stream Processor → ES + CH + Redis invalidation
```

## Performance Targets

| Metric | Target |
|---|---|
| Search Latency (p50) | < 50ms |
| Search Latency (p99) | < 200ms |
| Indexing Lag | < 5s (real-time) |
| Throughput | 50K+ QPS |
| Uptime SLA | 99.95% |
| Cache Hit Rate | > 60% |

## Project Structure

```
├── cmd/server/main.go                  # Entrypoint with graceful shutdown
├── config.yaml                         # Environment-variable-driven configuration
├── Dockerfile                          # Multi-stage production build
├── docker-compose.yaml                 # Full local development stack
└── internal/
    ├── api/
    │   ├── handlers.go                 # Search, Autocomplete, Trending endpoints
    │   ├── health.go                   # Liveness + Readiness probes
    │   ├── middleware.go               # RequestID, Logging, Recovery, RateLimiter, CORS
    │   └── router.go                   # Chi router with versioned API routes
    ├── cache/
    │   └── redis.go                    # Redis client with per-query-type TTL + stale fallback
    ├── clickhouse/
    │   └── client.go                   # Facets, analytics, fallback search, query perf logging
    ├── config/
    │   └── config.go                   # YAML config with env var expansion and validation
    ├── elasticsearch/
    │   └── client.go                   # ES client with circuit breaker, retry, bulk indexing
    ├── firestore/
    │   └── client.go                   # Batch get, hydration, real-time change listener
    ├── indexing/
    │   └── processor.go                # Stream processor with bulk buffer and flush loop
    ├── kafka/
    │   ├── consumer.go                 # Consumer with DLQ, retry, offset commit, lag tracking
    │   └── producer.go                 # Producer with batch publishing
    ├── models/
    │   └── search.go                   # Domain types (requests, responses, events)
    ├── observability/
    │   ├── logger.go                   # Structured JSON logging (zap)
    │   ├── metrics.go                  # Prometheus metrics (latency, counters, gauges)
    │   ├── slowquery.go                # Slow query detection and analytics
    │   └── tracing.go                  # OpenTelemetry distributed tracing
    ├── orchestrator/
    │   ├── intent.go                   # Rule-based intent classifier
    │   ├── orchestrator.go             # Core search with 5-level fallback chain
    │   ├── parser.go                   # Query parser (tokenize, normalize, field extraction)
    │   └── querybuilder.go             # ES query builder (BM25 + script_score + fuzzy)
    └── resilience/
        └── circuitbreaker.go           # Circuit breaker + exponential backoff retry
```

## Getting Started

### Prerequisites

- Go 1.22+
- Docker and Docker Compose

### Run Locally

```bash
# Start all infrastructure (Elasticsearch, Redis, ClickHouse, Kafka)
docker-compose up -d

# Wait for services to be healthy
docker-compose ps

# Run the server
go run ./cmd/server -config config.yaml
```

### Build

```bash
# Build binary
go build -o bin/search-server ./cmd/server

# Run binary
./bin/search-server -config config.yaml

# Docker
docker build -t search-server .
docker run -p 8080:8080 search-server
```

## API Endpoints

### Search

```bash
# GET request
curl "http://localhost:8080/api/v1/search?q=laptop&page=0&page_size=20&region=us"

# POST request
curl -X POST http://localhost:8080/api/v1/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "laptop",
    "filters": {"category": "electronics"},
    "page": 0,
    "page_size": 20,
    "region": "us"
  }'
```

### Autocomplete

```bash
curl "http://localhost:8080/api/v1/autocomplete?q=lap"
```

### Trending

```bash
curl "http://localhost:8080/api/v1/trending?region=us"
```

### Health Checks

```bash
# Liveness (is the process running)
curl http://localhost:8080/healthz

# Readiness (are all backends reachable)
curl http://localhost:8080/readyz

# Prometheus metrics
curl http://localhost:8080/metrics
```

## Fault Tolerance

The system implements a **5-level fallback chain** to ensure search never returns a hard error:

| Level | Source | When |
|---|---|---|
| 0 | Redis Cache | Cache hit on query fingerprint |
| 1 | Elasticsearch (primary) | Normal operation with circuit breaker + retry |
| 2 | Redis Stale Cache | Primary ES fails; serve slightly stale results |
| 3 | ClickHouse (degraded) | Both ES and cache unavailable; basic text search |
| 4 | Static Popular Results | All backends down; pre-loaded popular results |

### Resilience Mechanisms

- **Circuit Breaker**: Opens after 5 consecutive failures, half-opens after 30s for probe requests
- **Retry with Exponential Backoff**: 2 attempts, 50ms initial wait, 2x multiplier, 500ms max
- **Dead Letter Queue**: Failed Kafka messages are routed to a DLQ with error metadata for replay
- **Graceful Degradation**: Each backend (ClickHouse, Firestore, Kafka) is optional; the service starts and operates without them
- **Rate Limiting**: Token-bucket concurrency limiter (1000 concurrent requests)
- **Graceful Shutdown**: SIGINT/SIGTERM handling, drains in-flight HTTP requests, flushes bulk indexing buffer

## Data Flow

### Search Path (Read)

```
Request → Parse Query → Classify Intent → Check Cache
  → Route to backend (ES / ClickHouse / fan-out)
  → Hydrate from Firestore (if needed)
  → Cache result → Return
```

### Indexing Path (Write)

```
Firestore write → Kafka (docs.changes topic)
  → Stream Processor
    ├── Bulk buffer → Elasticsearch
    ├── ClickHouse (analytics changelog)
    └── Redis cache invalidation
```

## Configuration

All configuration is in `config.yaml` with environment variable expansion (`${VAR:-default}`):

| Variable | Description | Default |
|---|---|---|
| `ES_ADDRESS` | Elasticsearch URL | `http://localhost:9200` |
| `REDIS_ADDRESS` | Redis address | `localhost:6379` |
| `CH_ADDRESS` | ClickHouse address | `localhost:9000` |
| `KAFKA_BROKER` | Kafka broker | `localhost:9092` |
| `GCP_PROJECT_ID` | GCP project for Firestore | (empty) |
| `LOG_LEVEL` | Log level (debug/info/warn/error) | `info` |

### Cache TTL Strategy

| Query Type | TTL | Key Pattern |
|---|---|---|
| Autocomplete | 10 min | `ac:{prefix_hash}` |
| Trending | 60 sec | `trend:{region}` |
| Search Results | 2 min | `sr:{query_hash}` |
| Facet Counts | 5 min | `fc:{category}:{filters_hash}` |
| Stale Fallback | 1 hour | `sr:stale:{query_hash}` |

## Observability

### Prometheus Metrics

Key metrics exposed at `/metrics`:

- `search_request_duration_seconds` - Histogram by intent, source, status
- `search_requests_total` - Counter by intent and status
- `redis_cache_hits_total` / `redis_cache_misses_total` - Cache effectiveness
- `es_query_duration_seconds` - ES query latency by index
- `ch_query_duration_seconds` - ClickHouse query latency
- `circuit_breaker_state` - Circuit breaker status (0=closed, 1=half-open, 2=open)
- `slow_query_total` - Slow query counter by severity
- `search_fallback_total` - Fallback invocations by level
- `indexing_lag_seconds` - Real-time indexing pipeline lag
- `kafka_consumer_group_lag` - Kafka consumer lag

### Slow Query Detection

Queries exceeding thresholds are logged and tracked:

- **Warning**: > 200ms
- **Critical**: > 500ms

Critical queries are written to ClickHouse `query_performance` table for trend analysis.

### Distributed Tracing

OpenTelemetry traces propagate through all backends with a consistent `trace_id` for end-to-end request visibility.

## Elasticsearch Index Strategy

- **Index naming**: `search-{type}-{region}-{yyyy.MM}`
- **Tiered storage**: Hot (NVMe, 3 months) → Warm (SSD, 3-12 months) → Cold (object storage)
- **Minimal `_source`**: Only searchable fields indexed; full documents hydrated from Firestore
- **Script scoring**: `_score * (1 + log1p(popularity_score))` for relevance + popularity blending

## License

MIT
