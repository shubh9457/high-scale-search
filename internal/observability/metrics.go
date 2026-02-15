package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	SearchRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "search_request_duration_seconds",
			Help:    "Search request duration in seconds",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.5, 1, 2.5},
		},
		[]string{"intent", "source", "status"},
	)

	SearchRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "search_requests_total",
			Help: "Total number of search requests",
		},
		[]string{"intent", "status"},
	)

	CacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cache_hits_total",
			Help: "Total number of Redis cache hits",
		},
	)

	CacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cache_misses_total",
			Help: "Total number of Redis cache misses",
		},
	)

	ESQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "es_query_duration_seconds",
			Help:    "Elasticsearch query duration in seconds",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.5, 1},
		},
		[]string{"index", "status"},
	)

	CHQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ch_query_duration_seconds",
			Help:    "ClickHouse query duration in seconds",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		},
		[]string{"query_type", "status"},
	)

	IndexingLag = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "indexing_lag_seconds",
			Help: "Current indexing pipeline lag in seconds",
		},
	)

	IndexingEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indexing_events_total",
			Help: "Total number of indexing events processed",
		},
		[]string{"operation", "status"},
	)

	CircuitBreakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Circuit breaker state (0=closed, 1=half-open, 2=open)",
		},
		[]string{"name"},
	)

	SlowQueryCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "slow_query_total",
			Help: "Total number of slow queries",
		},
		[]string{"severity", "query_type"},
	)

	FallbackCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "search_fallback_total",
			Help: "Total number of search fallback invocations",
		},
		[]string{"level"},
	)

	ActiveConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "active_connections",
			Help: "Number of active connections to backend systems",
		},
		[]string{"backend"},
	)

	KafkaConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_group_lag",
			Help: "Kafka consumer group lag by topic/partition",
		},
		[]string{"topic", "partition"},
	)

	ESClusterHealth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "es_cluster_health_status",
			Help: "ES cluster health (0=green, 1=yellow, 2=red)",
		},
		[]string{"color"},
	)
)
